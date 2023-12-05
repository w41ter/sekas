// Copyright 2023-present The Sekas Authors.
// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use log::trace;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::*;
use sekas_schema::shard;

use super::{ExecCtx, Replica};
use crate::node::metrics::NODE_RETRY_TOTAL;
use crate::node::move_shard::MoveShardController;
use crate::serverpb::v1::MoveShardEvent;
use crate::{Error, Result};

pub async fn move_shard_with_retry(
    replica: &Replica,
    event: MoveShardEvent,
    desc: &MoveShardDesc,
) -> Result<()> {
    loop {
        let resp = match event {
            MoveShardEvent::Setup => replica.setup_shard_moving(desc).await,
            MoveShardEvent::Commit => replica.commit_shard_moving(desc).await,
            _ => panic!("Unexpected moving shard event"),
        };
        match resp {
            Ok(()) => return Ok(()),
            Err(Error::ServiceIsBusy(_)) | Err(Error::GroupNotReady(_)) => {
                // sleep and retry.
                NODE_RETRY_TOTAL.inc();
                sekas_runtime::time::sleep(Duration::from_micros(200)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

/// A wrapper function that detects and completes retries as quickly as
/// possible.
#[inline]
pub async fn execute(
    replica: &Replica,
    exec_ctx: &ExecCtx,
    request: &GroupRequest,
) -> Result<GroupResponse> {
    execute_internal(None, replica, exec_ctx, request).await
}

#[inline]
pub async fn forwardable_execute(
    migrate_ctrl: &MoveShardController,
    replica: &Replica,
    exec_ctx: &ExecCtx,
    request: &GroupRequest,
) -> Result<GroupResponse> {
    execute_internal(Some(migrate_ctrl), replica, exec_ctx, request).await
}

async fn execute_internal(
    move_shard_ctrl: Option<&MoveShardController>,
    replica: &Replica,
    exec_ctx: &ExecCtx,
    request: &GroupRequest,
) -> Result<GroupResponse> {
    let mut exec_ctx = exec_ctx.clone();
    exec_ctx.epoch = request.epoch;

    let request = request
        .request
        .as_ref()
        .and_then(|request| request.request.as_ref())
        .ok_or_else(|| Error::InvalidArgument("GroupRequest::request is None".into()))?;

    // TODO(walter) detect group request timeout.
    let mut freshed_descriptor = None;
    loop {
        exec_ctx.reset();
        trace!("group {} try execute request with epoch {}", exec_ctx.group_id, exec_ctx.epoch);
        match replica.execute(&mut exec_ctx, request).await {
            Ok(resp) => {
                let resp = if let Some(descriptor) = freshed_descriptor {
                    GroupResponse::with_error(resp, Error::EpochNotMatch(descriptor).into())
                } else {
                    GroupResponse::new(resp)
                };
                return Ok(resp);
            }
            Err(Error::Forward(forward_ctx)) => {
                if let Some(ctrl) = move_shard_ctrl {
                    let resp = ctrl.forward(forward_ctx, request).await?;
                    return Ok(GroupResponse::new(resp));
                } else {
                    panic!("receive forward response but no moving shard controller set");
                }
            }
            Err(Error::ServiceIsBusy(_)) | Err(Error::GroupNotReady(_)) => {
                // sleep and retry.
                NODE_RETRY_TOTAL.inc();
                sekas_runtime::time::sleep(Duration::from_micros(200)).await;
            }
            Err(Error::EpochNotMatch(desc)) => {
                if is_executable(&desc, request) {
                    debug_assert_ne!(desc.epoch, exec_ctx.epoch);
                    exec_ctx.epoch = desc.epoch;
                    freshed_descriptor = Some(desc);
                    NODE_RETRY_TOTAL.inc();
                    continue;
                }

                return Err(Error::EpochNotMatch(desc));
            }
            Err(Error::ShardNotFound(shard_id)) => {
                if exec_ctx.forward_shard_id.is_none() {
                    panic!(
                        "shard {shard_id} is not found in group {} for serving request {request:?} epoch {}",
                        replica.replica_info().group_id,
                        exec_ctx.epoch
                    );
                }

                // This is forwarding request and the target shard might be migrated to another
                // group. Return `EpochNotMatch` in this case to enforce client retrying with
                // fresh group descriptor.
                //
                // NOTES: the `accurate_epoch` should set to `true` for forwarding requests.
                return Err(Error::EpochNotMatch(replica.descriptor()));
            }
            Err(e) => return Err(e),
        }
    }
}

// TODO(walter) move retryable logic to sekas client.
fn is_executable(descriptor: &GroupDesc, request: &Request) -> bool {
    if !super::is_change_meta_request(request) {
        return match request {
            Request::Get(req) => is_target_shard_exists(descriptor, req.shard_id, &req.key),
            Request::Scan(req) => is_scan_retryable(descriptor, req),
            Request::Write(req) => {
                for delete in &req.deletes {
                    if !is_target_shard_exists(descriptor, req.shard_id, &delete.key) {
                        return false;
                    }
                }
                for put in &req.puts {
                    if !is_target_shard_exists(descriptor, req.shard_id, &put.key) {
                        return false;
                    }
                }
                true
            }
            Request::WriteIntent(req) => {
                let Some(write) = req.write.as_ref() else {
                    return false;
                };
                for delete in &write.deletes {
                    if !is_target_shard_exists(descriptor, write.shard_id, &delete.key) {
                        return false;
                    }
                }
                for put in &write.puts {
                    if !is_target_shard_exists(descriptor, write.shard_id, &put.key) {
                        return false;
                    }
                }

                true
            }
            Request::CommitIntent(req) => {
                for key in &req.keys {
                    if !is_target_shard_exists(descriptor, req.shard_id, key) {
                        return false;
                    }
                }
                true
            }
            Request::ClearIntent(req) => {
                for key in &req.keys {
                    if !is_target_shard_exists(descriptor, req.shard_id, key) {
                        return false;
                    }
                }
                true
            }
            _ => unreachable!(),
        };
    }

    false
}

fn is_target_shard_exists(desc: &GroupDesc, shard_id: u64, key: &[u8]) -> bool {
    // TODO(walter) support migrate meta.
    desc.shards
        .iter()
        .find(|s| s.id == shard_id)
        .map(|s| shard::belong_to(s, key))
        .unwrap_or_default()
}

fn is_scan_retryable(desc: &GroupDesc, req: &ShardScanRequest) -> bool {
    if let Some(prefix) = &req.prefix {
        return is_target_shard_exists(desc, req.shard_id, prefix);
    }
    // Now don't support retry range scan.
    false
}
