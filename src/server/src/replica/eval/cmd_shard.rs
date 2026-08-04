// Copyright 2026 The Sekas Authors.
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

use log::debug;
use sekas_api::server::v1::*;

use super::WriteEvalResult;
use crate::replica::{GroupEngine, SplitShard, SyncOp};
use crate::serverpb::v1::*;
use crate::{Error, Result};

pub async fn accept_shard(group_id: u64, epoch: u64, req: &AcceptShardRequest) -> WriteEvalResult {
    let move_shard_desc = MoveShardDesc {
        shard_desc: req.shard_desc.clone(),
        src_group_id: req.src_group_id,
        src_group_epoch: req.src_group_epoch,
        dest_group_id: group_id,
        dest_group_epoch: epoch,
    };
    let sync_op = SyncOp::move_shard(MoveShardEvent::Setup, move_shard_desc);
    WriteEvalResult::with_op(sync_op)
}

pub(crate) fn get_split_key(
    engine: &GroupEngine,
    req: &GetSplitKeyRequest,
) -> Result<GetSplitKeyResponse> {
    let split_key = match (req.split_start_key.as_deref(), req.split_target_size) {
        (Some(start_key), Some(target_size)) => {
            engine.estimate_split_key_after(req.shard_id, start_key, target_size)?
        }
        _ => engine.estimate_split_key(req.shard_id)?,
    };
    Ok(GetSplitKeyResponse { split_key })
}

/// Eval split shard request.
pub(crate) fn split_shard(
    engine: &GroupEngine,
    req: &SplitShardRequest,
) -> Result<WriteEvalResult> {
    let old_shard_id = req.old_shard_id;
    let new_shard_id = req.new_shard_id;

    debug!(
        "execute split shard {}, new shard id {}, has split key {}",
        old_shard_id,
        new_shard_id,
        req.split_key.is_some()
    );

    let shard_desc = engine.shard_desc(old_shard_id)?;
    let split_key = match req.split_key.as_ref().cloned() {
        Some(split_key) => {
            if !sekas_schema::shard::belong_to(&shard_desc, &split_key) {
                return Err(Error::InvalidArgument(format!(
                    "the user provided split key is not belong to the shard {old_shard_id}"
                )));
            }
            split_key
        }
        None => engine.estimate_split_key(old_shard_id)?.ok_or_else(|| {
            // ATTN: below error msg is used in `sekas_server::root::schedule.rs`.
            Error::InvalidArgument(format!(
                "shard estimated split keys is empty, shard id {}",
                old_shard_id
            ))
        })?,
    };

    debug!("execute split shard {}, split key {:?}", old_shard_id, split_key);
    debug_assert!(
        sekas_schema::shard::belong_to(&shard_desc, &split_key),
        "estimated split key {split_key:?} is not belongs to shard {shard_desc:?}"
    );

    let split_shard = SplitShard { old_shard_id, new_shard_id, split_key };
    let sync_op = Box::new(SyncOp { split_shard: Some(split_shard), ..Default::default() });
    Ok(WriteEvalResult::with_op(sync_op))
}

/// Eval merge shard request.
pub(crate) fn merge_shard(
    engine: &GroupEngine,
    req: &MergeShardRequest,
) -> Result<WriteEvalResult> {
    let left_shard_id = req.left_shard_id;
    let right_shard_id = req.right_shard_id;

    debug!("execute merge shard {right_shard_id} into {left_shard_id}",);

    let left_shard = engine.shard_desc(left_shard_id)?;
    let right_shard = engine.shard_desc(right_shard_id)?;
    let Some(RangePartition { start: _, end: left_end }) = &left_shard.range else {
        return Err(Error::InvalidData(format!(
            "apply merge shard but left shard {left_shard_id} range is missing",
        )));
    };
    let Some(RangePartition { start: right_start, end: _ }) = &right_shard.range else {
        return Err(Error::InvalidData(format!(
            "apply merge shard but right shard {right_shard_id} range is missing",
        )));
    };
    if left_end != right_start {
        return Err(Error::InvalidData(format!(
            "the left shard {left_shard_id} is not mergeable with right shard {right_shard_id}",
        )));
    }

    let merge_shard = MergeShard { left_shard_id, right_shard_id };
    let sync_op = Box::new(SyncOp { merge_shard: Some(merge_shard), ..Default::default() });
    Ok(WriteEvalResult::with_op(sync_op))
}

pub fn add_shard(shard: ShardDesc) -> WriteEvalResult {
    use crate::serverpb::v1::SyncOp;

    WriteEvalResult::with_op(SyncOp::add_shard(shard))
}

pub fn delete_shard(shard_id: u64) -> WriteEvalResult {
    use crate::serverpb::v1::SyncOp;

    WriteEvalResult::with_op(SyncOp::delete_shard(shard_id))
}
