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

mod cas;
mod cmd_accept_shard;
mod cmd_get;
mod cmd_move_replicas;
mod cmd_scan;
mod cmd_txn;
mod cmd_write;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::channel::oneshot;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::{ShardDesc, ShardWriteRequest};

pub(crate) use self::cmd_accept_shard::accept_shard;
pub(crate) use self::cmd_get::get;
pub(crate) use self::cmd_move_replicas::move_replicas;
pub(crate) use self::cmd_scan::scan;
pub(crate) use self::cmd_txn::write_intent;
pub(crate) use self::cmd_write::batch_write;
use crate::serverpb::v1::EvalResult;
use crate::{Error, Result};

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct ShardKey {
    shard_id: u64,
    user_key: Vec<u8>,
}

#[derive(Default)]
struct Latch {
    hold: bool,
    waiters: VecDeque<oneshot::Sender<LatchGuard>>,
}

pub struct LatchGuard {
    hold: bool,
    shard_key: ShardKey,
    latch_manager: RowLatchManager,
}

#[derive(Clone)]
pub struct RowLatchManager {
    latches: Arc<DashMap<ShardKey, Latch>>,
}

impl RowLatchManager {
    pub fn new() -> Self {
        RowLatchManager { latches: Arc::new(DashMap::with_shard_amount(16)) }
    }

    pub async fn acquire(
        &self,
        shard_id: u64,
        key: &[u8],
        timeout: Option<Duration>,
    ) -> Result<LatchGuard> {
        match self.acquire_internal(shard_id, key) {
            Ok(latch) => Ok(latch),
            Err(rx) => {
                if let Some(timeout) = timeout {
                    Ok(tokio::time::timeout(timeout, rx)
                        .await
                        .map_err(|_| Error::DeadlineExceeded("wait latch timeout".to_owned()))?
                        .expect("Will not be dropped without send()"))
                } else {
                    Ok(rx.await.expect("Will not be dropped without send()"))
                }
            }
        }
    }

    pub fn release(&self, shard_id: u64, key: &[u8]) {
        let shard_key = ShardKey { shard_id, user_key: key.to_owned() };

        self.latches.remove_if_mut(&shard_key, |shard_key, value| {
            let mut guard = LatchGuard {
                hold: true,
                shard_key: shard_key.clone(),
                latch_manager: self.clone(),
            };
            while let Some(sender) = value.waiters.pop_front() {
                guard = match sender.send(guard) {
                    Ok(()) => {
                        // The guard will wakes up the remaining waiters even the receiver is
                        // canceled.
                        value.hold = true;
                        return false;
                    }
                    Err(guard) => guard,
                };
            }

            // No more waiters, remove entry from map.
            guard.hold = false;
            value.hold = false;
            true
        });
    }

    fn acquire_internal(
        &self,
        shard_id: u64,
        key: &[u8],
    ) -> Result<LatchGuard, oneshot::Receiver<LatchGuard>> {
        let shard_key = ShardKey { shard_id, user_key: key.to_owned() };

        let mut entry = self.latches.entry(shard_key).or_default();
        let latch = entry.value_mut();
        if !latch.hold {
            latch.hold = true;
            Ok(LatchGuard {
                hold: true,
                shard_key: ShardKey { shard_id, user_key: key.to_owned() },
                latch_manager: self.clone(),
            })
        } else {
            let (tx, rx) = oneshot::channel();
            latch.waiters.push_back(tx);
            Err(rx)
        }
    }
}

impl Drop for LatchGuard {
    fn drop(&mut self) {
        if self.hold {
            self.latch_manager.release(self.shard_key.shard_id, &self.shard_key.user_key);
        }
    }
}

pub fn add_shard(shard: ShardDesc) -> EvalResult {
    use crate::serverpb::v1::SyncOp;

    EvalResult { op: Some(SyncOp::add_shard(shard)), ..Default::default() }
}

pub async fn acquire_row_latches(
    latch_mgr: &RowLatchManager,
    request: &Request,
    timeout: Option<Duration>,
) -> Result<Option<Vec<LatchGuard>>> {
    let (shard_id, mut keys) = match request {
        Request::Write(req) => (req.shard_id, collect_shard_write_keys(req)?),
        Request::WriteIntent(req) => {
            if let Some(req) = req.write.as_ref() {
                (req.shard_id, collect_shard_write_keys(req)?)
            } else {
                return Ok(None);
            }
        }
        Request::CommitIntent(req) => (req.shard_id, req.keys.clone()),
        Request::ClearIntent(req) => (req.shard_id, req.keys.clone()),
        Request::Scan(_)
        | Request::Get(_)
        | Request::CreateShard(_)
        | Request::ChangeReplicas(_)
        | Request::AcceptShard(_)
        | Request::Transfer(_)
        | Request::MoveReplicas(_) => return Ok(None),
    };

    if keys.is_empty() {
        return Ok(None);
    }

    // ATTN: Sort shard keys before acquiring any latch, to avoid deadlock.
    keys.sort_unstable();
    let mut latches = Vec::with_capacity(keys.len());
    for user_key in keys {
        latches.push(latch_mgr.acquire(shard_id, &user_key, timeout).await?);
    }
    Ok(Some(latches))
}

fn collect_shard_write_keys(req: &ShardWriteRequest) -> Result<Vec<Vec<u8>>> {
    let mut keys = Vec::with_capacity(req.puts.len() + req.deletes.len());
    for put in &req.puts {
        keys.push(put.key.clone());
    }
    for delete in &req.deletes {
        keys.push(delete.key.clone());
    }
    Ok(keys)
}
