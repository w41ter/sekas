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

mod cmd_accept_shard;
mod cmd_batch_write;
mod cmd_delete;
mod cmd_get;
mod cmd_move_replicas;
mod cmd_put;
mod cmd_scan;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use dashmap::DashMap;
use engula_api::server::v1::{
    group_request_union::Request, ShardDeleteRequest, ShardDesc, ShardPutRequest,
};
use futures::channel::oneshot;

pub(crate) use self::{
    cmd_accept_shard::accept_shard, cmd_batch_write::batch_write, cmd_delete::delete, cmd_get::get,
    cmd_move_replicas::move_replicas, cmd_put::put, cmd_scan::scan,
};
use crate::{serverpb::v1::EvalResult, Error, Result};

const FLAT_KEY_VERSION: u64 = u64::MAX - 1;
pub const MIGRATING_KEY_VERSION: u64 = 0;

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
        RowLatchManager {
            latches: Arc::new(DashMap::with_shard_amount(16)),
        }
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
        let shard_key = ShardKey {
            shard_id,
            user_key: key.to_owned(),
        };

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
        let shard_key = ShardKey {
            shard_id,
            user_key: key.to_owned(),
        };

        let mut entry = self.latches.entry(shard_key).or_default();
        let latch = entry.value_mut();
        if !latch.hold {
            latch.hold = true;
            Ok(LatchGuard {
                hold: true,
                shard_key: ShardKey {
                    shard_id,
                    user_key: key.to_owned(),
                },
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
            self.latch_manager
                .release(self.shard_key.shard_id, &self.shard_key.user_key);
        }
    }
}

pub fn add_shard(shard: ShardDesc) -> EvalResult {
    use crate::serverpb::v1::SyncOp;

    EvalResult {
        op: Some(SyncOp::add_shard(shard)),
        ..Default::default()
    }
}

pub async fn acquire_row_latches(
    latch_mgr: &RowLatchManager,
    request: &Request,
    timeout: Option<Duration>,
) -> Result<Option<Vec<LatchGuard>>> {
    match request {
        Request::Put(ShardPutRequest { shard_id, put }) => {
            let put = put
                .as_ref()
                .ok_or_else(|| Error::InvalidArgument("ShardPutRequest::put is None".into()))?;

            let guard = latch_mgr.acquire(*shard_id, &put.key, timeout).await?;
            Ok(Some(vec![guard]))
        }
        Request::Delete(ShardDeleteRequest { shard_id, delete }) => {
            let delete = delete.as_ref().ok_or_else(|| {
                Error::InvalidArgument("ShardDeleteRequest::delete is None".into())
            })?;
            let guard = latch_mgr.acquire(*shard_id, &delete.key, timeout).await?;
            Ok(Some(vec![guard]))
        }
        Request::BatchWrite(batch_writes) => {
            let mut keys = Vec::with_capacity(batch_writes.puts.len() + batch_writes.deletes.len());
            for req in &batch_writes.puts {
                let put = req
                    .put
                    .as_ref()
                    .ok_or_else(|| Error::InvalidArgument("ShardPutRequest::put is None".into()))?;
                keys.push(ShardKey {
                    shard_id: req.shard_id,
                    user_key: put.key.clone(),
                });
            }
            for req in &batch_writes.deletes {
                let delete = req.delete.as_ref().ok_or_else(|| {
                    Error::InvalidArgument("ShardDeleteRequest::delete is None".into())
                })?;
                keys.push(ShardKey {
                    shard_id: req.shard_id,
                    user_key: delete.key.clone(),
                });
            }

            // Sort shard keys before acquiring any latch, to avoid deadlock.
            keys.sort_unstable();
            let mut latches = Vec::with_capacity(keys.len());
            for ShardKey { shard_id, user_key } in keys {
                latches.push(latch_mgr.acquire(shard_id, &user_key, timeout).await?);
            }
            Ok(Some(latches))
        }
        Request::Scan(_)
        | Request::Get(_)
        | Request::CreateShard(_)
        | Request::ChangeReplicas(_)
        | Request::AcceptShard(_)
        | Request::Transfer(_)
        | Request::MoveReplicas(_) => Ok(None),
    }
}
