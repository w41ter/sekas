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

use std::collections::VecDeque;

use dashmap::{mapref::entry::Entry, DashMap};
use engula_api::server::v1::ShardDesc;
use futures::channel::oneshot;

pub(crate) use self::{
    cmd_accept_shard::accept_shard, cmd_batch_write::batch_write, cmd_delete::delete, cmd_get::get,
    cmd_move_replicas::move_replicas, cmd_put::put, cmd_scan::scan,
};
use crate::{serverpb::v1::EvalResult, Error, Result};

const FLAT_KEY_VERSION: u64 = u64::MAX - 1;
pub const MIGRATING_KEY_VERSION: u64 = 0;

pub fn add_shard(shard: ShardDesc) -> EvalResult {
    use crate::serverpb::v1::SyncOp;

    EvalResult {
        op: Some(SyncOp::add_shard(shard)),
        ..Default::default()
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct ShardKey {
    shard_id: u64,
    key: Vec<u8>,
}

#[derive(Default)]
struct Latch {
    hold: bool,
    waiters: VecDeque<oneshot::Sender<()>>,
}

pub struct RowLatchManager {
    latches: DashMap<ShardKey, Latch>,
}

impl RowLatchManager {
    pub fn new() -> Self {
        RowLatchManager {
            latches: DashMap::with_shard_amount(16),
        }
    }

    pub fn acquire(&self, shard_id: u64, key: &[u8]) -> Result<()> {
        let shard_key = ShardKey {
            shard_id,
            key: key.to_owned(),
        };

        // TODO: ensure order of latches.
        let mut entry = self.latches.entry(shard_key).or_default();
        let latch = entry.value_mut();
        if !latch.hold {
            latch.hold = true;
            Ok(())
        } else {
            let (tx, rx) = oneshot::channel();
            latch.waiters.push_back(tx);
            Err(Error::RowLockBusy(rx))
        }
    }

    pub fn release(&self, shard_id: u64, key: &[u8]) {
        let shard_key = ShardKey {
            shard_id,
            key: key.to_owned(),
        };

        self.latches.remove_if_mut(&shard_key, |_, value| {
            while let Some(sender) = value.waiters.pop_front() {
                if sender.send(()).is_ok() {
                    value.hold = true;
                    return false;
                }
            }

            // No more waiters, remove entry from map.
            true
        });
    }
}
