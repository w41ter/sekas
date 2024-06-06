// Copyright 2023-present The Engula Authors.
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

//! A mod to hold the helper functions of XxxDesc.

use crate::server::v1::{GroupDesc, RangePartition, ShardDesc};

impl ShardDesc {
    pub fn whole(shard_id: u64, table_id: u64) -> Self {
        ShardDesc {
            id: shard_id,
            table_id,
            range: Some(RangePartition { start: vec![], end: vec![] }),
        }
    }

    pub fn with_range(shard_id: u64, table_id: u64, start: Vec<u8>, end: Vec<u8>) -> Self {
        ShardDesc { id: shard_id, table_id, range: Some(RangePartition { start, end }) }
    }
}

impl GroupDesc {
    /// Get the target shard desc, [`None`] is returned if no such shard exists.
    pub fn shard(&self, shard_id: u64) -> Option<&ShardDesc> {
        self.shards.iter().find(|shard| shard.id == shard_id)
    }

    /// Get the target shard desc, [`None`] is returned if no such shard exists.
    pub fn shard_mut(&mut self, shard_id: u64) -> Option<&mut ShardDesc> {
        self.shards.iter_mut().find(|shard| shard.id == shard_id)
    }

    // Drop the target shard.
    pub fn drop_shard(&mut self, shard_id: u64) {
        self.shards.retain(|shard| shard.id != shard_id);
    }
}
