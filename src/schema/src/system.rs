// Copyright 2023 The Sekas Authors.
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
pub mod col;
pub mod db;

use sekas_api::server::v1::*;
use sekas_api::v1::CollectionDesc;

/// Return the shards of the system unity collections.
pub fn unity_col_shards() -> Vec<ShardDesc> {
    vec![
        col::database_shard_desc(),
        col::collection_shard_desc(),
        col::meta_shard_desc(),
        col::node_shard_desc(),
        col::group_shard_desc(),
        col::replica_state_shard_desc(),
        col::job_shard_desc(),
        col::job_history_shard_desc(),
    ]
}

/// Return the shards of the txn collection.
pub fn txn_col_shards() -> Vec<ShardDesc> {
    vec![ShardDesc {
        id: crate::FIRST_TXN_SHARD_ID,
        collection_id: col::TXN_ID,
        partition: Some(shard_desc::Partition::Hash(HashPartition {
            slot_id: 0,
            end_slot_id: col::TXN_SLOTS,
            slots: col::TXN_SLOTS,
        })),
    }]
}

/// Return the collections of the system database.
pub fn collections() -> Vec<CollectionDesc> {
    vec![
        col::database_desc(),
        col::collection_desc(),
        col::meta_desc(),
        col::node_desc(),
        col::group_desc(),
        col::replica_state_desc(),
        col::job_desc(),
        col::job_history_desc(),
        col::txn_desc(),
    ]
}
