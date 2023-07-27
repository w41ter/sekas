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
pub mod shard;

use std::collections::BTreeMap;

use lazy_static::lazy_static;
use sekas_api::v1::{collection_desc, CollectionDesc};

/// The collection id of local states, which allows commit without replicating.
pub const LOCAL_COLLECTION_ID: u64 = 0;

pub const SYSTEM_DATABASE_NAME: &str = "__system__";
pub const SYSTEM_DATABASE_ID: u64 = 1;
pub const SYSTEM_TXN_COLLECTION: &str = "txn";
pub const SYSTEM_TXN_COLLECTION_ID: u64 = LOCAL_COLLECTION_ID + 1;
pub const SYSTEM_COLLECTION_COLLECTION: &str = "collection";
pub const SYSTEM_COLLECTION_COLLECTION_ID: u64 = SYSTEM_TXN_COLLECTION_ID + 1;
pub const SYSTEM_COLLECTION_COLLECTION_SHARD: u64 = 1;
pub const SYSTEM_DATABASE_COLLECTION: &str = "database";
pub const SYSTEM_DATABASE_COLLECTION_ID: u64 = SYSTEM_COLLECTION_COLLECTION_ID + 1;
pub const SYSTEM_DATABASE_COLLECTION_SHARD: u64 = SYSTEM_COLLECTION_COLLECTION_SHARD + 1;
pub const SYSTEM_MATE_COLLECTION: &str = "meta";
pub const SYSTEM_MATE_COLLECTION_ID: u64 = SYSTEM_DATABASE_COLLECTION_ID + 1;
pub const SYSTEM_MATE_COLLECTION_SHARD: u64 = SYSTEM_DATABASE_COLLECTION_SHARD + 1;
pub const SYSTEM_NODE_COLLECTION: &str = "node";
pub const SYSTEM_NODE_COLLECTION_ID: u64 = SYSTEM_MATE_COLLECTION_ID + 1;
pub const SYSTEM_NODE_COLLECTION_SHARD: u64 = SYSTEM_MATE_COLLECTION_SHARD + 1;
pub const SYSTEM_GROUP_COLLECTION: &str = "group";
pub const SYSTEM_GROUP_COLLECTION_ID: u64 = SYSTEM_NODE_COLLECTION_ID + 1;
pub const SYSTEM_GROUP_COLLECTION_SHARD: u64 = SYSTEM_NODE_COLLECTION_SHARD + 1;
pub const SYSTEM_REPLICA_STATE_COLLECTION: &str = "replica_state";
pub const SYSTEM_REPLICA_STATE_COLLECTION_ID: u64 = SYSTEM_GROUP_COLLECTION_ID + 1;
pub const SYSTEM_REPLICA_STATE_COLLECTION_SHARD: u64 = SYSTEM_GROUP_COLLECTION_SHARD + 1;
pub const SYSTEM_JOB_COLLECTION: &str = "job";
pub const SYSTEM_JOB_COLLECTION_ID: u64 = SYSTEM_REPLICA_STATE_COLLECTION_ID + 1;
pub const SYSTEM_JOB_COLLECTION_SHARD: u64 = SYSTEM_REPLICA_STATE_COLLECTION_SHARD + 1;
pub const SYSTEM_JOB_HISTORY_COLLECTION: &str = "job_history";
pub const SYSTEM_JOB_HISTORY_COLLECTION_ID: u64 = SYSTEM_JOB_COLLECTION_ID + 1;
pub const SYSTEM_JOB_HISTORY_COLLECTION_SHARD: u64 = SYSTEM_JOB_COLLECTION_SHARD + 1;

pub const USER_COLLECTION_INIT_ID: u64 = SYSTEM_JOB_HISTORY_COLLECTION_ID + 1;

pub const DEFAULT_TXN_COLLECTION_SLOTS: u32 = 256;

lazy_static! {
    pub static ref SYSTEM_COLLECTION_SHARD: BTreeMap<u64, u64> = BTreeMap::from([
        (SYSTEM_COLLECTION_COLLECTION_ID, SYSTEM_COLLECTION_COLLECTION_SHARD),
        (SYSTEM_DATABASE_COLLECTION_ID, SYSTEM_DATABASE_COLLECTION_SHARD),
        (SYSTEM_MATE_COLLECTION_ID, SYSTEM_MATE_COLLECTION_SHARD),
        (SYSTEM_NODE_COLLECTION_ID, SYSTEM_NODE_COLLECTION_SHARD),
        (SYSTEM_GROUP_COLLECTION_ID, SYSTEM_GROUP_COLLECTION_SHARD),
        (SYSTEM_REPLICA_STATE_COLLECTION_ID, SYSTEM_REPLICA_STATE_COLLECTION_SHARD),
        (SYSTEM_JOB_COLLECTION_ID, SYSTEM_JOB_COLLECTION_SHARD),
        (SYSTEM_JOB_HISTORY_COLLECTION_ID, SYSTEM_JOB_HISTORY_COLLECTION_SHARD),
    ]);
}

/// Return the shard id of system collection.
pub fn system_shard_id(collection_id: u64) -> u64 {
    let shard = SYSTEM_COLLECTION_SHARD.get(&collection_id);
    if shard.is_none() {
        panic!("no such shard exists for system collection {collection_id}");
    }
    shard.unwrap().to_owned()
}

/// Return the default descriptor of txn collection
pub fn txn_collection() -> CollectionDesc {
    CollectionDesc {
        id: SYSTEM_COLLECTION_COLLECTION_ID,
        name: SYSTEM_TXN_COLLECTION.into(),
        db: SYSTEM_DATABASE_ID,
        partition: Some(collection_desc::Partition::Hash(collection_desc::HashPartition {
            slots: DEFAULT_TXN_COLLECTION_SLOTS,
        })),
    }
}
