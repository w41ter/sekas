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
pub mod db;
pub mod keys;
pub mod table;
pub mod txn;

use sekas_api::server::v1::*;

/// Return the shards of the system unity tables.
pub fn unity_table_shards() -> Vec<ShardDesc> {
    vec![
        table::database_shard_desc(),
        table::table_shard_desc(),
        table::meta_shard_desc(),
        table::node_shard_desc(),
        table::group_shard_desc(),
        table::replica_state_shard_desc(),
        table::job_shard_desc(),
        table::job_history_shard_desc(),
        table::txn_shard_desc(),
    ]
}

/// Return the tables of the system database.
pub fn tables() -> Vec<TableDesc> {
    vec![
        table::database_desc(),
        table::table_desc(),
        table::meta_desc(),
        table::node_desc(),
        table::group_desc(),
        table::replica_state_desc(),
        table::job_desc(),
        table::job_history_desc(),
        table::txn_desc(),
    ]
}

/// Return the descriptor of the root group.
pub fn root_group() -> GroupDesc {
    GroupDesc {
        id: crate::ROOT_GROUP_ID,
        epoch: crate::INITIAL_EPOCH,
        shards: unity_table_shards(),
        replicas: vec![ReplicaDesc {
            id: crate::FIRST_REPLICA_ID,
            node_id: crate::FIRST_NODE_ID,
            role: ReplicaRole::Voter.into(),
        }],
    }
}

/// Return the descriptor of the first user group.
pub fn init_group() -> GroupDesc {
    GroupDesc {
        id: crate::FIRST_GROUP_ID,
        epoch: crate::INITIAL_EPOCH,
        shards: vec![],
        replicas: vec![ReplicaDesc {
            id: crate::INIT_USER_REPLICA_ID,
            node_id: crate::FIRST_NODE_ID,
            role: ReplicaRole::Voter.into(),
        }],
    }
}
