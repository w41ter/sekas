// Copyright 2024-present The Engula Authors.
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

//! A mod to hold the helper functions of build requests.

use crate::server::v1::*;

impl GroupRequest {
    /// build transfer leader request.
    pub fn transfer_leader(group_id: u64, epoch: u64, transferee: u64) -> Self {
        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Transfer(TransferRequest {
                    transferee,
                })),
            }),
        }
    }

    /// build create shard request.
    pub fn create_shard(group_id: u64, epoch: u64, shard_desc: ShardDesc) -> Self {
        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::CreateShard(CreateShardRequest {
                    shard: Some(shard_desc),
                })),
            }),
        }
    }

    /// build add replica request.
    pub fn add_replica(group_id: u64, epoch: u64, replica_id: u64, new_node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Add.into(),
                    replica_id,
                    node_id: new_node_id,
                }],
            }),
        };

        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        }
    }

    /// build add learner request.
    pub fn add_learner(group_id: u64, epoch: u64, replica_id: u64, learner_node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::AddLearner.into(),
                    replica_id,
                    node_id: learner_node_id,
                }],
            }),
        };

        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        }
    }

    /// build remove replica request
    pub fn remove_replica(group_id: u64, epoch: u64, replica_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Remove.into(),
                    replica_id,
                    ..Default::default()
                }],
            }),
        };

        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        }
    }

    /// build accept shard request.
    pub fn accept_shard(
        group_id: u64,
        epoch: u64,
        src_group_id: u64,
        src_group_epoch: u64,
        shard_desc: &ShardDesc,
    ) -> Self {
        GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::AcceptShard(AcceptShardRequest {
                    src_group_id,
                    src_group_epoch,
                    shard_desc: Some(shard_desc.to_owned()),
                })),
            }),
        }
    }
}
