// Copyright 2023-present The Engula Authors.
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

use prost::Message;
use sekas_api::server::v1::*;
use tonic::transport::Channel;
use tonic::IntoRequest;

#[derive(Debug, Clone)]
pub struct Client {
    client: node_client::NodeClient<Channel>,
}

impl Client {
    pub fn new(channel: Channel) -> Self {
        Client { client: node_client::NodeClient::new(channel) }
    }

    pub async fn connect(addr: String) -> Result<Self, tonic::transport::Error> {
        let addr = format!("http://{}", addr);
        let client = node_client::NodeClient::connect(addr).await?;
        Ok(Self { client })
    }

    pub async fn get_root(&self) -> Result<RootDesc, tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::GetRoot(GetRootRequest::default())),
            })
            .await?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::GetRoot(resp)) => Ok(resp.root.unwrap_or_default()),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `GetRootResponse` is required".to_owned(),
            )),
        }
    }

    // NOTE: This method is always called by the root group.
    pub async fn create_replica(
        &self,
        replica_id: u64,
        group_desc: GroupDesc,
    ) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let req = CreateReplicaRequest { replica_id, group: Some(group_desc) };
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::CreateReplica(req)),
            })
            .await?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::CreateReplica(_)) => Ok(()),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `CreateReplicaResponse` is required".to_owned(),
            )),
        }
    }

    // NOTE: This method is always called by the root group.
    pub async fn remove_replica(
        &self,
        replica_id: u64,
        group: GroupDesc,
    ) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let req = RemoveReplicaRequest { replica_id, group: Some(group) };
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::RemoveReplica(req)),
            })
            .await?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::RemoveReplica(_)) => Ok(()),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `RemoveReplicaResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn batch_group_requests(
        &self,
        req: impl IntoRequest<BatchRequest>,
    ) -> Result<Vec<GroupResponse>, tonic::Status> {
        let mut client = self.client.clone();
        let res = client.batch(req).await?;
        Ok(res.into_inner().responses)
    }

    pub async fn root_heartbeat(
        &self,
        req: HeartbeatRequest,
    ) -> Result<HeartbeatResponse, tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .admin(NodeAdminRequest { request: Some(node_admin_request::Request::Heartbeat(req)) })
            .await?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::Heartbeat(resp)) => Ok(resp),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `HeartbeatResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn forward(&self, req: ForwardRequest) -> Result<ForwardResponse, tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .migrate(MigrateRequest { request: Some(migrate_request::Request::Forward(req)) })
            .await?;
        match resp.into_inner().response {
            Some(migrate_response::Response::Forward(resp)) => Ok(resp),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `ForwardResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn setup_migration(&self, desc: MigrationDesc) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .migrate(MigrateRequest {
                request: Some(migrate_request::Request::Setup(SetupMigrationRequest {
                    desc: Some(desc),
                })),
            })
            .await?;
        match resp.into_inner().response {
            Some(migrate_response::Response::Setup(_)) => Ok(()),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `SetupMigrationDesc` is required".to_owned(),
            )),
        }
    }

    pub async fn commit_migration(&self, desc: MigrationDesc) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let resp = client
            .migrate(MigrateRequest {
                request: Some(migrate_request::Request::Commit(CommitMigrationRequest {
                    desc: Some(desc),
                })),
            })
            .await?;
        match resp.into_inner().response {
            Some(migrate_response::Response::Commit(_)) => Ok(()),
            _ => Err(tonic::Status::internal(
                "Invalid response type, `CommitMigrationDesc` is required".to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestBatchBuilder {
    node_id: u64,
    requests: Vec<GroupRequest>,
}

impl RequestBatchBuilder {
    pub fn new(node_id: u64) -> Self {
        Self { node_id, requests: vec![] }
    }

    pub fn get(mut self, group_id: u64, epoch: u64, shard_id: u64, key: Vec<u8>) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Get(ShardGetRequest {
                    shard_id,
                    start_version: u64::MAX,
                    key,
                })),
            }),
        });
        self
    }

    pub fn create_shard(mut self, group_id: u64, epoch: u64, shard_desc: ShardDesc) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::CreateShard(CreateShardRequest {
                    shard: Some(shard_desc),
                })),
            }),
        });
        self
    }

    pub fn add_replica(mut self, group_id: u64, epoch: u64, replica_id: u64, node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Add.into(),
                    replica_id,
                    node_id,
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        });
        self
    }

    pub fn add_learner(mut self, group_id: u64, epoch: u64, replica_id: u64, node_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::AddLearner.into(),
                    replica_id,
                    node_id,
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        });
        self
    }

    pub fn remove_replica(mut self, group_id: u64, epoch: u64, replica_id: u64) -> Self {
        let change_replicas = ChangeReplicasRequest {
            change_replicas: Some(ChangeReplicas {
                changes: vec![ChangeReplica {
                    change_type: ChangeReplicaType::Remove.into(),
                    replica_id,
                    ..Default::default()
                }],
            }),
        };

        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::ChangeReplicas(change_replicas)),
            }),
        });
        self
    }

    pub fn accept_shard(
        mut self,
        group_id: u64,
        epoch: u64,
        src_group_id: u64,
        src_group_epoch: u64,
        shard_desc: &ShardDesc,
    ) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::AcceptShard(AcceptShardRequest {
                    src_group_id,
                    src_group_epoch,
                    shard_desc: Some(shard_desc.to_owned()),
                })),
            }),
        });
        self
    }

    pub fn transfer_leader(mut self, group_id: u64, epoch: u64, transferee: u64) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Transfer(TransferRequest {
                    transferee,
                })),
            }),
        });
        self
    }

    pub fn shard_prefix(mut self, group_id: u64, epoch: u64, shard_id: u64, prefix: &[u8]) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Scan(ShardScanRequest {
                    shard_id,
                    start_version: u64::MAX,
                    prefix: Some(prefix.to_owned()),
                    ..Default::default()
                })),
            }),
        });
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn shard_scan(
        mut self,
        group_id: u64,
        epoch: u64,
        shard_id: u64,
        limit: u64,
        limit_bytes: u64,
        exclude_start_key: bool,
        exclude_end_key: bool,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Self {
        self.requests.push(GroupRequest {
            group_id,
            epoch,
            request: Some(GroupRequestUnion {
                request: Some(group_request_union::Request::Scan(ShardScanRequest {
                    shard_id,
                    start_version: u64::MAX,
                    limit,
                    limit_bytes,
                    exclude_start_key,
                    exclude_end_key,
                    prefix: None,
                    start_key,
                    end_key,
                })),
            }),
        });
        self
    }

    pub fn build(self) -> BatchRequest {
        BatchRequest { node_id: self.node_id, requests: self.requests }
    }
}

#[derive(Default, Clone, Debug)]
pub struct RpcTimeout<T: Message> {
    timeout: Option<Duration>,
    msg: T,
}

impl<T: Message> RpcTimeout<T> {
    pub fn new(timeout: Option<Duration>, msg: T) -> Self {
        RpcTimeout { timeout, msg }
    }
}

impl<T: Message> IntoRequest<T> for RpcTimeout<T> {
    fn into_request(self) -> tonic::Request<T> {
        use tonic::Request;

        let mut req = Request::new(self.msg);
        if let Some(duration) = self.timeout {
            req.set_timeout(duration);
        }
        req
    }
}
