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

use sekas_api::server::v1::*;
use tonic::{Request, Response, Status};

use super::metrics::*;
use crate::serverpb::v1::MoveShardEvent;
use crate::{record_latency, record_latency_opt, Error, Server};

pub struct WatchKeyStream {}

impl futures::Stream for WatchKeyStream {
    type Item = Result<WatchKeyResponse, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // 1. scan the key to obtain the base version.
        // 2. watch key's updation.
        // 3. read the key value and return
        // 4. skip to 1
        todo!()
    }
}

#[crate::async_trait]
impl node_server::Node for Server {
    type WatchStream = WatchKeyStream;

    async fn group(
        &self,
        request: Request<GroupRequest>,
    ) -> Result<Response<GroupResponse>, Status> {
        let group_request = request.into_inner();
        record_latency_opt!(take_group_request_metrics(&group_request));
        let server = self.clone();
        let response =
            self.node.execute_request(&group_request).await.unwrap_or_else(error_to_response);
        Ok(Response::new(response))
    }

    async fn admin(
        &self,
        request: Request<NodeAdminRequest>,
    ) -> Result<Response<NodeAdminResponse>, Status> {
        let request = request.into_inner();
        let Some(request) = request.request else {
            return Err(Status::invalid_argument("AdminRequest::request is empty".to_owned()));
        };
        let resp = match request {
            node_admin_request::Request::GetRoot(_) => {
                node_admin_response::Response::GetRoot(self.get_root().await?)
            }
            node_admin_request::Request::CreateReplica(req) => {
                node_admin_response::Response::CreateReplica(self.create_replica(req).await?)
            }
            node_admin_request::Request::RemoveReplica(req) => {
                node_admin_response::Response::RemoveReplica(self.remove_replica(req).await?)
            }
            node_admin_request::Request::Heartbeat(req) => {
                node_admin_response::Response::Heartbeat(self.root_heartbeat(req).await?)
            }
        };
        Ok(Response::new(NodeAdminResponse { response: Some(resp) }))
    }

    async fn move_shard(
        &self,
        request: Request<MoveShardRequest>,
    ) -> Result<Response<MoveShardResponse>, Status> {
        let req = request.into_inner();
        let Some(req) = req.request else {
            return Err(Status::invalid_argument("MoveShardRequest::request is empty"));
        };
        let resp = match req {
            move_shard_request::Request::Forward(req) => {
                move_shard_response::Response::Forward(self.forward(req).await?)
            }
            move_shard_request::Request::AcquireShard(req) => {
                let Some(desc) = req.desc else {
                    return Err(Status::invalid_argument(
                        "AcquireShardRequest::desc is empty".to_owned(),
                    ));
                };
                record_latency!(take_migrate_request_metrics());
                self.node.move_shard(MoveShardEvent::Setup, desc).await?;
                move_shard_response::Response::AcquireShard(AcquireShardResponse::default())
            }
            move_shard_request::Request::MoveOut(req) => {
                let Some(desc) = req.desc else {
                    return Err(Status::invalid_argument(
                        "MoveOutRequest::desc is empty".to_owned(),
                    ));
                };
                record_latency!(take_migrate_request_metrics());
                self.node.move_shard(MoveShardEvent::Commit, desc).await?;
                move_shard_response::Response::MoveOut(MoveOutResponse::default())
            }
        };
        Ok(Response::new(MoveShardResponse { response: Some(resp) }))
    }

    async fn watch(
        &self,
        _request: Request<WatchKeyRequest>,
    ) -> Result<Response<WatchKeyStream>, Status> {
        todo!()
    }
}

impl Server {
    async fn forward(&self, request: ForwardRequest) -> Result<ForwardResponse, Status> {
        record_latency!(take_forward_request_metrics());
        Ok(self.node.forward(request).await?)
    }

    async fn get_root(&self) -> Result<GetRootResponse, Status> {
        record_latency!(take_get_root_request_metrics());
        let root = self.node.get_root().await;
        Ok(GetRootResponse { root: Some(root) })
    }

    async fn create_replica(
        &self,
        request: CreateReplicaRequest,
    ) -> Result<CreateReplicaResponse, Status> {
        record_latency!(take_create_replica_request_metrics());
        let group_desc =
            request.group.ok_or_else(|| Status::invalid_argument("the field `group` is empty"))?;
        let replica_id = request.replica_id;
        self.node.create_replica(replica_id, group_desc).await?;
        Ok(CreateReplicaResponse {})
    }

    async fn remove_replica(
        &self,
        request: RemoveReplicaRequest,
    ) -> Result<RemoveReplicaResponse, Status> {
        record_latency!(take_remove_replica_request_metrics());
        let group_desc =
            request.group.ok_or_else(|| Status::invalid_argument("the field `group` is empty"))?;
        let replica_id = request.replica_id;
        self.node.remove_replica(replica_id, &group_desc).await?;
        Ok(RemoveReplicaResponse {})
    }

    async fn root_heartbeat(&self, request: HeartbeatRequest) -> Result<HeartbeatResponse, Status> {
        record_latency!(take_root_heartbeat_request_metrics());
        let mut piggybacks_resps = Vec::with_capacity(request.piggybacks.len());

        for req in request.piggybacks {
            let info = match req.info.unwrap() {
                piggyback_request::Info::SyncRoot(req) => {
                    piggyback_response::Info::SyncRoot(self.update_root(req).await?)
                }
                piggyback_request::Info::CollectStats(req) => {
                    piggyback_response::Info::CollectStats(self.node.collect_stats(&req).await)
                }
                piggyback_request::Info::CollectGroupDetail(req) => {
                    piggyback_response::Info::CollectGroupDetail(
                        self.node.collect_group_detail(&req).await,
                    )
                }
                piggyback_request::Info::CollectMovingShardState(req) => {
                    piggyback_response::Info::CollectMovingShardState(
                        self.node.collect_moving_shard_state(&req).await,
                    )
                }
                piggyback_request::Info::CollectScheduleState(req) => {
                    piggyback_response::Info::CollectScheduleState(
                        self.node.collect_schedule_state(&req).await,
                    )
                }
            };
            piggybacks_resps.push(PiggybackResponse { info: Some(info) });
        }

        let root = self.node.get_root().await;
        Ok(HeartbeatResponse {
            timestamp: request.timestamp,
            root_epoch: root.epoch,
            piggybacks: piggybacks_resps,
        })
    }

    async fn update_root(&self, req: SyncRootRequest) -> crate::Result<SyncRootResponse> {
        if let Some(root) = req.root {
            self.node.update_root(root).await?;
        }
        Ok(SyncRootResponse {})
    }
}

fn error_to_response(err: Error) -> GroupResponse {
    GroupResponse { response: None, error: Some(err.into()) }
}
