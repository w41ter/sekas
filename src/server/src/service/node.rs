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
use sekas_runtime::JoinHandle;
use tonic::{Request, Response, Status};

use super::metrics::*;
use crate::serverpb::v1::MigrationEvent;
use crate::{record_latency, record_latency_opt, Error, Server};

#[crate::async_trait]
impl node_server::Node for Server {
    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let batch_request = request.into_inner();
        record_latency!(take_batch_request_metrics(&batch_request));
        if batch_request.requests.len() == 1 {
            let request = batch_request.requests.into_iter().next().expect("already checked");
            let server = self.clone();
            let response =
                Box::pin(async move { server.submit_group_request(&request).await }).await;
            Ok(Response::new(BatchResponse { responses: vec![response] }))
        } else {
            let handles = self.submit_group_requests(batch_request.requests);
            let mut responses = Vec::with_capacity(handles.len());
            for handle in handles {
                responses.push(handle.await.map_err(Error::from)?);
            }

            Ok(Response::new(BatchResponse { responses }))
        }
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

    async fn migrate(
        &self,
        request: Request<MigrateRequest>,
    ) -> Result<Response<MigrateResponse>, Status> {
        let req = request.into_inner();
        let Some(req) = req.request else {
            return Err(Status::invalid_argument("MigrateRequest::request is empty"));
        };
        let resp = match req {
            migrate_request::Request::Forward(req) => {
                migrate_response::Response::Forward(self.forward(req).await?)
            }
            migrate_request::Request::Setup(req) => {
                let Some(desc) = req.desc else {
                    return Err(Status::invalid_argument(
                        "SetupMigrationRequest::desc is empty".to_owned(),
                    ));
                };
                record_latency!(take_migrate_request_metrics());
                self.node.migrate(MigrationEvent::Setup, desc).await?;
                migrate_response::Response::Setup(SetupMigrationResponse::default())
            }
            migrate_request::Request::Commit(req) => {
                let Some(desc) = req.desc else {
                    return Err(Status::invalid_argument(
                        "CommitMigrationRequest::desc is empty".to_owned(),
                    ));
                };
                record_latency!(take_migrate_request_metrics());
                self.node.migrate(MigrationEvent::Commit, desc).await?;
                migrate_response::Response::Commit(CommitMigrationResponse::default())
            }
        };
        Ok(Response::new(MigrateResponse { response: Some(resp) }))
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
                piggyback_request::Info::CollectMigrationState(req) => {
                    piggyback_response::Info::CollectMigrationState(
                        self.node.collect_migration_state(&req).await,
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

    async fn submit_group_request(&self, request: &GroupRequest) -> GroupResponse {
        record_latency_opt!(take_group_request_metrics(request));
        self.node.execute_request(request).await.unwrap_or_else(error_to_response)
    }

    fn submit_group_requests(&self, requests: Vec<GroupRequest>) -> Vec<JoinHandle<GroupResponse>> {
        let mut handles = Vec::with_capacity(requests.len());
        for request in requests.into_iter() {
            let server = self.clone();
            let handle =
                sekas_runtime::spawn(async move { server.submit_group_request(&request).await });
            handles.push(handle);
        }
        handles
    }
}

fn error_to_response(err: Error) -> GroupResponse {
    GroupResponse { response: None, error: Some(err.into()) }
}
