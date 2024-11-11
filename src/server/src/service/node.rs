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

use std::pin::Pin;
use std::task::{Context, Poll};

use async_stream::try_stream;
use futures::channel::mpsc;
use futures::StreamExt;
use log::trace;
use sekas_api::server::v1::group_request_union::Request as ShardRequest;
use sekas_api::server::v1::group_response_union::Response as ShardResponse;
use sekas_api::server::v1::watch_key_response::WatchResult;
use sekas_api::server::v1::*;
use sekas_schema::system::txn::{TXN_INTENT_VERSION, TXN_MAX_VERSION};
use tonic::{Request, Response, Status};

use super::metrics::*;
use crate::replica::ExecCtx;
use crate::serverpb::v1::MoveShardEvent;
use crate::{record_latency, record_latency_opt, Error, Server};

pub struct GroupStream {
    inner: Pin<Box<dyn futures::Stream<Item = Result<GroupResponse, Status>> + Send>>,
}

impl futures::Stream for GroupStream {
    type Item = Result<GroupResponse, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.poll_next_unpin(cx)
    }
}

fn handle_group_request(
    server: Server,
    request: GroupRequest,
) -> impl futures::Stream<Item = Result<GroupResponse, Status>> {
    try_stream! {
        record_latency_opt!(take_group_request_metrics(&request));
        let mut exec_ctx = ExecCtx::default();
        let inner_request = request
            .request
            .as_ref()
            .and_then(|request| request.request.as_ref())
            .ok_or_else(|| Error::InvalidArgument("GroupRequest::request is None".into()))?;
        if !matches!(inner_request, ShardRequest::WatchKey(_)) {
            let response =
                server.node.execute_request(&exec_ctx, &request).await.unwrap_or_else(error_to_response);
            yield response;
            return;
        }

        let ShardRequest::WatchKey(watch_key_req) = inner_request else { panic!("unreachable") };
        trace!("receive watch key request, shard {} key {} version {}",
            watch_key_req.shard_id,
            sekas_rock::ascii::escape_bytes(&watch_key_req.key),
            watch_key_req.version
        );

        let (sender, mut receiver) = mpsc::unbounded();
        exec_ctx.watch_event_sender = Some(sender);
        if let Err(err) = server.node.execute_request(&exec_ctx, &request).await {
            yield error_to_response(err);
            return;
        }

        // Clear the ownership of sender.
        exec_ctx.watch_event_sender = None;

        // scan the key to obtain an version.
        let scan_req = ShardScanRequest {
            shard_id: watch_key_req.shard_id,
            start_version: TXN_MAX_VERSION,
            limit: 0,
            limit_bytes: 0,
            end_key: None,
            exclude_end_key: true,
            exclude_start_key: false,
            prefix: None,
            start_key: Some(watch_key_req.key.clone()),
            include_raw_data: true,
            ignore_txn_intent: true,
            allow_scan_moving_shard: true,
        };
        let group_scan_req = GroupRequest {
            group_id: request.group_id,
            epoch: request.epoch,
            request: Some(GroupRequestUnion {
                request: Some(ShardRequest::Scan(scan_req)),
            }),
        };
        let resp = match server.node.execute_request(&exec_ctx, &group_scan_req).await {
            Ok(resp) => resp,
            Err(err) => {
                yield error_to_response(err);
                return;
            }
        };
        let ShardResponse::Scan(mut scan_resp) = resp.response
            .expect("The GroupResponse::response is required")
            .response
            .expect("The GroupResponseUnion::response is required")
            else { panic!("The ScanResponse is required in here") };
        if scan_resp.has_more {
            panic!("We should ensure that this scan request will returns the entire value set of an key");
        }

        // Whether the old value exists.
        if !scan_resp.data.is_empty() {
            if scan_resp.data.len() != 1 {
                panic!("The scan request issues one key but got {} value set, request {:?}, response {:?}",
                    scan_resp.data.len(), group_scan_req, scan_resp);
            }

            let value_set = &mut scan_resp.data[0];
            for value in std::mem::take(&mut value_set.values).into_iter().rev() {
                if value.version < watch_key_req.version {
                    continue;
                }
                let watch_key_resp = WatchKeyResponse {
                    result: WatchResult::ValueUpdated as i32,
                    value: Some(value),
                };
                yield GroupResponse {
                    response: Some(GroupResponseUnion {
                        response: Some(ShardResponse::WatchKey(watch_key_resp)),
                    }),
                    ..Default::default()
                };
            }
        }
        // TODO(walter) change receiver to async channel.
        while let Some(event) = receiver.next().await {
            if event.version == TXN_INTENT_VERSION || event.version < watch_key_req.version {
                continue;
            }
            let value = Value {
                content: event.value.map(Vec::from),
                version: event.version,
            };
            let watch_key_resp = WatchKeyResponse {
                result: WatchResult::ValueUpdated as i32,
                value: Some(value),
            };
            yield GroupResponse {
                response: Some(GroupResponseUnion {
                    response: Some(ShardResponse::WatchKey(watch_key_resp)),
                }),
                ..Default::default()
            };
        }

        let watch_key_resp = WatchKeyResponse {
            result: WatchResult::ShardMoved as i32,
            ..Default::default()
        };
        yield GroupResponse {
            response: Some(GroupResponseUnion {
                response: Some(ShardResponse::WatchKey(watch_key_resp))
            }),
            ..Default::default()
        };
    }
}

#[crate::async_trait]
impl node_server::Node for Server {
    type GroupStream = GroupStream;

    async fn group(
        &self,
        request: Request<GroupRequest>,
    ) -> Result<Response<Self::GroupStream>, Status> {
        let group_response_stream =
            Box::pin(handle_group_request(self.clone(), request.into_inner()));
        Ok(Response::new(GroupStream { inner: group_response_stream }))
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
        use piggyback_request::Info as Request;
        use piggyback_response::Info as Response;
        record_latency!(take_root_heartbeat_request_metrics());
        let mut piggybacks_resps = Vec::with_capacity(request.piggybacks.len());

        for piggyback in request.piggybacks {
            let Some(info) = piggyback.info else { continue };
            let resp = match info {
                Request::SyncRoot(req) => Response::SyncRoot(self.update_root(req).await?),
                Request::CollectStats(req) => {
                    Response::CollectStats(self.node.collect_stats(&req).await)
                }
                Request::CollectGroupDetail(req) => {
                    Response::CollectGroupDetail(self.node.collect_group_detail(&req).await)
                }
                Request::CollectMovingShardState(req) => Response::CollectMovingShardState(
                    self.node.collect_moving_shard_state(&req).await,
                ),
                Request::CollectScheduleState(req) => {
                    Response::CollectScheduleState(self.node.collect_schedule_state(&req).await)
                }
            };
            piggybacks_resps.push(PiggybackResponse { info: Some(resp) });
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
