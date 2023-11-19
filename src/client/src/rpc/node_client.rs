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

#[cfg(test)]
mod timeout_error_tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use socket2::{Domain, Socket, Type};
    use tonic::transport::Endpoint;
    use tonic::Code;

    use super::{Client as NodeClient, *};
    use crate::error::retryable_rpc_err;

    #[tokio::test]
    async fn connect_timeout_report_timed_out() {
        // Connect to a non-routable IP address.
        let channel = Endpoint::new("http://10.255.255.1:1234".to_owned())
            .unwrap()
            .connect_timeout(Duration::from_millis(100))
            .connect_lazy();
        let client = NodeClient::new(channel);
        let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
        match client.batch_group_requests(RpcTimeout::new(Some(Duration::from_secs(3)), req)).await
        {
            Ok(_) => unreachable!(),
            Err(status) => {
                assert!(retryable_rpc_err(&status), "Expect Code::TimedOut, but got {status:?}");
            }
        }
    }

    #[tokio::test]
    async fn rpc_timeout_report_canceled() {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
        socket.listen(1).unwrap();
        let port = socket.local_addr().unwrap().as_socket_ipv4().unwrap().port();

        let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_timeout(Duration::from_millis(100))
            .connect_lazy();
        let client = NodeClient::new(channel);
        let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
        match client
            .batch_group_requests(RpcTimeout::new(Some(Duration::from_millis(100)), req))
            .await
        {
            Ok(_) => unreachable!(),
            Err(status) => {
                assert!(
                    matches!(status.code(), Code::Cancelled),
                    "Expect Code::Cancelled, got {status:?}"
                );
            }
        }
    }
}

#[cfg(test)]
mod transport_error_tests {
    use std::error::Error;
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::os::unix::prelude::{FromRawFd, IntoRawFd};
    use std::panic;
    use std::time::Duration;

    use log::info;
    use sekas_api::server::v1::node_server::NodeServer;
    use sekas_api::server::v1::*;
    use socket2::{Domain, Socket, Type};
    use tokio::sync::oneshot;
    use tonic::transport::Endpoint;

    use super::{Client as NodeClient, RequestBatchBuilder};
    use crate::error::{find_io_error, retryable_rpc_err, transport_err};

    struct MockedServer {}

    #[allow(unused)]
    #[tonic::async_trait]
    impl node_server::Node for MockedServer {
        async fn batch(
            &self,
            request: tonic::Request<sekas_api::server::v1::BatchRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::BatchResponse>, tonic::Status> {
            todo!()
        }

        async fn admin(
            &self,
            request: tonic::Request<sekas_api::server::v1::NodeAdminRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::NodeAdminResponse>, tonic::Status>
        {
            todo!()
        }

        async fn migrate(
            &self,
            request: tonic::Request<sekas_api::server::v1::MigrateRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::MigrateResponse>, tonic::Status>
        {
            todo!()
        }
    }

    #[tokio::test]
    async fn broken_pipe() {
        if cfg!(target_os = "macos") {
            return;
        }

        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        socket.set_linger(Some(Duration::ZERO)).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
        socket.listen(1).unwrap();
        let port = socket.local_addr().unwrap().as_socket_ipv4().unwrap().port();

        let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await
            .unwrap();
        let client = NodeClient::new(channel);
        let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
        drop(socket);
        match client.batch_group_requests(req).await {
            Ok(_) => unreachable!(),
            Err(status) => {
                info!("message {} details {status:?}", status.message());
                assert!(!retryable_rpc_err(&status));
                assert!(transport_err(&status));
                let err = find_io_error(&status).unwrap();
                assert!(matches!(err.kind(), ErrorKind::BrokenPipe));
            }
        }
    }

    // TODO: it is difficult to reproduce `connection reset` error in different env.
    #[tokio::test]
    #[ignore]
    async fn connection_closed() {
        if cfg!(target_os = "macos") {
            return;
        }

        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        socket.set_nodelay(true).unwrap();
        socket.set_nonblocking(true).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
        socket.listen(10).unwrap();
        let port = socket.local_addr().unwrap().as_socket_ipv4().unwrap().port();

        let (sender, receiver) = oneshot::channel::<()>();
        let handle = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
                async move {
                    use tokio::net::TcpListener;
                    use tokio_stream::wrappers::TcpListenerStream;
                    use tonic::transport::Server;

                    let listener =
                        unsafe { std::net::TcpListener::from_raw_fd(socket.into_raw_fd()) };
                    let listener = TcpListener::from_std(listener).unwrap();
                    let listener = TcpListenerStream::new(listener);
                    info!("listen mocked service");
                    let server = Server::builder()
                        .add_service(NodeServer::new(MockedServer {}))
                        .serve_with_incoming(listener);
                    tokio::select! {
                        _ = server => {}
                        _ = receiver => {
                            info!("shutdown");
                        }
                    };
                },
            );
        });

        let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
            .unwrap()
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await
            .unwrap();

        let client = NodeClient::new(channel);

        drop(sender);
        handle.join().unwrap();
        match client.get_root().await {
            Ok(_) => unreachable!(),
            Err(status) => {
                info!("message {} details {status:?}", status.message());
                assert!(retryable_rpc_err(&status));

                let mut cause = status.source();
                let found = loop {
                    if let Some(err) = cause {
                        if err.to_string().starts_with("operation was canceled: connection closed")
                        {
                            break true;
                        }
                        cause = err.source();
                    } else {
                        break false;
                    }
                };
                assert!(found, "status is {status:?}");
            }
        }
    }

    #[tokio::test]
    async fn connection_reset() {
        if cfg!(target_os = "macos") {
            return;
        }

        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        socket.set_linger(Some(Duration::ZERO)).unwrap();
        socket.bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into()).unwrap();
        socket.listen(100).unwrap();
        let port = socket.local_addr().unwrap().as_socket_ipv4().unwrap().port();

        let handle = tokio::spawn(async move {
            let channel = Endpoint::new(format!("http://127.0.0.1:{port}"))
                .unwrap()
                .connect_timeout(Duration::from_millis(100))
                .connect()
                .await
                .unwrap();
            let client = NodeClient::new(channel);
            let req = RequestBatchBuilder::new(0).transfer_leader(1, 1, 1).build();
            match client.batch_group_requests(req).await {
                Ok(_) => unreachable!(),
                Err(status) => {
                    info!("message {} details {status:?}", status.message());
                    assert!(!retryable_rpc_err(&status));
                    assert!(transport_err(&status));
                    let err = find_io_error(&status).unwrap();
                    assert!(matches!(err.kind(), ErrorKind::ConnectionReset));
                }
            }
        });

        tokio::time::sleep(Duration::from_secs(1)).await;

        drop(socket);
        handle.await.unwrap();
    }
}
