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

use std::pin::Pin;
use std::time::Duration;

use futures::{Stream, StreamExt};
use prost::Message;
use sekas_api::server::v1::*;
use vaelo::{IntoRequest, RpcChannel, Status};

use crate::error::rpc_status;
use crate::trace::inject_trace_context;

#[derive(Debug, Clone)]
pub struct Client {
    client: node_client::NodeClient,
}

impl Client {
    pub fn new(channel: RpcChannel) -> Self {
        Client { client: node_client::NodeClient::new(channel) }
    }

    pub async fn connect(addr: String) -> Result<Self, Status> {
        let channel = RpcChannel::grpc_endpoint(addr)
            .await
            .map_err(rpc_status)?
            .build()
            .map_err(rpc_status)?;
        let client = node_client::NodeClient::new(channel);
        let client = Self { client };
        client.get_root().await?;
        Ok(client)
    }

    pub async fn get_root(&self) -> Result<RootDesc, Status> {
        let client = self.client.clone();
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::GetRoot(GetRootRequest::default())),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::GetRoot(resp)) => Ok(resp.root.unwrap_or_default()),
            _ => Err(Status::internal(
                "Invalid response type, `GetRootResponse` is required".to_owned(),
            )),
        }
    }

    // NOTE: This method is always called by the root group.
    pub async fn create_replica(
        &self,
        replica_id: u64,
        group_desc: GroupDesc,
    ) -> Result<(), Status> {
        let client = self.client.clone();
        let req = CreateReplicaRequest { replica_id, group: Some(group_desc) };
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::CreateReplica(req)),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::CreateReplica(_)) => Ok(()),
            _ => Err(Status::internal(
                "Invalid response type, `CreateReplicaResponse` is required".to_owned(),
            )),
        }
    }

    // NOTE: This method is always called by the root group.
    pub async fn remove_replica(&self, replica_id: u64, group: GroupDesc) -> Result<(), Status> {
        let client = self.client.clone();
        let req = RemoveReplicaRequest { replica_id, group: Some(group) };
        let resp = client
            .admin(NodeAdminRequest {
                request: Some(node_admin_request::Request::RemoveReplica(req)),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::RemoveReplica(_)) => Ok(()),
            _ => Err(Status::internal(
                "Invalid response type, `RemoveReplicaResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn group_request(
        &self,
        req: impl IntoTimedRequest<GroupRequest>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<GroupResponse, Status>> + Send>>, Status> {
        let client = self.client.clone();
        let (req, timeout) = req.into_timed_request();
        let call = client.group(inject_trace_context(req));
        let res = match timeout {
            Some(timeout) => call.timeout(timeout).await,
            None => call.await,
        }
        .map_err(rpc_status)?;
        Ok(Box::pin(res.into_inner().map(|result| result.map_err(rpc_status))))
    }

    pub async fn unary_group_request(
        &self,
        req: impl IntoTimedRequest<GroupRequest>,
    ) -> Result<GroupResponse, Status> {
        let client = self.client.clone();
        let (req, timeout) = req.into_timed_request();
        let call = client.unary_group(inject_trace_context(req));
        let res = match timeout {
            Some(timeout) => call.timeout(timeout).await,
            None => call.await,
        }
        .map_err(rpc_status)?;
        Ok(res.into_inner())
    }

    /// Submits a unary group request without retaining a response observer.
    pub fn detach_unary_group_request(&self, req: impl IntoTimedRequest<GroupRequest>) {
        let client = self.client.clone();
        let (req, timeout) = req.into_timed_request();
        let call = client.unary_group(inject_trace_context(req));
        match timeout {
            Some(timeout) => call.timeout(timeout).start().detach(),
            None => call.start().detach(),
        }
    }

    pub async fn root_heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse, Status> {
        self.root_heartbeat_inner(req, None).await
    }

    pub async fn root_heartbeat_timeout(
        &self,
        req: HeartbeatRequest,
        timeout: Duration,
    ) -> Result<HeartbeatResponse, Status> {
        self.root_heartbeat_inner(req, Some(timeout)).await
    }

    async fn root_heartbeat_inner(
        &self,
        req: HeartbeatRequest,
        timeout: Option<Duration>,
    ) -> Result<HeartbeatResponse, Status> {
        let client = self.client.clone();
        let call = client
            .admin(NodeAdminRequest { request: Some(node_admin_request::Request::Heartbeat(req)) });
        let resp = match timeout {
            Some(timeout) => call.timeout(timeout).await,
            None => call.await,
        }
        .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(node_admin_response::Response::Heartbeat(resp)) => Ok(resp),
            _ => Err(Status::internal(
                "Invalid response type, `HeartbeatResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn forward(&self, req: ForwardRequest) -> Result<ForwardResponse, Status> {
        let client = self.client.clone();
        let resp = client
            .move_shard(MoveShardRequest {
                request: Some(move_shard_request::Request::Forward(req)),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(move_shard_response::Response::Forward(resp)) => Ok(resp),
            _ => Err(Status::internal(
                "Invalid response type, `ForwardResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn acquire_shard(&self, desc: MoveShardDesc) -> Result<(), Status> {
        let client = self.client.clone();
        let resp = client
            .move_shard(MoveShardRequest {
                request: Some(move_shard_request::Request::AcquireShard(AcquireShardRequest {
                    desc: Some(desc),
                })),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(move_shard_response::Response::AcquireShard(_)) => Ok(()),
            _ => Err(Status::internal(
                "Invalid response type, `AcquireShardResponse` is required".to_owned(),
            )),
        }
    }

    pub async fn move_out(&self, desc: MoveShardDesc) -> Result<(), Status> {
        let client = self.client.clone();
        let resp = client
            .move_shard(MoveShardRequest {
                request: Some(move_shard_request::Request::MoveOut(MoveOutRequest {
                    desc: Some(desc),
                })),
            })
            .await
            .map_err(rpc_status)?;
        match resp.into_inner().response {
            Some(move_shard_response::Response::MoveOut(_)) => Ok(()),
            _ => Err(Status::internal(
                "Invalid response type, `MoveOutResponse` is required".to_owned(),
            )),
        }
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

pub trait IntoTimedRequest<T> {
    fn into_timed_request(self) -> (vaelo::Request<T>, Option<Duration>);
}

impl<T: Message> IntoTimedRequest<T> for RpcTimeout<T> {
    fn into_timed_request(self) -> (vaelo::Request<T>, Option<Duration>) {
        (vaelo::Request::new(self.msg), self.timeout)
    }
}

impl<T: Message> IntoTimedRequest<T> for T {
    fn into_timed_request(self) -> (vaelo::Request<T>, Option<Duration>) {
        (self.into_request(), None)
    }
}

#[cfg(test)]
#[cfg(any())]
mod timeout_error_tests {
    use std::net::SocketAddr;

    use socket2::{Domain, Socket, Type};
    use tonic::Code;
    use tonic::transport::Endpoint;

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
        let req = GroupRequest::transfer_leader(1, 1, 1);
        match client.group_request(RpcTimeout::new(Some(Duration::from_secs(3)), req)).await {
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
        let req = GroupRequest::transfer_leader(1, 1, 1);
        match client.group_request(RpcTimeout::new(Some(Duration::from_millis(100)), req)).await {
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
#[cfg(any())]
mod transport_error_tests {
    use std::error::Error;
    use std::io::ErrorKind;
    use std::net::SocketAddr;
    use std::os::unix::prelude::{FromRawFd, IntoRawFd};
    use std::panic;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures::Stream;
    use log::info;
    use sekas_api::server::v1::node_server::NodeServer;
    use sekas_api::server::v1::*;
    use socket2::{Domain, Socket, Type};
    use tokio::sync::oneshot;
    use tonic::transport::Endpoint;

    use super::Client as NodeClient;
    use crate::error::{find_io_error, retryable_rpc_err, transport_err};

    struct MockedServer {}
    struct MockGroupStream {}

    impl Stream for MockGroupStream {
        type Item = Result<GroupResponse, tonic::Status>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }

    #[allow(unused)]
    #[tonic::async_trait]
    impl node_server::Node for MockedServer {
        type GroupStream = MockGroupStream;

        async fn unary_group(
            &self,
            request: tonic::Request<sekas_api::server::v1::GroupRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::GroupResponse>, tonic::Status> {
            todo!()
        }

        async fn group(
            &self,
            request: tonic::Request<sekas_api::server::v1::GroupRequest>,
        ) -> Result<tonic::Response<Self::GroupStream>, tonic::Status> {
            todo!()
        }

        async fn unary_group(
            &self,
            request: tonic::Request<sekas_api::server::v1::GroupRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::GroupResponse>, tonic::Status> {
            todo!()
        }

        async fn admin(
            &self,
            request: tonic::Request<sekas_api::server::v1::NodeAdminRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::NodeAdminResponse>, tonic::Status>
        {
            todo!()
        }

        async fn move_shard(
            &self,
            request: tonic::Request<sekas_api::server::v1::MoveShardRequest>,
        ) -> Result<tonic::Response<sekas_api::server::v1::MoveShardResponse>, tonic::Status>
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
        let req = GroupRequest::transfer_leader(1, 1, 1);
        drop(socket);
        match client.group_request(req).await {
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
            let req = GroupRequest::transfer_leader(1, 1, 1);
            match client.group_request(req).await {
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
