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
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::{Request, Response, Status, Streaming};

use crate::etcd::v3::*;

type Result<T> = std::result::Result<T, Status>;

pub struct LeaseKeepAliveStream {}

impl futures::Stream for LeaseKeepAliveStream {
    type Item = Result<LeaseKeepAliveResponse>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct Lease {}

#[tonic::async_trait]
impl lease_server::Lease for Lease {
    /// LeaseGrant creates a lease which expires if the server does not receive
    /// a keepAlive within a given time to live period. All keys attached to
    /// the lease will be expired and deleted if the lease expires. Each
    /// expired key generates a delete event in the event history.
    async fn lease_grant(
        &self,
        _request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>> {
        todo!()
    }

    /// LeaseRevoke revokes a lease. All keys attached to the lease will expire
    /// and be deleted.
    async fn lease_revoke(
        &self,
        _request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>> {
        todo!()
    }

    /// Server streaming response type for the LeaseKeepAlive method.
    type LeaseKeepAliveStream = LeaseKeepAliveStream;

    /// LeaseKeepAlive keeps the lease alive by streaming keep alive requests
    /// from the client to the server and streaming keep alive responses
    /// from the server to the client.
    async fn lease_keep_alive(
        &self,
        _request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>> {
        todo!()
    }

    /// LeaseTimeToLive retrieves lease information.
    async fn lease_time_to_live(
        &self,
        _request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>> {
        todo!()
    }

    /// LeaseLeases lists all existing leases.
    async fn lease_leases(
        &self,
        _request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>> {
        todo!()
    }
}
