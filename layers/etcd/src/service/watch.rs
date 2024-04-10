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

use tonic::{Request, Response, Streaming};

use crate::etcd::v3::*;

type Result<T> = std::result::Result<T, tonic::Status>;

pub struct Watch {}

pub struct WatchStream {}

impl futures::Stream for WatchStream {
    type Item = Result<WatchResponse>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

#[tonic::async_trait]
impl watch_server::Watch for Watch {
    /// Server streaming response type for the Watch method.
    type WatchStream = WatchStream;

    /// Watch watches for events happening or that have happened. Both input and
    /// output are streams; the input stream is for creating and canceling
    /// watchers and the output stream sends events. One watch RPC can watch
    /// on multiple key ranges, streaming events for several watches at
    /// once. The entire event history can be watched starting from the last
    /// compaction revision.
    async fn watch(
        &self,
        _request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<WatchStream>> {
        todo!()
    }
}
