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

use futures::Stream;
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
pub struct TcpIncoming {
    inner: TcpListener,
    nodelay: bool,
}

impl TcpIncoming {
    pub fn from_listener(listener: TcpListener, nodelay: bool) -> Self {
        TcpIncoming { inner: listener, nodelay }
    }
}

impl Stream for TcpIncoming {
    type Item = Result<TcpStream, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_accept(cx) {
            Poll::Ready(Ok((stream, _))) => {
                if let Err(error) = stream.set_nodelay(self.nodelay) {
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(stream)))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}
