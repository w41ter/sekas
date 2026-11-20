// Copyright 2026 The Sekas Authors.
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

use std::net::SocketAddr;

use anyhow::{Context, Result};
use bytes::BufMut;
use log::info;
use prometheus::{Encoder, TextEncoder};
use vaelo::{BoundServer, IoBufBuilder, ServerBuilder, http};

const METRICS_PATH: &str = "/admin/metrics";

pub(super) async fn bind(addr: SocketAddr) -> Result<BoundServer> {
    let server = ServerBuilder::new()
        .rpc_protocols([])
        .route(http::Method::GET, METRICS_PATH, scrape)
        .bind(addr)
        .await
        .with_context(|| format!("bind benchmark metrics on {addr}"))?;
    info!("Serving benchmark metrics at http://{}{METRICS_PATH}", server.local_addr());
    Ok(server)
}

async fn scrape(
    _: http::Request<http::Incoming>,
) -> Result<http::Response<http::Full>, http::Error> {
    let encoder = TextEncoder::new();
    let mut output = Vec::new();
    encoder
        .encode(&prometheus::gather(), &mut output)
        .map_err(|err| http::Error::Body(err.to_string()))?;

    let mut body = IoBufBuilder::new();
    body.put_slice(&output);
    Ok(http::Response::builder()
        .header("content-type", encoder.format_type())
        .body(http::Full::new(body.finish()))
        .expect("valid Prometheus content type"))
}
