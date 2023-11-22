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
mod helper;

use log::info;
use sekas_rock::fn_name;

use crate::helper::client::*;
use crate::helper::context::*;
use crate::helper::init::setup_panic_hook;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[sekas_macro::test]
async fn bootstrap_simple_cluster() {
    let mut ctx = TestContext::new(fn_name!());
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    // After this point, initialization has been completed.
    node_client_with_retry(&node_1_addr).await;
}

#[sekas_macro::test]
async fn bootstrap_cluster_join_node() {
    let mut ctx = TestContext::new(fn_name!());
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    let node_2_addr = ctx.next_listen_address();
    ctx.spawn_server(2, &node_2_addr, false, vec![node_1_addr.clone()]);

    info!("spawn 2 server, now connect both");
    node_client_with_retry(&node_1_addr).await;
    node_client_with_retry(&node_2_addr).await;

    // At this point, initialization and join has been completed.
}

#[sekas_macro::test]
async fn bootstrap_restart_cluster() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(3).await;

    // Shutdown and restart servers.
    ctx.shutdown();

    let nodes = ctx.start_servers(nodes).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;
    app.create_database("db".into()).await.unwrap();
}
