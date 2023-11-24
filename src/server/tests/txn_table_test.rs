// Copyright 2023-present The Sekas Authors.
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

use std::time::Duration;

use log::info;
use sekas_api::server::v1::TxnState;
use sekas_client::ClientOptions;
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
async fn begin_txn_idempotent() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(1).await;
    let c = ClusterClient::new(nodes).await;
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(50)),
        timeout: Some(Duration::from_millis(200)),
    };
    let client = c.app_client_with_options(opts).await;

    let ts_table = sekas_client::TxnStateTable::new(client, Some(Duration::from_secs(5)));

    let start_version = 123321;
    ts_table.begin_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    info!("read txn record: {txn_record_opt:?}");
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running));

    ts_table.begin_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running));
}

#[sekas_macro::test]
async fn commit_txn_idempotent() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(1).await;
    let c = ClusterClient::new(nodes).await;
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(50)),
        timeout: Some(Duration::from_millis(200)),
    };
    let client = c.app_client_with_options(opts).await;

    let ts_table = sekas_client::TxnStateTable::new(client, Some(Duration::from_secs(5)));

    let start_version = 123321;
    ts_table.begin_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running && txn_record.commit_version.is_none()));

    let commit_version = start_version + 1;
    ts_table.commit_txn(start_version, commit_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Committed && txn_record.commit_version == Some(commit_version)));

    // Commit txn is idempotent.
    ts_table.commit_txn(start_version, commit_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Committed && txn_record.commit_version == Some(commit_version)));
}

#[sekas_macro::test]
async fn abort_txn_idempotent() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(1).await;
    let c = ClusterClient::new(nodes).await;
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(50)),
        timeout: Some(Duration::from_millis(200)),
    };
    let client = c.app_client_with_options(opts).await;

    let ts_table = sekas_client::TxnStateTable::new(client, Some(Duration::from_secs(5)));

    let start_version = 123321;
    ts_table.begin_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running && txn_record.commit_version.is_none()));

    ts_table.abort_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Aborted && txn_record.commit_version.is_none()));

    // Abort txn is idempotent.
    ts_table.abort_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Aborted && txn_record.commit_version.is_none()));
}

#[sekas_macro::test]
async fn txn_state_table_normal_case() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(1).await;
    let c = ClusterClient::new(nodes).await;
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(50)),
        timeout: Some(Duration::from_millis(200)),
    };
    let client = c.app_client_with_options(opts).await;

    let ts_table = sekas_client::TxnStateTable::new(client, Some(Duration::from_secs(5)));

    let start_version = 123321;
    ts_table.begin_txn(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running && txn_record.commit_version.is_none()));

    // Some heartbeats
    ts_table.heartbeat(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running && txn_record.commit_version.is_none()));

    ts_table.heartbeat(start_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Running && txn_record.commit_version.is_none()));

    let commit_version = start_version + 123;
    ts_table.commit_txn(start_version, commit_version).await.unwrap();
    let txn_record_opt = ts_table.get_txn_record(start_version).await.unwrap();
    assert!(matches!(txn_record_opt, Some(txn_record)
        if txn_record.start_version == start_version && txn_record.state == TxnState::Committed && txn_record.commit_version == Some(commit_version)));
}
