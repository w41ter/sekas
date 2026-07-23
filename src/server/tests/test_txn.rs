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
#![allow(unused)]
mod helper;

use std::time::Duration;

use log::info;
use sekas_api::server::v1::{TxnState, *};
use sekas_client::{AppError, ClientOptions, Error, TxnReadOptions, WriteBuilder};
use sekas_rock::fn_name;

use crate::helper::client::*;
use crate::helper::context::*;
use crate::helper::init::setup_panic_hook;
use crate::helper::runtime::*;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[sekas_macro::test]
async fn txn_write_batch_basic() {
    // TODO(walter) add two table and write in batch.
}

#[sekas_macro::test]
async fn txn_point_read_overlay_is_opt_in() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let put_key = b"put_key".to_vec();
    let delete_key = b"delete_key".to_vec();
    let add_key = b"add_key".to_vec();

    db.put(co.id, delete_key.clone(), b"deleted".to_vec()).await.unwrap();
    db.put(co.id, add_key.clone(), 1_i64.to_be_bytes().to_vec()).await.unwrap();

    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(put_key.clone()).ensure_put(b"local".to_vec()));
    txn.delete(co.id, WriteBuilder::new(delete_key.clone()).ensure_delete());
    txn.put(co.id, WriteBuilder::new(add_key.clone()).ensure_add(41));

    assert!(txn.get(co.id, put_key.clone()).await.unwrap().is_none());
    assert_eq!(txn.get(co.id, delete_key.clone()).await.unwrap(), Some(b"deleted".to_vec()));
    assert_eq!(txn.get(co.id, add_key.clone()).await.unwrap(), Some(1_i64.to_be_bytes().to_vec()));

    let overlay = TxnReadOptions { overlay_writes: true };
    assert_eq!(
        txn.get_with_options(co.id, put_key.clone(), overlay).await.unwrap(),
        Some(b"local".to_vec())
    );
    assert!(txn.get_with_options(co.id, delete_key.clone(), overlay).await.unwrap().is_none());
    assert_eq!(
        txn.get_with_options(co.id, add_key.clone(), overlay).await.unwrap(),
        Some(42_i64.to_be_bytes().to_vec())
    );

    txn.commit().await.unwrap();

    assert_eq!(db.get(co.id, put_key).await.unwrap(), Some(b"local".to_vec()));
    assert!(db.get(co.id, delete_key).await.unwrap().is_none());
    assert_eq!(db.get(co.id, add_key).await.unwrap(), Some(42_i64.to_be_bytes().to_vec()));
}

#[sekas_macro::test]
async fn txn_guard_key_conflicts() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let guard_key = b"account_pair_guard".to_vec();
    let key_a = b"account_a".to_vec();
    let key_b = b"account_b".to_vec();
    db.put(co.id, guard_key.clone(), b"guard".to_vec()).await.unwrap();

    let mut txn_a = db.begin_txn();
    txn_a.put(co.id, WriteBuilder::new(guard_key.clone()).ensure_put(Vec::new()));
    txn_a.put(co.id, WriteBuilder::new(key_a.clone()).ensure_put(b"a".to_vec()));
    let _ = txn_a.start_version().await.unwrap();

    let mut txn_b = db.begin_txn();
    txn_b.put(co.id, WriteBuilder::new(guard_key).ensure_put(Vec::new()));
    txn_b.put(co.id, WriteBuilder::new(key_b).ensure_put(b"b".to_vec()));
    txn_b.commit().await.unwrap();

    let result = txn_a.commit().await;
    assert!(matches!(result, Err(AppError::TxnConflict)));

    assert!(db.get(co.id, key_a).await.unwrap().is_none());
}

#[sekas_macro::test]
async fn txn_abort_and_clear_intents_after_prepare_failure() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let prepared_key = b"prepared_key".to_vec();
    let conflict_key = b"conflict_key".to_vec();
    let initial = b"initial".to_vec();

    db.put(co.id, conflict_key.clone(), initial.clone()).await.unwrap();

    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(prepared_key.clone()).ensure_put(b"prepared".to_vec()));
    txn.put(
        co.id,
        WriteBuilder::new(conflict_key.clone())
            .expect_not_exists()
            .ensure_put(b"should_not_commit".to_vec()),
    );
    let start_version = txn.start_version().await.unwrap();

    let result = txn.commit().await;
    assert!(matches!(result, Err(AppError::CasFailed(1, 0, _))));

    let txn_table = sekas_client::TxnStateTable::new(app.clone(), Some(Duration::from_secs(5)));
    let record = txn_table.get_txn_record(start_version).await.unwrap().unwrap();
    assert_eq!(record.state, TxnState::Aborted);
    assert!(record.commit_version.is_none());

    assert!(db.get(co.id, prepared_key.clone()).await.unwrap().is_none());
    assert_eq!(db.get(co.id, conflict_key.clone()).await.unwrap(), Some(initial));

    db.put(co.id, prepared_key.clone(), b"after_abort".to_vec()).await.unwrap();
    assert_eq!(db.get(co.id, prepared_key).await.unwrap(), Some(b"after_abort".to_vec()));
}
