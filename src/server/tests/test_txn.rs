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
use sekas_client::{AppError, ClientOptions, Error};
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
    let start_version = txn.get_start_version().await.unwrap();

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
