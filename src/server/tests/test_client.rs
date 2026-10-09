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

use std::time::Duration;

use log::info;
use sekas_client::{AppError, ClientOptions};
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
async fn client_to_unreachable_peers() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(50)),
        timeout: Some(Duration::from_millis(200)),
    };
    let client = c.app_client_with_options(opts).await;
    let db = client.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let k = "key".as_bytes().to_vec();
    let v = "value".as_bytes().to_vec();
    db.put(co.id, k.clone(), v).await.unwrap();
    let r = db.get(co.id, k).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "value"));

    info!("shutdown cluster");

    ctx.shutdown();
    let k = "key".as_bytes().to_vec();
    let v = "value-1".as_bytes().to_vec();
    assert!(matches!(
        db.put(co.id, k.clone(), v.clone()).await,
        Err(AppError::Network(_) | AppError::DeadlineExceeded(_))
    ));
    assert!(matches!(
        db.put(co.id, k.clone(), v.clone()).await,
        Err(AppError::DeadlineExceeded(_))
    ));
    assert!(matches!(
        db.put(co.id, k.clone(), v.clone()).await,
        Err(AppError::DeadlineExceeded(_))
    ));
    assert!(matches!(
        db.put(co.id, k.clone(), v.clone()).await,
        Err(AppError::DeadlineExceeded(_))
    ));
}

#[sekas_macro::test]
async fn client_create_duplicated_database_or_table() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let client = c.app_client().await;
    let db = client.create_database("test_db".to_string()).await.unwrap();
    assert!(matches!(
        client.create_database("test_db".to_string()).await,
        Err(AppError::AlreadyExists(_))
    ));
    let co = db.create_table("test_co".to_string()).await.unwrap();
    assert!(matches!(
        db.create_table("test_co".to_string()).await,
        Err(AppError::AlreadyExists(_))
    ));
    c.assert_table_ready(co.id).await;

    let k = "key".as_bytes().to_vec();
    let v = "value".as_bytes().to_vec();
    db.put(co.id, k.clone(), v).await.unwrap();
    let r = db.get(co.id, k).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "value"));
}

#[sekas_macro::test]
async fn client_access_not_exists_database_or_table() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let client = c.app_client().await;
    assert!(matches!(
        client.open_database("test_db".to_string()).await,
        Err(AppError::NotFound(_))
    ));
    let db = client.create_database("test_db".to_string()).await.unwrap();
    assert!(matches!(db.open_table("test_co".to_string()).await, Err(AppError::NotFound(_))));
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let k = "key".as_bytes().to_vec();
    let v = "value".as_bytes().to_vec();
    db.put(co.id, k.clone(), v).await.unwrap();
    let r = db.get(co.id, k).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "value"));
}

#[sekas_macro::test]
async fn client_request_to_offline_leader() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let client = c.app_client().await;
    let db = client.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();

    c.assert_table_ready(co.id).await;
    c.assert_root_group_has_promoted().await;

    for i in 0..1000 {
        let k = format!("key-{i}").as_bytes().to_vec();
        let v = format!("value-{i}").as_bytes().to_vec();
        match db.put(co.id, k.clone(), v).await {
            Ok(_) => {}
            Err(AppError::Network(_)) => continue,
            Err(e) => {
                panic!("put {k:?}: {e:?}");
            }
        }
        let r = db.get(co.id, k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));
        if i == 100 {
            let state = c.find_router_group_state_by_key(co.id, b"key").await.unwrap();
            let node_id = c.get_group_leader_node_id(state.id).await.unwrap();
            ctx.stop_server(node_id).await;
        }
    }
}

#[sekas_macro::test]
async fn client_get_with_version() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let client = c.app_client().await;
    let db = client.create_database("test_db".to_owned()).await.unwrap();
    let table = db.create_table("test_table".to_owned()).await.unwrap();
    c.assert_table_ready(table.id).await;

    let key = b"key".to_vec();
    let absent = db.begin_txn().start_version().await.unwrap();
    db.put(table.id, key.clone(), b"old".to_vec()).await.unwrap();
    let old = db.get_raw_value(table.id, key.clone()).await.unwrap().unwrap();
    let snapshot = db.begin_txn().start_version().await.unwrap();
    db.put(table.id, key.clone(), b"new".to_vec()).await.unwrap();
    assert_eq!(db.get(table.id, key.clone()).await.unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.get_with_version(table.id, key.clone(), absent).await.unwrap(), None);
    assert_eq!(
        db.get_with_version(table.id, key.clone(), old.version).await.unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(
        db.get_raw_value_with_version(table.id, key.clone(), snapshot).await.unwrap(),
        Some(old)
    );

    db.delete(table.id, key.clone()).await.unwrap();
    let deleted = db.begin_txn().start_version().await.unwrap();
    db.put(table.id, key.clone(), b"reborn".to_vec()).await.unwrap();
    assert_eq!(db.get_with_version(table.id, key.clone(), deleted).await.unwrap(), None);
    let tombstone =
        db.get_raw_value_with_version(table.id, key.clone(), deleted).await.unwrap().unwrap();
    assert_eq!(tombstone.content, None);
    assert_eq!(
        db.get_with_version(table.id, key.clone(), snapshot).await.unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(db.get(table.id, key).await.unwrap(), Some(b"reborn".to_vec()));
}
