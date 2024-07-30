// Copyright 2023-resent The Sekas Authors.
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

use futures::StreamExt;
use log::info;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use sekas_api::server::v1::ReplicaRole;
use sekas_client::{AppError, ClientOptions, RangeRequest, SekasClient, WriteBuilder};
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
async fn cluster_rw_with_single_node() {
    let mut ctx = TestContext::new(fn_name!());
    let node_1_addr = ctx.next_listen_address();
    ctx.spawn_server(1, &node_1_addr, true, vec![]);

    node_client_with_retry(&node_1_addr).await;

    let addrs = vec![node_1_addr];
    let client = SekasClient::new(ClientOptions::default(), addrs).await.unwrap();
    let db = client.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();

    let k = "book_name".as_bytes().to_vec();
    let v = "rust_in_actions".as_bytes().to_vec();
    db.put(co.id, k.clone(), v).await.unwrap();
    let r = db.get(co.id, k).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "rust_in_actions"));
}

#[sekas_macro::test]
async fn cluster_rw_put_and_get() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let k = "book_name".as_bytes().to_vec();
    let v = "rust_in_actions".as_bytes().to_vec();
    db.put(co.id, k.clone(), v).await.unwrap();
    let r = db.get(co.id, k).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "rust_in_actions"));
}

#[sekas_macro::test]
async fn cluster_rw_put_many_keys() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    for i in 0..100 {
        let k = format!("key-{i}").as_bytes().to_vec();
        let v = format!("value-{i}").as_bytes().to_vec();
        db.put(co.id, k.clone(), v).await.unwrap();
        let r = db.get(co.id, k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));
    }
}

#[sekas_macro::test]
async fn cluster_rw_with_config_change() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let root_addr = nodes.get(&0).unwrap().clone();
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;
    c.assert_root_group_has_promoted().await;

    for i in 0..300 {
        if i == 20 {
            ctx.stop_server(2).await;
            ctx.add_server(vec![root_addr.clone()], 3).await;
        }

        let k = format!("key-{i}").as_bytes().to_vec();
        let v = format!("value-{i}").as_bytes().to_vec();
        db.put(co.id, k.clone(), v).await.unwrap();
        let r = db.get(co.id, k).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));
    }
}

#[sekas_macro::test]
async fn cluster_rw_with_leader_transfer() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    for i in 0..100 {
        let k = format!("key-{i}").as_bytes().to_vec();
        let v = format!("value-{i}").as_bytes().to_vec();
        db.put(co.id, k.clone(), v).await.unwrap();
        let r = db.get(co.id, k.clone()).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(r, Some(Ok(v)) if v == format!("value-{i}")));

        if i % 10 == 0 {
            let state = c.find_router_group_state_by_key(co.id, k.as_slice()).await.unwrap();
            let leader_id = state.leader_state.unwrap().0;
            for (id, replica) in state.replicas {
                if id != leader_id && replica.role == ReplicaRole::Voter as i32 {
                    info!("transfer leadership of group {} from {} to {}", state.id, leader_id, id);
                    let mut client = c.group(state.id);
                    client.transfer_leader(id).await.unwrap();
                    break;
                }
            }
        }
    }
}

#[sekas_macro::test]
async fn cluster_rw_with_shard_moving() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_num_cpus(3); // Add another group to accept shards.
    ctx.enable_group_balance();

    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let source_state = c.find_router_group_state_by_key(co.id, &[0]).await.unwrap();
    let prev_group_id = source_state.id;
    let target_group_id = 2;
    c.assert_num_group_voters(target_group_id, 3).await;

    for i in 0..100 {
        let k = format!("key-{i}").as_bytes().to_vec();
        let v = format!("value-{i}").as_bytes().to_vec();
        db.put(co.id, k.clone(), v).await.unwrap();
        let r = db.get(co.id, k.clone()).await.unwrap();
        let r = r.map(String::from_utf8);
        assert!(matches!(&r, Some(Ok(v)) if v == &format!("value-{i}")), "index {i}: {r:?}");

        if i % 10 == 0 {
            let source_state = c.find_router_group_state_by_key(co.id, &[0]).await.unwrap();
            if source_state.id == target_group_id {
                continue;
            }
            let shard_desc = c.get_shard_desc(co.id, &[0]).await.unwrap();
            let mut client = c.group(target_group_id);
            spawn(async move {
                client
                    .accept_shard(source_state.id, source_state.epoch, &shard_desc)
                    .await
                    .unwrap();
            });
        }
    }
    let source_state = c.find_router_group_state_by_key(co.id, &[0]).await.unwrap();
    assert_ne!(source_state.id, prev_group_id);
}

#[test]
#[ignore]
fn cluster_rw_single_server_large_read_write() {
    fn next_bytes(rng: &mut SmallRng, range: std::ops::Range<usize>) -> Vec<u8> {
        const BYTES: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let len = rng.gen_range(range);
        let mut buf = vec![0u8; len];
        rng.fill(buf.as_mut_slice());
        buf.iter_mut().for_each(|v| *v = BYTES[(*v % 62) as usize]);
        buf
    }

    block_on_current(async move {
        let mut ctx = TestContext::new("rw_test__single_server_large_read_write");
        let nodes = ctx.bootstrap_servers(1).await;
        let c = ClusterClient::new(nodes).await;
        let app = c.app_client().await;

        let db = app.create_database("test_db".to_string()).await.unwrap();
        let co = db.create_table("test_co".to_string()).await.unwrap();
        c.assert_table_ready(co.id).await;

        let mut rng = SmallRng::seed_from_u64(0);
        let leading = 10;
        for id in 0..655350 {
            let key = format!("user{id:0leading$}").into_bytes();
            let value = next_bytes(&mut rng, 1024..1025);
            db.put(co.id, key, value).await.unwrap();
        }
        for id in 0..655350 {
            let key = format!("user{id:0leading$}").into_bytes();
            assert!(db.get(co.id, key).await.unwrap().is_some());
        }
    });
}

#[sekas_macro::test]
async fn cluster_rw_put_with_condition() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let k = "book_name".as_bytes().to_vec();
    let v = "rust_in_actions".as_bytes().to_vec();

    // 1. Put if exists failed
    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(k.clone()).expect_exists().ensure_put(v.clone()));
    let r = txn.commit().await;
    info!("put if exists failed: {r:?}");
    assert!(matches!(r, Err(AppError::CasFailed(0, 0, _))));

    // 2. Put if not exists success
    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(k.clone()).expect_not_exists().ensure_put(v.clone()));
    txn.commit().await.unwrap();
    let r = db.get(co.id, k.clone()).await.unwrap();
    let r = r.map(String::from_utf8);
    assert!(matches!(r, Some(Ok(v)) if v == "rust_in_actions"));

    // 3. Put if not exists failed
    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(k.clone()).expect_not_exists().ensure_put(v.clone()));
    let r = txn.commit().await;
    assert!(matches!(r, Err(AppError::CasFailed(0, 0, _))));

    // 4. Put if exists success
    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(k.clone()).expect_exists().ensure_put(v.clone()));
    let r = txn.commit().await;
    assert!(r.is_ok());

    // 5.Put with expected value failed
    let mut txn = db.begin_txn();
    txn.put(
        co.id,
        WriteBuilder::new(k.clone()).expect_value(b"rust".to_vec()).ensure_put(v.clone()),
    );
    let r = txn.commit().await;
    assert!(matches!(r, Err(AppError::CasFailed(0, 0, _))));

    // 6.Put with expected value success
    let mut txn = db.begin_txn();
    txn.put(
        co.id,
        WriteBuilder::new(k.clone())
            .expect_value(b"rust_in_actions".to_vec())
            .ensure_put(v.clone()),
    );
    let r = txn.commit().await;
    assert!(r.is_ok());
}

#[sekas_macro::test]
async fn cluster_rw_concurrent_inc() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let k = "book_name".as_bytes().to_vec();

    let cloned_co = co.clone();
    let cloned_db = db.clone();
    let handle_1 = spawn(async move {
        let k = "book_name".as_bytes().to_vec();
        for _ in 0..1000 {
            let mut txn = cloned_db.begin_txn();
            txn.put(cloned_co.id, WriteBuilder::new(k.clone()).ensure_add(1));
            txn.commit().await.unwrap();
        }
    });

    let cloned_co = co.clone();
    let cloned_db = db.clone();
    let handle_2 = spawn(async move {
        let k = "book_name".as_bytes().to_vec();
        for _ in 0..1000 {
            let mut txn = cloned_db.begin_txn();
            txn.put(cloned_co.id, WriteBuilder::new(k.clone()).ensure_add(1));
            txn.commit().await.unwrap();
        }
    });

    handle_1.await.unwrap();
    handle_2.await.unwrap();

    let expect = 2000i64.to_be_bytes().to_vec();
    let r = db.get(co.id, k.clone()).await.unwrap().unwrap();
    assert_eq!(r, expect);
}

#[sekas_macro::test]
async fn cluster_rw_write_two_table_in_batch() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("db".to_string()).await.unwrap();
    let co1 = db.create_table("co1".to_string()).await.unwrap();
    let co2 = db.create_table("co2".to_string()).await.unwrap();
    c.assert_table_ready(co1.id).await;
    c.assert_table_ready(co2.id).await;

    let k = "book_name".as_bytes().to_vec();
    let v = "rust_in_actions".as_bytes().to_vec();

    let mut txn = db.begin_txn();
    txn.put(co1.id, WriteBuilder::new(k.clone()).ensure_put(v.clone()));
    txn.put(co2.id, WriteBuilder::new(k.clone()).ensure_put(v.clone()));
    txn.commit().await.unwrap();

    let r1 = db.get_raw_value(co1.id, k.clone()).await.unwrap().unwrap();
    let value = r1.content.map(String::from_utf8);
    assert!(matches!(value, Some(Ok(v)) if v == "rust_in_actions"));

    let r2 = db.get_raw_value(co2.id, k).await.unwrap().unwrap();
    let value = r2.content.map(String::from_utf8);
    assert!(matches!(value, Some(Ok(v)) if v == "rust_in_actions"));

    assert_eq!(r1.version, r2.version);
}

#[sekas_macro::test]
async fn cluster_rw_entire_range() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("db".to_string()).await.unwrap();
    let co = db.create_table("co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    for i in 0..100 {
        let k = format!("key {i:010}").into_bytes();
        let v = format!("value {i}").into_bytes();
        println!("write key {:?}", k);

        let mut txn = db.begin_txn();
        txn.put(co.id, WriteBuilder::new(k.clone()).ensure_put(v.clone()));
        txn.commit().await.unwrap();
    }

    let range_request = RangeRequest {
        table_id: co.id,
        version: None,
        range: sekas_client::Range::all(),
        limit: 10,
        limit_bytes: 0,
        buffered_requests: 1,
    };
    let mut range_stream = db.range(range_request).await.unwrap();

    let mut index = 0;
    while let Some(values) = range_stream.next().await {
        for value_set in values.unwrap() {
            assert_eq!(
                value_set.user_key,
                format!("key {index:010}").into_bytes(),
                "current index {index}"
            );
            assert_eq!(value_set.values.len(), 1, "current index {index}");
            assert!(
                matches!(&value_set.values[0].content,
                Some(bytes) if bytes == &format!("value {index}").into_bytes()),
                "current index {index}"
            );
            index += 1;
        }
    }
    assert_eq!(index, 100);
}

#[sekas_macro::test]
async fn cluster_rw_range_with_many_shard() {
    // FIXME(walter) feature split shard is required.
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("db".to_string()).await.unwrap();
    let co = db.create_table("co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    for i in 0..100 {
        let k = format!("key {i:010}").into_bytes();
        let v = format!("value {i}").into_bytes();
        println!("write key {:?}", k);

        let mut txn = db.begin_txn();
        txn.put(co.id, WriteBuilder::new(k.clone()).ensure_put(v.clone()));
        txn.commit().await.unwrap();
    }

    let range_request = RangeRequest {
        table_id: co.id,
        version: None,
        range: sekas_client::Range::all(),
        limit: 10,
        limit_bytes: 0,
        buffered_requests: 1,
    };
    let mut range_stream = db.range(range_request).await.unwrap();

    let mut index = 0;
    while let Some(values) = range_stream.next().await {
        for value_set in values.unwrap() {
            assert_eq!(
                value_set.user_key,
                format!("key {index:010}").into_bytes(),
                "current index {index}"
            );
            assert_eq!(value_set.values.len(), 1, "current index {index}");
            assert!(
                matches!(&value_set.values[0].content,
                Some(bytes) if bytes == &format!("value {index}").into_bytes()),
                "current index {index}"
            );
            index += 1;
        }
    }
    assert_eq!(index, 100);
}

#[sekas_macro::test]
async fn cluster_rw_watch_key() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("db".to_string()).await.unwrap();
    let co = db.create_table("co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    const KEY: &str = "KEY";
    let db_clone = db.clone();
    let table_id = co.id;
    let handle = spawn(async move {
        // watch the key.
        let mut receiver = db_clone.watch(table_id, KEY.as_bytes()).await.unwrap();

        // The first value already exists.
        let value = receiver.next().await.unwrap().unwrap();
        let content = value.content.unwrap();
        info!("first value is {}", sekas_rock::ascii::escape_bytes(&content));
        assert_eq!(content.len(), core::mem::size_of::<i64>());
        let mut buf = [0u8; 8];
        buf[..].copy_from_slice(&content);
        let count = i64::from_be_bytes(buf);
        info!("start count is {}", count);

        for i in (count + 1)..101 {
            let value = receiver.next().await.unwrap().unwrap();
            let content = value.content.unwrap();
            assert_eq!(content.len(), core::mem::size_of::<i64>());
            let mut buf = [0u8; 8];
            buf[..].copy_from_slice(&content);
            let count = i64::from_be_bytes(buf);
            assert_eq!(count, i);
            info!("receive update for count {i}");
        }
    });

    // update
    for _ in 0..100 {
        let mut txn = db.begin_txn();
        txn.put(co.id, WriteBuilder::new(KEY.as_bytes().to_vec()).ensure_add(1));
        txn.commit().await.unwrap();
    }

    handle.await.unwrap();
}

#[ignore]
#[sekas_macro::test]
async fn cluster_rw_watch_key_with_version() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("db".to_string()).await.unwrap();
    let co = db.create_table("co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    const KEY: &str = "key";
    let mut txn = db.begin_txn();
    txn.put(co.id, WriteBuilder::new(KEY.as_bytes().to_vec()).ensure_add(1));
    txn.commit().await.unwrap();
    let version = db.get_raw_value(co.id, KEY.as_bytes().to_vec()).await.unwrap().unwrap().version;

    let db_clone = db.clone();
    let table_id = co.id;
    spawn(async move {
        // watch the key.
        let mut receiver =
            db_clone.watch_with_version(table_id, KEY.as_bytes(), version).await.unwrap();
        for i in 1..101 {
            let value = receiver.next().await.unwrap().unwrap();
            let content = value.content.unwrap();
            assert_eq!(content.len(), core::mem::size_of::<i64>());
            let mut buf = [0u8; 8];
            buf[..].copy_from_slice(&content);
            let count = i64::from_be_bytes(buf);
            assert_eq!(count, i + 1); // the target version should be skipped.
        }
    });

    // update
    for _ in 0..100 {
        let mut txn = db.begin_txn();
        txn.put(co.id, WriteBuilder::new(KEY.as_bytes().to_vec()).ensure_add(1));
        txn.commit().await.unwrap();
    }
}
