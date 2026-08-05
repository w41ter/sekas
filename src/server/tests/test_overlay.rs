// Copyright 2026-present The Sekas Authors.
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

use std::sync::Arc;
use std::time::Duration;

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_client::{ClientOptions, GroupClient, SekasClient, WriteBuilder};
use sekas_rock::fn_name;
use sekas_server::TestingBarrier;

use crate::helper::client::ClusterClient;
use crate::helper::context::TestContext;
use crate::helper::init::setup_panic_hook;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

struct OverlayTestCluster {
    _ctx: TestContext,
    barrier: Arc<TestingBarrier>,
    cluster: ClusterClient,
    client: SekasClient,
}

impl OverlayTestCluster {
    async fn new(name: &str) -> Self {
        let barrier = Arc::new(TestingBarrier::new());
        let mut ctx = TestContext::new(name);
        ctx.mut_replica_testing_knobs().after_overlay_insert = Some(barrier.clone());
        let node_addr = ctx.next_listen_address();
        ctx.spawn_server(1, &node_addr, true, vec![]);
        let client =
            SekasClient::new(ClientOptions::default(), vec![node_addr.clone()]).await.unwrap();
        let cluster = ClusterClient::new([(1, node_addr)].into()).await;
        OverlayTestCluster { _ctx: ctx, barrier, cluster, client }
    }

    async fn create_table(&self) -> (sekas_client::Database, TableDesc) {
        let db = self.client.create_database("test_db".to_string()).await.unwrap();
        let table = db.create_table("test_table".to_string()).await.unwrap();
        (db, table)
    }

    async fn wait_key_routed(&self, table_id: u64, key: &[u8]) {
        for _ in 0..1000 {
            if self.cluster.find_router_group_state_by_key(table_id, key).await.is_some()
                && self.cluster.get_shard_desc(table_id, key).await.is_some()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("key {key:?} is not routed for table {table_id}");
    }
}

fn raw_write(shard_id: u64, put: PutRequest) -> Request {
    Request::Write(ShardWriteRequest { shard_id, puts: vec![put], ..Default::default() })
}

async fn request_in_group(
    client: &SekasClient,
    group: sekas_client::RouterGroupState,
    request: &Request,
) -> Response {
    let mut group_client = GroupClient::new(group, client.clone());
    group_client.request(request).await.unwrap()
}

async fn raw_put_in_group(
    client: &SekasClient,
    group: sekas_client::RouterGroupState,
    shard_id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
) -> ShardWriteResponse {
    let request = raw_write(shard_id, PutRequest { key, value, ..Default::default() });
    let Response::Write(resp) = request_in_group(client, group, &request).await else {
        unreachable!()
    };
    resp
}

async fn assert_pending_task<T>(handle: &tokio::task::JoinHandle<T>) {
    tokio::time::timeout(Duration::from_millis(50), async {
        while !handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect_err("task should wait for overlay fence");
}

async fn wait_overlay_inserted(barrier: &TestingBarrier, observed: u64) {
    tokio::time::timeout(Duration::from_secs(5), barrier.wait_reached(observed))
        .await
        .expect("overlay insert hook is not reached");
}

#[sekas_macro::test]
async fn raw_write_reads_pending_overlay_before_returning_cas_failed() {
    let cluster = OverlayTestCluster::new(fn_name!()).await;
    let (db, table) = cluster.create_table().await;
    let key = b"overlay-cas".to_vec();
    cluster.wait_key_routed(table.id, &key).await;
    let group = cluster.cluster.find_router_group_state_by_key(table.id, &key).await.unwrap();
    let shard = cluster.cluster.get_shard_desc(table.id, &key).await.unwrap();

    let observed = cluster.barrier.arm();
    let client = cluster.client.clone();
    let group_for_write1 = group.clone();
    let key_for_write1 = key.clone();
    let barrier = cluster.barrier.clone();
    let write1 = tokio::spawn(async move {
        raw_put_in_group(&client, group_for_write1, shard.id, key_for_write1, b"pending".to_vec())
            .await
    });
    wait_overlay_inserted(&barrier, observed).await;

    assert!(db.get(table.id, key.clone()).await.unwrap().is_none());

    let client = cluster.client.clone();
    let group_for_write2 = group.clone();
    let key_for_write2 = key.clone();
    let write2 = tokio::spawn(async move {
        let put = WriteBuilder::new(key_for_write2)
            .expect_not_exists()
            .ensure_put(b"should-not-write".to_vec());
        let request = raw_write(shard.id, put);
        let Response::Write(resp) = request_in_group(&client, group_for_write2, &request).await
        else {
            unreachable!()
        };
        resp
    });

    assert_pending_task(&write2).await;
    cluster.barrier.release();

    let write1_resp = write1.await.unwrap();
    write1_resp.puts.into_iter().next().unwrap().into_result().unwrap();

    let write2_resp = write2.await.unwrap();
    let err = write2_resp.puts.into_iter().next().unwrap().into_result().unwrap_err();
    assert!(matches!(sekas_client::Error::from(err), sekas_client::Error::CasFailed(0, 0, _)));
    assert_eq!(db.get(table.id, key).await.unwrap(), Some(b"pending".to_vec()));
}

#[sekas_macro::test]
async fn get_and_query_intent_do_not_read_pending_overlay() {
    let cluster = OverlayTestCluster::new(fn_name!()).await;
    let (db, table) = cluster.create_table().await;
    let key = b"overlay-read-boundary".to_vec();
    cluster.wait_key_routed(table.id, &key).await;
    db.put(table.id, key.clone(), b"old".to_vec()).await.unwrap();
    let group = cluster.cluster.find_router_group_state_by_key(table.id, &key).await.unwrap();
    let shard = cluster.cluster.get_shard_desc(table.id, &key).await.unwrap();

    let observed = cluster.barrier.arm();
    let client = cluster.client.clone();
    let group_for_write = group.clone();
    let key_for_write = key.clone();
    let barrier = cluster.barrier.clone();
    let write = tokio::spawn(async move {
        raw_put_in_group(&client, group_for_write, shard.id, key_for_write, b"pending".to_vec())
            .await
    });
    wait_overlay_inserted(&barrier, observed).await;

    assert_eq!(db.get(table.id, key.clone()).await.unwrap(), Some(b"old".to_vec()));

    let mut group_client = GroupClient::new(group, cluster.client.clone());
    let query = Request::QueryIntent(QueryIntentRequest {
        start_version: 1,
        shard_keys: vec![ShardKey { shard_id: shard.id, user_key: key.clone() }],
    });
    let Response::QueryIntent(resp) = group_client.request(&query).await.unwrap() else {
        unreachable!()
    };
    assert!(resp.shard_keys[0].clone().into_result().is_ok());
    assert!(resp.shard_keys[0].clone().into_result().unwrap().is_none());

    cluster.barrier.release();
    write.await.unwrap().puts.into_iter().next().unwrap().into_result().unwrap();
}

#[sekas_macro::test]
async fn write_intent_reads_pending_overlay_value() {
    let cluster = OverlayTestCluster::new(fn_name!()).await;
    let (db, table) = cluster.create_table().await;
    let key = b"overlay-intent".to_vec();
    cluster.wait_key_routed(table.id, &key).await;
    let group = cluster.cluster.find_router_group_state_by_key(table.id, &key).await.unwrap();
    let shard = cluster.cluster.get_shard_desc(table.id, &key).await.unwrap();

    let observed = cluster.barrier.arm();
    let client = cluster.client.clone();
    let group_for_write = group.clone();
    let key_for_write = key.clone();
    let barrier = cluster.barrier.clone();
    let write = tokio::spawn(async move {
        raw_put_in_group(&client, group_for_write, shard.id, key_for_write, b"pending".to_vec())
            .await
    });
    wait_overlay_inserted(&barrier, observed).await;

    let mut txn = db.begin_txn();
    txn.put(
        table.id,
        WriteBuilder::new(key.clone())
            .expect_value(b"pending".to_vec())
            .ensure_put(b"txn".to_vec()),
    );
    let commit = tokio::spawn(async move { txn.commit().await });
    assert_pending_task(&commit).await;

    cluster.barrier.release();
    write.await.unwrap().puts.into_iter().next().unwrap().into_result().unwrap();
    commit.await.unwrap().unwrap();
    assert_eq!(db.get(table.id, key).await.unwrap(), Some(b"txn".to_vec()));
}

#[sekas_macro::test]
async fn pending_clear_intent_hides_engine_intent_from_following_write() {
    let cluster = OverlayTestCluster::new(fn_name!()).await;
    let (db, table) = cluster.create_table().await;
    let key = b"overlay-clear-intent".to_vec();
    cluster.wait_key_routed(table.id, &key).await;

    let start_version = db.begin_txn().start_version().await.unwrap();
    let txn_table =
        sekas_client::TxnStateTable::new(cluster.client.clone(), Some(Duration::from_secs(5)));
    txn_table.begin_txn(start_version).await.unwrap();

    let group = cluster.cluster.find_router_group_state_by_key(table.id, &key).await.unwrap();
    let shard = cluster.cluster.get_shard_desc(table.id, &key).await.unwrap();
    let write_intent = Request::WriteIntent(WriteIntentRequest {
        start_version,
        writes: vec![ShardWriteRequest {
            shard_id: shard.id,
            puts: vec![WriteBuilder::new(key.clone()).ensure_put(b"intent".to_vec())],
            deletes: Vec::new(),
        }],
        check_write_conflict: false,
        async_commit: false,
        deadline_ms: 0,
    });
    let mut group_client = GroupClient::new(group.clone(), cluster.client.clone());
    let Response::WriteIntent(resp) = group_client.request(&write_intent).await.unwrap() else {
        unreachable!()
    };
    resp.writes.into_iter().next().unwrap().into_result().unwrap();

    let observed = cluster.barrier.arm();
    let clear = Request::ClearIntent(ClearIntentRequest {
        start_version,
        shard_keys: vec![ShardKey { shard_id: shard.id, user_key: key.clone() }],
    });
    let client = cluster.client.clone();
    let group_for_clear = group.clone();
    let barrier = cluster.barrier.clone();
    let clear_task =
        tokio::spawn(async move { request_in_group(&client, group_for_clear, &clear).await });
    wait_overlay_inserted(&barrier, observed).await;

    let mut txn = db.begin_txn();
    txn.put(
        table.id,
        WriteBuilder::new(key.clone()).expect_not_exists().ensure_put(b"after-clear".to_vec()),
    );
    let commit = tokio::spawn(async move { txn.commit().await });
    assert_pending_task(&commit).await;

    cluster.barrier.release();
    let Response::ClearIntent(resp) = clear_task.await.unwrap() else { unreachable!() };
    resp.shard_keys.into_iter().next().unwrap().into_result().unwrap();
    commit.await.unwrap().unwrap();

    assert_eq!(db.get(table.id, key).await.unwrap(), Some(b"after-clear".to_vec()));
}
