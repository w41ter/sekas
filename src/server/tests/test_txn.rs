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
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
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
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let table_a = db.create_table("test_table_a".to_string()).await.unwrap();
    let table_b = db.create_table("test_table_b".to_string()).await.unwrap();
    c.assert_table_ready(table_a.id).await;
    c.assert_table_ready(table_b.id).await;

    let key_a = b"batch_key_a".to_vec();
    let key_b = b"batch_key_b".to_vec();
    let key_c = b"batch_key_c".to_vec();

    let mut txn = db.begin_txn();
    txn.put(table_a.id, WriteBuilder::new(key_a.clone()).ensure_put(b"a".to_vec()));
    txn.put(table_b.id, WriteBuilder::new(key_b.clone()).ensure_put(b"b".to_vec()));
    txn.put(table_a.id, WriteBuilder::new(key_c.clone()).ensure_add(7));
    let resp = txn.commit().await.unwrap();
    assert!(resp.version > 0);

    assert_eq!(db.get(table_a.id, key_a).await.unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.get(table_b.id, key_b).await.unwrap(), Some(b"b".to_vec()));
    assert_eq!(db.get(table_a.id, key_c).await.unwrap(), Some(7_i64.to_be_bytes().to_vec()));
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
async fn txn_read_resolves_committed_orphan_intent() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let key = b"orphan_intent_key".to_vec();
    let value = b"committed_value".to_vec();

    let txn = db.begin_txn();
    let start_version = txn.start_version().await.unwrap();
    drop(txn);
    let commit_version = start_version + 1;

    let shard = c.get_shard_desc(co.id, &key).await.unwrap();
    let group_state = c.find_router_group_state_by_key(co.id, &key).await.unwrap();
    let mut group_client = sekas_client::GroupClient::new(group_state, app.clone());
    group_client
        .request(&Request::WriteIntent(WriteIntentRequest {
            start_version,
            writes: vec![ShardWriteRequest {
                shard_id: shard.id,
                puts: vec![WriteBuilder::new(key.clone()).ensure_put(value.clone())],
                deletes: Vec::new(),
            }],
        }))
        .await
        .expect("write intent with partial forward should succeed");

    let txn_table = sekas_client::TxnStateTable::new(app.clone(), Some(Duration::from_secs(5)));
    txn_table.begin_txn(start_version).await.unwrap();
    txn_table.commit_txn(start_version, commit_version).await.unwrap();

    assert_eq!(db.get(co.id, key.clone()).await.unwrap(), Some(value));
    let resolved = db.get_raw_value(co.id, key).await.unwrap().unwrap();
    assert_eq!(resolved.version, commit_version);
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

#[sekas_macro::test]
async fn txn_prepare_partial_success_reports_per_entry_results() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let prepared_key = b"partial_prepared_key".to_vec();
    let conflict_key = b"partial_conflict_key".to_vec();
    db.put(co.id, conflict_key.clone(), b"initial".to_vec()).await.unwrap();

    let txn = db.begin_txn();
    let start_version = txn.start_version().await.unwrap();
    drop(txn);

    let prepared_shard = c.get_shard_desc(co.id, &prepared_key).await.unwrap();
    let conflict_shard = c.get_shard_desc(co.id, &conflict_key).await.unwrap();
    let group_state = c.find_router_group_state_by_key(co.id, &prepared_key).await.unwrap();
    assert_eq!(
        group_state.id,
        c.find_router_group_state_by_key(co.id, &conflict_key).await.unwrap().id
    );

    let mut group_client = sekas_client::GroupClient::new(group_state, app.clone());
    let resp = group_client
        .request(&Request::WriteIntent(WriteIntentRequest {
            start_version,
            writes: vec![
                ShardWriteRequest {
                    shard_id: prepared_shard.id,
                    puts: vec![
                        WriteBuilder::new(prepared_key.clone()).ensure_put(b"prepared".to_vec()),
                    ],
                    deletes: Vec::new(),
                },
                ShardWriteRequest {
                    shard_id: conflict_shard.id,
                    puts: vec![
                        WriteBuilder::new(conflict_key.clone())
                            .expect_not_exists()
                            .ensure_put(b"should_not_commit".to_vec()),
                    ],
                    deletes: Vec::new(),
                },
            ],
        }))
        .await
        .unwrap();

    let Response::WriteIntent(resp) = resp else { panic!("WriteIntentResponse is required") };
    assert_eq!(resp.writes.len(), 2);
    assert!(matches!(
        Error::from(resp.writes[0].clone().into_result().unwrap_err()),
        Error::NotFound(_)
    ));
    assert!(matches!(
        Error::from(resp.writes[1].clone().into_result().unwrap_err()),
        Error::CasFailed(1, 0, _)
    ));

    assert!(db.get(co.id, prepared_key).await.unwrap().is_none());
    assert_eq!(db.get(co.id, conflict_key).await.unwrap(), Some(b"initial".to_vec()));
}

#[sekas_macro::test]
async fn txn_write_intent_batch_deduplicates_same_key_latches() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database("test_db".to_string()).await.unwrap();
    let co = db.create_table("test_co".to_string()).await.unwrap();
    c.assert_table_ready(co.id).await;

    let key = b"same_key_put_delete".to_vec();
    let txn = db.begin_txn();
    let start_version = txn.start_version().await.unwrap();
    drop(txn);

    let shard = c.get_shard_desc(co.id, &key).await.unwrap();
    let group_state = c.find_router_group_state_by_key(co.id, &key).await.unwrap();
    let mut group_client = sekas_client::GroupClient::new(group_state, app.clone());
    let request = Request::WriteIntent(WriteIntentRequest {
        start_version,
        writes: vec![
            ShardWriteRequest {
                shard_id: shard.id,
                puts: vec![WriteBuilder::new(key.clone()).ensure_put(b"value".to_vec())],
                deletes: Vec::new(),
            },
            ShardWriteRequest {
                shard_id: shard.id,
                deletes: vec![WriteBuilder::new(key.clone()).ensure_delete()],
                puts: Vec::new(),
            },
        ],
    });

    let resp = tokio::time::timeout(Duration::from_secs(5), group_client.request(&request))
        .await
        .expect("duplicated latch acquisition should not block")
        .unwrap();
    let Response::WriteIntent(resp) = resp else { panic!("WriteIntentResponse is required") };
    assert_eq!(resp.writes.len(), 2);
    assert!(resp.writes[0].clone().into_result().is_ok());
    assert!(resp.writes[1].clone().into_result().is_ok());
}

#[sekas_macro::test]
async fn txn_intent_batch_partially_forwards_moving_shard() {
    use collect_moving_shard_state_response::State;

    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(2).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let src_group_id = 100000;
    let dest_group_id = 100001;
    let moving_shard_id = 10000000;
    let local_shard_id = 10000001;
    let table_id = 424242;
    let moving_shard = ShardDesc::with_range(moving_shard_id, table_id, vec![], b"m".to_vec());
    let local_shard = ShardDesc::with_range(local_shard_id, table_id, b"m".to_vec(), vec![]);

    create_txn_test_group(
        &c,
        src_group_id,
        vec![0],
        vec![moving_shard.clone(), local_shard.clone()],
    )
    .await;
    create_txn_test_group(&c, dest_group_id, vec![1], vec![moving_shard.clone()]).await;
    c.assert_group_leader(src_group_id).await;
    c.assert_group_leader(dest_group_id).await;

    let src_epoch = c.must_group_epoch(src_group_id).await;
    let dest_epoch = c.must_group_epoch(dest_group_id).await;
    let desc = MoveShardDesc {
        shard_desc: Some(moving_shard.clone()),
        src_group_id,
        src_group_epoch: src_epoch,
        dest_group_id,
        dest_group_epoch: dest_epoch,
    };
    c.group(src_group_id).acquire_shard(&desc).await.unwrap();
    wait_moving_shard_state(&c, src_group_id, &[State::Prepare, State::Moving]).await;

    let db = app.create_database("manual_txn_db".to_string()).await.unwrap();
    let txn = db.begin_txn();
    let start_version = txn.start_version().await.unwrap();
    drop(txn);
    let moving_key_a = b"b_moving_key_a".to_vec();
    let moving_key_b = b"c_moving_key_b".to_vec();
    let local_key = b"z_local_key".to_vec();

    let mut group_client = sekas_client::GroupClient::new(
        c.get_router_group_state(src_group_id).await.unwrap(),
        app.clone(),
    );
    let resp = group_client
        .request(&Request::WriteIntent(WriteIntentRequest {
            start_version,
            writes: vec![
                ShardWriteRequest {
                    shard_id: moving_shard_id,
                    puts: vec![
                        WriteBuilder::new(moving_key_a.clone()).ensure_put(b"moving-a".to_vec()),
                    ],
                    deletes: Vec::new(),
                },
                ShardWriteRequest {
                    shard_id: moving_shard_id,
                    puts: vec![
                        WriteBuilder::new(moving_key_b.clone()).ensure_put(b"moving-b".to_vec()),
                    ],
                    deletes: Vec::new(),
                },
                ShardWriteRequest {
                    shard_id: local_shard_id,
                    puts: vec![
                        WriteBuilder::new(local_key.clone()).ensure_put(b"local".to_vec()),
                    ],
                    deletes: Vec::new(),
                },
            ],
        }))
        .await
        .unwrap();

    let Response::WriteIntent(resp) = resp else { panic!("WriteIntentResponse is required") };
    assert_eq!(resp.writes.len(), 3);
    assert!(resp.writes[0].clone().into_result().is_ok());
    assert!(resp.writes[1].clone().into_result().is_ok());
    assert!(resp.writes[2].clone().into_result().is_ok());

    let txn_table = sekas_client::TxnStateTable::new(app.clone(), Some(Duration::from_secs(5)));
    txn_table.begin_txn(start_version).await.unwrap();
    let commit_version = start_version + 1;
    txn_table.commit_txn(start_version, commit_version).await.unwrap();

    let resp = group_client
        .request(&Request::CommitIntent(CommitIntentRequest {
            start_version,
            commit_version,
            shard_keys: vec![
                ShardKey { shard_id: moving_shard_id, user_key: moving_key_a.clone() },
                ShardKey { shard_id: moving_shard_id, user_key: moving_key_b.clone() },
                ShardKey { shard_id: local_shard_id, user_key: local_key.clone() },
            ],
        }))
        .await
        .unwrap();
    let Response::CommitIntent(resp) = resp else { panic!("CommitIntentResponse is required") };
    assert_eq!(resp.shard_keys.len(), 3);
    assert!(resp.shard_keys[0].clone().into_result().is_ok());
    assert!(resp.shard_keys[1].clone().into_result().is_ok());
    assert!(resp.shard_keys[2].clone().into_result().is_ok());

    let mut dest_client = c.group(dest_group_id);
    assert_eq!(
        get_from_group(&mut dest_client, moving_shard_id, moving_key_a).await,
        Some(b"moving-a".to_vec())
    );
    assert_eq!(
        get_from_group(&mut dest_client, moving_shard_id, moving_key_b).await,
        Some(b"moving-b".to_vec())
    );
    let mut src_client = c.group(src_group_id);
    assert_eq!(
        get_from_group(&mut src_client, local_shard_id, local_key).await,
        Some(b"local".to_vec())
    );
}

async fn create_txn_test_group(
    c: &ClusterClient,
    group_id: u64,
    nodes: Vec<u64>,
    shards: Vec<ShardDesc>,
) {
    let replicas = nodes
        .iter()
        .cloned()
        .map(|node_id| ReplicaDesc {
            id: group_id * 10 + node_id,
            node_id,
            role: ReplicaRole::Voter as i32,
        })
        .collect::<Vec<_>>();
    let group_desc =
        GroupDesc { id: group_id, shards, replicas: replicas.clone(), ..Default::default() };
    for replica in replicas {
        c.create_replica(replica.node_id, replica.id, group_desc.clone()).await;
    }
}

async fn wait_moving_shard_state(
    c: &ClusterClient,
    group_id: u64,
    expect: &[collect_moving_shard_state_response::State],
) {
    for _ in 0..1000 {
        if let Some(node_id) = c.get_group_leader_node_id(group_id).await
            && let Ok(resp) = c.collect_moving_shard_state(group_id, node_id).await
            && expect.iter().any(|state| resp.state == *state as i32)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("group {group_id} did not enter expected moving shard state");
}

async fn get_from_group(
    client: &mut sekas_client::GroupClient,
    shard_id: u64,
    key: Vec<u8>,
) -> Option<Vec<u8>> {
    let resp = client
        .request(&Request::Get(ShardGetRequest {
            shard_id,
            start_version: u64::MAX,
            user_key: key,
        }))
        .await
        .unwrap();
    let Response::Get(resp) = resp else { panic!("ShardGetResponse is required") };
    resp.value.and_then(|value| value.content)
}
