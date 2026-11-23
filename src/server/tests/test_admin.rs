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
use prost::Message;
use sekas_api::server::v1::*;
use sekas_client::{ClientOptions, NodeClient, SekasClient};
use sekas_parser::{ColumnResult, ExecuteResult};
use sekas_rock::fn_name;
use sekas_server::diagnosis;

use crate::helper::client::{ClusterClient, node_client_with_retry};
use crate::helper::context::*;
use crate::helper::init::setup_panic_hook;

#[ctor::ctor(unsafe)]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[sekas_macro::test]
async fn admin_show_scheduling() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_num_cpus(2);
    ctx.set_replicas_per_group(1);
    ctx.enable_group_balance();
    ctx.enable_shard_balance();
    ctx.set_schedule_interval_sec(1);
    let nodes = ctx.bootstrap_servers(1).await;
    let client =
        SekasClient::new(ClientOptions::default(), nodes.into_values().collect()).await.unwrap();

    let body = client.handle_statement("show scheduler;").await.unwrap();
    let ExecuteResult::Data(scheduler) = serde_json::from_slice(&body).unwrap() else {
        panic!("show scheduler must return a table");
    };
    assert_eq!(scheduler.columns, ["ready", "running", "waiting", "background_jobs"]);
    assert_eq!(scheduler.rows.len(), 1);
    assert_eq!(scheduler.rows[0].values.len(), 4);

    let balancer = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let body = client.handle_statement("show balancer;").await.unwrap();
            let ExecuteResult::Data(status) = serde_json::from_slice(&body).unwrap() else {
                panic!("show balancer must return a table");
            };
            if status.rows[0].values[1] == "no_action" && status.rows[3].values[1] == "no_action" {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("scheduler must complete a poll");
    assert_eq!(balancer.columns, ["policy", "status", "decision"]);
    assert_eq!(balancer.rows.len(), 4);
    let groups = &balancer.rows[0].values;
    assert_eq!(groups[0], "group_count");
    assert_eq!(groups[1], "no_action");
    assert_eq!(groups[2], "nodes=1, min_nodes=1, cpus=2, groups=2, desired_groups=2");
    assert_eq!(groups.len(), 3);
    assert!(balancer.rows[1..3].iter().all(|row| row.values[1] == "disabled"));
    let shards = &balancer.rows[3].values;
    assert_eq!(shards[0], "shard_count");
    assert_eq!(shards[1], "no_action");
    let decision = shards[2].as_str().unwrap();
    assert!(decision.contains("user_groups=1"), "{decision}");
    assert!(decision.contains("tolerance=0.50"), "{decision}");

    let body = client.handle_statement("show scheduler_tasks;").await.unwrap();
    let ExecuteResult::Data(tasks) = serde_json::from_slice(&body).unwrap() else {
        panic!("show scheduler_tasks must return a table");
    };
    assert_eq!(tasks.columns, ["task", "target", "state", "age_s", "error_retries", "ready_in_s"]);
    assert!(tasks.rows.is_empty());

    let body = client.handle_statement("show scheduler from root;").await.unwrap();
    assert!(matches!(serde_json::from_slice(&body).unwrap(), ExecuteResult::Msg(_)));
}

#[sekas_macro::test]
async fn admin_show_traffic() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    ctx.set_heartbeat_interval_sec(1);
    ctx.set_schedule_interval_sec(1);
    let nodes = ctx.bootstrap_servers(1).await;
    let cluster = ClusterClient::new(nodes).await;
    let client = cluster.app_client().await;
    let empty = client.create_database("empty".into()).await.unwrap();
    let db = client.create_database("traffic".into()).await.unwrap();
    let table = db.create_table("data".into()).await.unwrap();
    cluster.assert_table_ready_with_voters(table.id, 1).await;
    let group = cluster.find_router_group_state_by_key(table.id, b"key").await.unwrap();
    let shard = cluster.get_shard_desc(table.id, b"key").await.unwrap();

    let databases = show(&client, "show databases").await;
    for metric in [
        "bytes",
        "num_shards",
        "read_qps",
        "write_qps",
        "read_bytes_per_sec",
        "write_bytes_per_sec",
    ] {
        assert_eq!(show_cell(&databases, empty.desc().id, metric).as_f64(), Some(0.));
    }

    let shards_cmd = format!("show shards from {}", group.id);
    for (command, id) in [
        ("show tables from traffic", table.id),
        ("show databases", db.desc().id),
        ("show groups", group.id),
        (&shards_cmd, shard.id),
    ] {
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                db.put(table.id, b"key".to_vec(), b"value".to_vec()).await.unwrap();
                assert_eq!(
                    db.get(table.id, b"key".to_vec()).await.unwrap(),
                    Some(b"value".to_vec())
                );
                let result = show(&client, command).await;
                let read = show_cell(&result, id, "read_qps").as_f64().unwrap_or(0.);
                let write = show_cell(&result, id, "write_qps").as_f64().unwrap_or(0.);
                if read > 0. && write > 0. {
                    break (result, read, write);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{command} must report read and write traffic"));
        let (rows, read, write) = result;
        let read_bytes = show_cell(&rows, id, "read_bytes_per_sec").as_f64().unwrap();
        let write_bytes = show_cell(&rows, id, "write_bytes_per_sec").as_f64().unwrap();
        assert!((read_bytes / read - 5.).abs() < 0.0001, "{command} read payload ratio");
        assert!((write_bytes / write - 8.).abs() < 0.0001, "{command} write payload ratio");
        if command.starts_with("show table") || command == "show databases" {
            assert_eq!(show_cell(&rows, id, "num_shards").as_u64(), Some(1));
            assert!(show_cell(&rows, id, "bytes").is_u64());
        }
    }
}

async fn show(client: &SekasClient, statement: &str) -> ColumnResult {
    let body = client.handle_statement(statement).await.unwrap();
    let ExecuteResult::Data(result) = serde_json::from_slice(&body).unwrap() else {
        panic!("{statement} must return a table");
    };
    assert!(result.rows.iter().all(|row| row.values.len() == result.columns.len()));
    result
}

fn show_cell<'a>(data: &'a ColumnResult, id: u64, column: &str) -> &'a serde_json::Value {
    let row = data.rows.iter().find(|row| row.values[0].as_u64() == Some(id)).unwrap();
    let index = data.columns.iter().position(|name| name == column).unwrap();
    &row.values[index]
}

#[sekas_macro::test]
async fn admin_unsplittable_shard_does_not_leave_a_retry_task() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    ctx.set_move_shard_size_limit(1024);
    ctx.set_schedule_interval_sec(1);
    let addr = ctx.next_listen_address();
    let ready = ctx.spawn_server_with_handle(0, &addr, true, vec![]);
    let server = ready.await.unwrap();
    let cluster = ClusterClient::new([(0, addr)].into_iter().collect()).await;
    let client = cluster.app_client().await;
    let db = client.create_database("db".into()).await.unwrap();
    let table = db.create_table("table".into()).await.unwrap();
    cluster.assert_table_ready_with_voters(table.id, 1).await;
    let attempts = || {
        prometheus::gather()
            .iter()
            .find(|family| family.name() == "root_reconcile_scheduler_task_handle_total")
            .and_then(|family| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "type" && label.value() == "split_shard")
                    })
                    .map(|metric| metric.get_counter().value())
            })
            .unwrap_or(0.0)
    };
    let before = attempts();
    // Above the split threshold, but a single user key has no split point.
    db.put(table.id, b"key".to_vec(), vec![b'v'; 8 * 1024]).await.unwrap();
    // Seed the known fixture size independently of RocksDB's approximate
    // sampling, then drive one poll without waiting for a heartbeat.
    let group = cluster.find_router_group_state_by_key(table.id, b"key").await.unwrap();
    let shard = cluster.get_shard_desc(table.id, b"key").await.unwrap();
    let stats = vec![sekas_api::server::v1::GroupStats {
        group_id: group.id,
        shard_count: 1,
        shard_stats: vec![sekas_api::server::v1::ShardStats {
            shard_id: shard.id,
            table_id: table.id,
            shard_size: 8 * 1024,
            traffic: None,
        }],
        ..Default::default()
    }];
    tokio::time::timeout(
        Duration::from_secs(2),
        sekas_server::testutil::drive_scheduling(&server, stats),
    )
    .await
    .expect("one scheduler poll must finish promptly")
    .unwrap();
    assert!(attempts() > before);
    let body = client.handle_statement("show scheduler_tasks;").await.unwrap();
    let ExecuteResult::Data(tasks) = serde_json::from_slice(&body).unwrap() else {
        panic!("show scheduler_tasks must return a table");
    };
    assert!(tasks.rows.is_empty(), "an empty split result must complete the task");
    db.put(table.id, b"key".to_vec(), b"updated".to_vec()).await.unwrap();
    assert_eq!(db.get(table.id, b"key".to_vec()).await.unwrap(), Some(b"updated".to_vec()));
}

#[sekas_macro::test]
async fn admin_balance_init_cluster() {
    let node_count = 4;
    let mut ctx = TestContext::new(fn_name!());
    let start = tokio::time::Instant::now();
    let nodes = ctx.bootstrap_servers(node_count).await;
    let addrs = nodes.values().cloned().collect::<Vec<_>>();
    tokio::time::sleep(Duration::from_secs(10)).await;

    loop {
        let m = current_metadata(addrs.to_owned()).await;
        if m.stable {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    let m = current_metadata(addrs.to_owned()).await;
    assert!(m.balanced);
    assert!(m.groups_ready);
    assert_eq!(m.scheduler_tasks, 0);
    assert_eq!(m.ongoing_jobs, 0);
    assert!(m.stable);
    let stats = m
        .nodes
        .iter()
        .map(|n| {
            let replies = n.replicas.iter().filter(|r| r.group != 0);
            let leaders = replies.to_owned().filter(|r| r.raft_role == 2);
            (n.id, replies.count(), leaders.count())
        })
        .collect::<Vec<_>>();
    info!("{stats:?}, balanced: {}", m.balanced);
    info!("init cluster balance takes {:?}", start.elapsed());
}

#[sekas_macro::test]
async fn admin_update_database_and_table() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    let nodes = ctx.bootstrap_servers(1).await;
    let addrs = nodes.values().cloned().collect::<Vec<_>>();
    let c = SekasClient::new(ClientOptions::default(), addrs).await.unwrap();

    let db = c.create_database("update_db".into()).await.unwrap();
    let updated_db = c.update_database(db.desc()).await.unwrap();
    assert_eq!(updated_db.desc().id, db.desc().id);
    assert_eq!(updated_db.desc().name, db.desc().name);

    let mut table = updated_db.create_table("update_table".into()).await.unwrap();
    table.properties.insert("owner".to_owned(), "admin_update_database_and_table".to_owned());
    table.properties.insert("retention".to_owned(), "7d".to_owned());

    let updated_table = updated_db.update_table(table.clone()).await.unwrap();
    assert_eq!(updated_table.id, table.id);
    assert_eq!(
        updated_table.properties.get("owner").map(String::as_str),
        Some("admin_update_database_and_table")
    );

    let reopened = updated_db.open_table("update_table".into()).await.unwrap();
    assert_eq!(reopened.properties.get("retention").map(String::as_str), Some("7d"));
}

#[sekas_macro::test]
async fn admin_delete() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    ctx.mut_replica_testing_knobs().disable_scheduler_orphan_replica_detecting_intervals = true;
    let nodes = ctx.bootstrap_servers(1).await;
    let addrs = nodes.values().cloned().collect::<Vec<_>>();
    let c = SekasClient::new(ClientOptions::default(), addrs.to_owned()).await.unwrap();
    {
        let db = c.create_database("test1".into()).await.unwrap();
        let c1 = db.create_table("test_co1".into()).await.unwrap();
        db.put(c1.id, "k1".into(), "v1".into()).await.unwrap();
        db.delete_table("test_co1".into()).await.unwrap();
        assert!(db.open_table("test_co1".into()).await.is_err());
        db.create_table("test_co1".into()).await.unwrap();
        let oc2 = db.open_table("test_co1".into()).await.unwrap();
        assert!(db.get(oc2.id, "k1".into()).await.unwrap().is_none())
    }
    {
        c.create_database("test_db1".into()).await.unwrap();
        let db1 = c.open_database("test_db1".into()).await.unwrap();
        db1.create_table("co1".into()).await.unwrap();
        assert!(db1.list_table().await.unwrap().len() == 1);
        c.delete_database("test_db1".into()).await.unwrap();
        assert!(c.open_database("test_db1".into()).await.is_err());
        c.create_database("test_db1".into()).await.unwrap();
        let od2 = c.open_database("test_db1".into()).await.unwrap();
        assert!(od2.list_table().await.unwrap().is_empty());
    }
}

#[sekas_macro::test]
async fn admin_delete_purges_table_shards() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    ctx.mut_replica_testing_knobs().disable_scheduler_orphan_replica_detecting_intervals = true;
    let nodes = ctx.bootstrap_servers(1).await;
    let c = ClusterClient::new(nodes.clone()).await;
    let app = c.app_client().await;

    let db = app.create_database("purge_db".into()).await.unwrap();
    let table = db.create_table("purge_table".into()).await.unwrap();
    db.put(table.id, b"key".to_vec(), b"value".to_vec()).await.unwrap();

    let shard = c.get_shard_desc(table.id, b"key").await.expect("table shard exists");
    let group =
        c.find_router_group_state_by_key(table.id, b"key").await.expect("table group exists");
    assert!(c.group_contains_shard(group.id, shard.id));

    db.delete_table("purge_table".into()).await.unwrap();
    assert!(db.open_table("purge_table".into()).await.is_err());
    wait_shard_removed(&c, group.id, shard.id).await;

    ctx.stop_server(0).await;
    let restarted_addr = nodes.get(&0).unwrap().clone();
    ctx.spawn_server(0, &restarted_addr, false, vec![restarted_addr.clone()]);
    node_client_with_retry(&restarted_addr).await;
    let restarted = ClusterClient::new(nodes.clone()).await;
    wait_shard_removed(&restarted, group.id, shard.id).await;
    let app = restarted.app_client().await;
    let db = app.open_database("purge_db".into()).await.unwrap();

    let recreated = db.create_table("purge_table".into()).await.unwrap();
    assert_ne!(recreated.id, table.id);
    assert!(db.get(recreated.id, b"key".to_vec()).await.unwrap().is_none());

    let db2 = app.create_database("purge_db_all".into()).await.unwrap();
    let table2 = db2.create_table("purge_table".into()).await.unwrap();
    let table3 = db2.create_table("purge_table_extra".into()).await.unwrap();
    db2.put(table2.id, b"key".to_vec(), b"value".to_vec()).await.unwrap();
    db2.put(table3.id, b"key".to_vec(), b"value".to_vec()).await.unwrap();
    let shard2 =
        restarted.get_shard_desc(table2.id, b"key").await.expect("database table shard exists");
    let group2 = restarted
        .find_router_group_state_by_key(table2.id, b"key")
        .await
        .expect("database table group exists");
    let shard3 = restarted
        .get_shard_desc(table3.id, b"key")
        .await
        .expect("database extra table shard exists");
    let group3 = restarted
        .find_router_group_state_by_key(table3.id, b"key")
        .await
        .expect("database extra table group exists");

    app.delete_database("purge_db_all".into()).await.unwrap();
    assert!(app.open_database("purge_db_all".into()).await.is_err());
    wait_shard_removed(&restarted, group2.id, shard2.id).await;
    wait_shard_removed(&restarted, group3.id, shard3.id).await;
}

#[sekas_macro::test]
async fn admin_basic() {
    let node_count = 4;
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(node_count).await;
    let addrs = nodes.values().cloned().collect::<Vec<_>>();

    let c = SekasClient::new(ClientOptions::default(), addrs.to_owned()).await.unwrap();
    let sys_db = c.open_database("__system__".to_owned()).await.unwrap();
    let sys_db_col = sys_db.open_table("database".to_owned()).await.unwrap();
    let sys_col_col = sys_db.open_table("table".to_owned()).await.unwrap();

    // test create database.
    let new_db_name = "db1".to_owned();
    let new_db_id = 2;
    let new_db = {
        let cnt = c.list_database().await.unwrap().len();

        assert!(
            sys_db.get(sys_db_col.id, new_db_name.as_bytes().to_owned()).await.unwrap().is_none()
        );

        let new_db = c.create_database(new_db_name.to_owned()).await.unwrap();

        assert!(c.list_database().await.unwrap().len() == cnt + 1);

        use prost::Message;
        let db_bytes =
            sys_db.get(sys_db_col.id, new_db_name.to_owned().into_bytes()).await.unwrap().unwrap();
        let db_desc = DatabaseDesc::decode(&*db_bytes).unwrap();
        assert!(db_desc.id == new_db_id);

        new_db
    };

    // test create table.
    let new_table_name = "col1".to_owned();
    let cnt = new_db.list_table().await.unwrap().len();
    let value = sys_db.get(sys_col_col.id, table_key(new_db_id, &new_table_name)).await.unwrap();
    assert!(value.is_none());

    new_db.create_table(new_table_name.to_owned()).await.unwrap();
    assert!(new_db.list_table().await.unwrap().len() == cnt + 1);

    let col_bytes =
        sys_db.get(sys_col_col.id, table_key(new_db_id, &new_table_name)).await.unwrap().unwrap();
    let col_desc = TableDesc::decode(&*col_bytes).unwrap();
    assert_eq!(col_desc.name, new_table_name);

    // check meta data api.
    let m = current_metadata(addrs).await;
    let d = m.databases.iter().find(|d| d.name == new_db_name).expect("created database not found");
    d.tables.iter().find(|c| c.name == new_table_name).expect("created table not found");
    assert!(m.nodes.len() == node_count);
}

async fn wait_shard_removed(c: &ClusterClient, group_id: u64, shard_id: u64) {
    for _ in 0..10000 {
        if !c.group_contains_shard(group_id, shard_id) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("group {group_id} still contains shard {shard_id}");
}

fn table_key(database_id: u64, table_name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + table_name.len());
    buf.extend_from_slice(database_id.to_le_bytes().as_slice());
    buf.extend_from_slice(table_name.as_bytes());
    buf
}

async fn current_metadata(nodes: Vec<String>) -> diagnosis::Metadata {
    use vaelo::http::BodyExt as _;

    let root_addr = find_root(nodes).await;
    let remote = root_addr.parse().unwrap();
    let client = vaelo::http::Client::http1(remote).build().unwrap();
    let request = vaelo::http::Request::builder()
        .method(vaelo::http::Method::GET)
        .uri("/admin/metadata")
        .body(vaelo::http::Empty::new())
        .unwrap();
    let response = client.request(request).timeout(Duration::from_secs(30)).await.unwrap();
    assert!(response.status().is_success(), "GET /admin/metadata returned {}", response.status());
    let content = response.into_body().collect().await.unwrap();
    let content = content.chunks().flatten().copied().collect::<Vec<_>>();
    let json_res = serde_json::from_slice(&content);
    json_res.unwrap_or_else(|_| panic!("decode json fail: {:?}", content))
}

async fn find_root(nodes: Vec<String>) -> String {
    for node in nodes {
        let n_cli = NodeClient::connect(node).await;
        if n_cli.is_err() {
            continue;
        }
        let n_cli = n_cli.unwrap();
        let roots = n_cli.get_root().await.unwrap();
        return roots.root_nodes[0].addr.to_owned();
    }
    panic!("no avaliable root")
}
