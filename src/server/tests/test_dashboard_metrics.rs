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

use futures::StreamExt;
use sekas_api::server::v1::ShardScanRequest;
use sekas_client::WriteBuilder;
use sekas_rock::fn_name;

use crate::helper::client::ClusterClient;
use crate::helper::context::TestContext;
use crate::helper::init::setup_panic_hook;

#[ctor::ctor(unsafe)]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[sekas_macro::test]
async fn dashboard_metrics_follow_logical_operations_and_txn_paths() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.set_replicas_per_group(1);
    let cluster = ClusterClient::new(ctx.bootstrap_servers(1).await).await;
    let client = cluster.app_client().await;
    let db = client.create_database("metrics_db".into()).await.unwrap();
    let table_id = db.create_table("metrics_table".into()).await.unwrap().id;
    cluster.assert_table_ready_with_voters(table_id, 1).await;

    let key = b"key";
    let mut watch = db.watch(table_id, key).await.unwrap();
    db.put(table_id, key.to_vec(), b"value".to_vec()).await.unwrap();
    let update =
        tokio::time::timeout(Duration::from_secs(5), watch.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(update.content.as_deref(), Some(b"value".as_slice()));
    drop(watch);
    assert_eq!(db.get(table_id, key.to_vec()).await.unwrap().as_deref(), Some(b"value".as_slice()));
    let shard_id = cluster.get_shard_desc(table_id, key).await.unwrap().id;
    let scan = db
        .scan(ShardScanRequest { shard_id, prefix: Some(key.to_vec()), ..Default::default() })
        .await
        .unwrap();
    assert_eq!(scan.data.len(), 1);
    db.delete(table_id, key.to_vec()).await.unwrap();

    let mut txn = db.begin_txn();
    txn.start_version().await.unwrap();
    txn.put(table_id, WriteBuilder::new(b"a".to_vec()).ensure_put(b"abc".to_vec()));
    txn.put(table_id, WriteBuilder::new(b"b".to_vec()).ensure_put(b"de".to_vec()));
    txn.commit().await.unwrap();

    let families = prometheus::gather();
    let metric = |family, labels: &[(&str, &str)]| {
        families
            .iter()
            .find(|f| f.name() == family)
            .unwrap()
            .get_metric()
            .iter()
            .find(|m| {
                labels.iter().all(|&(name, value)| {
                    m.get_label().iter().any(|label| label.name() == name && label.value() == value)
                })
            })
            .unwrap()
    };
    for (operation, input, output) in [
        ("put", 8.0, 0.0),
        ("get", 3.0, 5.0),
        ("scan", 3.0, 8.0),
        ("delete", 3.0, 0.0),
        ("commit", 7.0, 0.0),
    ] {
        let labels = [("type", operation)];
        assert_eq!(metric("client_database_request_total", &labels).get_counter().value(), 1.0);
        assert_eq!(
            metric("client_database_request_duration_seconds", &labels)
                .get_histogram()
                .sample_count(),
            1,
        );
        for (direction, bytes) in [("input", input), ("output", output)] {
            assert_eq!(
                metric(
                    "client_database_operation_bytes_total",
                    &[("type", operation), ("direction", direction)]
                )
                .get_counter()
                .value(),
                bytes,
            );
        }
    }
    for (path, count, bytes) in [("local", 2, 11.0), ("commit", 3, 18.0)] {
        let labels = [("path", path)];
        assert_eq!(
            metric("client_txn_duration_seconds", &labels).get_histogram().sample_count(),
            count
        );
        assert_eq!(
            metric("client_txn_committed_bytes_total", &labels).get_counter().value(),
            bytes
        );
    }
    for phase in ["begin", "prepare", "commit", "dispatch_commit_intents"] {
        assert_eq!(
            metric("client_txn_phase_duration_seconds", &[("phase", phase)])
                .get_histogram()
                .sample_count(),
            1,
        );
    }
    let keys = metric("client_txn_commit_keys", &[]).get_histogram();
    assert_eq!(keys.sample_count(), 3);
    assert_eq!(keys.get_sample_sum(), 4.0);
    for (family, watch_label) in [
        ("group_client_group_request_bytes_total", "watch"),
        ("node_service_group_request_bytes_total", "watch_key"),
    ] {
        for operation in ["get", watch_label] {
            for direction in ["sent", "received"] {
                assert!(
                    metric(family, &[("type", operation), ("direction", direction)])
                        .get_counter()
                        .value()
                        > 0.0
                );
            }
        }
    }
    assert!(metric("raftgroup_propose_bytes_total", &[]).get_counter().value() > 0.0);
}
