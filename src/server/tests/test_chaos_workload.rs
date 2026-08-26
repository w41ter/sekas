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

use helper::client::ClusterClient;
use helper::context::TestContext;
use helper::init::setup_panic_hook;
use sekas_chaos::{OperationWeights, SekasExecutor, SizeRange, WorkloadConfig, WorkloadRunner};
use sekas_rock::fn_name;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[sekas_macro::test]
async fn stable_workload_matches_final_model() {
    let mut context = TestContext::new(fn_name!());
    let nodes = context.bootstrap_servers(3).await;
    let cluster = ClusterClient::new(nodes).await;
    let client = cluster.app_client().await;
    let database = client.create_database("chaos-workload-db".to_string()).await.unwrap();
    let first = database.create_table("chaos-workload-a".to_string()).await.unwrap();
    let second = database.create_table("chaos-workload-b".to_string()).await.unwrap();
    cluster.assert_table_ready(first.id).await;
    cluster.assert_table_ready(second.id).await;

    let config = WorkloadConfig {
        run_id: fn_name!().to_string(),
        tables: vec![first.id, second.id],
        seed: 0x5eca5,
        writers: 2,
        readers: 2,
        slots_per_writer: 32,
        operations_per_writer: Some(40),
        value_size: SizeRange::new(8, 64),
        verify_width: SizeRange::new(1, 8),
        batch_width: SizeRange::new(2, 6),
        transaction_width: SizeRange::new(2, 6),
        operation_weights: OperationWeights {
            put: 4,
            delete: 2,
            batch: 2,
            transaction: 2,
            add_i64: 0,
        },
        reconciliation_timeout: Duration::from_secs(10),
        ..WorkloadConfig::default()
    };
    let executor = Arc::new(SekasExecutor::new(database));
    let report = WorkloadRunner::new(config, executor).unwrap().start().wait().await;
    assert!(report.is_valid(), "workload failed: {:#?}", report.failures);
    assert_eq!(report.stats.operations, 80);

    drop(cluster);
    drop(context);
}
