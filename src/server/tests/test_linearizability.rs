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

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use helper::client::{ClusterClient, node_client_with_retry};
use helper::context::TestContext;
use helper::init::setup_panic_hook;
use helper::runtime::spawn;
use log::info;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use sekas_checker::history::{Call, CallResult, History, KvOp};
use sekas_checker::report::RunReport;
use sekas_checker::{CheckOutcome, LinearizabilityChecker};
use sekas_client::{AppError, Database};
use sekas_rock::fn_name;
use sekas_runtime::ExecutorOwner;
use tokio::sync::{Barrier, Mutex};

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[derive(Clone)]
struct Recorder {
    next_id: Arc<AtomicU64>,
    start: Instant,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl Recorder {
    fn new() -> Self {
        Recorder {
            next_id: Arc::new(AtomicU64::new(1)),
            start: Instant::now(),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn record_call<T>(
        &self,
        process: usize,
        op: KvOp,
        future: impl Future<Output = T>,
        map_result: impl FnOnce(T) -> CallResult,
    ) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let invoke_time = self.elapsed_micros();
        let result = future.await;
        let complete_time = self.elapsed_micros();
        let result = map_result(result);
        self.calls.lock().await.push(Call { id, process, op, result, invoke_time, complete_time });
    }

    async fn history(&self) -> History {
        let mut calls = self.calls.lock().await.clone();
        calls.sort_by_key(|call| (call.invoke_time, call.complete_time, call.id));
        History::from_calls(calls)
    }

    async fn assert_all_workers_active(&self, workers: usize) {
        let calls = self.calls.lock().await;
        let mut counts = vec![0usize; workers];
        for call in calls.iter().filter(|call| call.process < workers) {
            counts[call.process] += 1;
        }
        assert!(
            counts.iter().all(|count| *count > 0),
            "not all workers executed operations: {counts:?}"
        );
    }

    fn elapsed_micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

#[derive(Clone, Copy, Debug)]
enum KvAction {
    Get,
    Put,
    Delete,
}

async fn bootstrap_linearizability_cluster(
    name: &str,
    enable_group_balance: bool,
) -> (TestContext, ClusterClient, Database, u64, HashMap<u64, String>) {
    let mut ctx = TestContext::new(name);
    if enable_group_balance {
        ctx.set_num_cpus(3);
        ctx.enable_group_balance();
    }

    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes.clone()).await;
    let app = c.app_client().await;
    let db = app.create_database(format!("{name}_db")).await.unwrap();
    let table = db.create_table(format!("{name}_table")).await.unwrap();
    c.assert_table_ready(table.id).await;
    (ctx, c, db, table.id, nodes)
}

async fn run_kv_workload(
    db: Database,
    table_id: u64,
    recorder: Recorder,
    workers: usize,
    ops_per_worker: usize,
    keys: Vec<Vec<u8>>,
    seed: u64,
) {
    let start = Arc::new(Barrier::new(workers));
    let mut handles = Vec::with_capacity(workers);
    for process in 0..workers {
        let db = db.clone();
        let recorder = recorder.clone();
        let keys = keys.clone();
        let start = start.clone();
        handles.push(spawn(async move {
            let mut rng = SmallRng::seed_from_u64(seed + process as u64);
            start.wait().await;
            for seq in 0..ops_per_worker {
                let key = keys[rng.gen_range(0..keys.len())].clone();
                let action = match rng.gen_range(0..10) {
                    0..=3 => KvAction::Get,
                    4..=8 => KvAction::Put,
                    _ => KvAction::Delete,
                };
                execute_action(&db, table_id, &recorder, process, seq, key, action).await;
                sekas_runtime::yield_now().await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
    recorder.assert_all_workers_active(workers).await;
}

fn numbered_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
    (0..count).map(|idx| format!("{prefix}-{idx:02}").into_bytes()).collect()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

async fn execute_action(
    db: &Database,
    table_id: u64,
    recorder: &Recorder,
    process: usize,
    seq: usize,
    key: Vec<u8>,
    action: KvAction,
) {
    match action {
        KvAction::Get => {
            let op = KvOp::Get { key: key.clone() };
            recorder.record_call(process, op, db.get(table_id, key), map_get_result).await;
        }
        KvAction::Put => {
            let value = format!("p{process}-s{seq}").into_bytes();
            let op = KvOp::Put { key: key.clone(), value: value.clone() };
            recorder.record_call(process, op, db.put(table_id, key, value), map_put_result).await;
        }
        KvAction::Delete => {
            let op = KvOp::Delete { key: key.clone() };
            recorder.record_call(process, op, db.delete(table_id, key), map_delete_result).await;
        }
    }
}

fn map_get_result(result: Result<Option<Vec<u8>>, AppError>) -> CallResult {
    match result {
        Ok(value) => CallResult::Get(value),
        Err(err) => CallResult::Info(format!("{err:?}")),
    }
}

fn map_put_result(result: Result<(), AppError>) -> CallResult {
    match result {
        Ok(()) => CallResult::Put,
        Err(err) => CallResult::Info(format!("{err:?}")),
    }
}

fn map_delete_result(result: Result<(), AppError>) -> CallResult {
    match result {
        Ok(()) => CallResult::Delete,
        Err(err) => CallResult::Info(format!("{err:?}")),
    }
}

fn assert_linearizable(name: &str, seed: u64, history: History) {
    let report = LinearizabilityChecker::new().with_max_calls_per_key(120).check(&history);
    if !matches!(report.outcome, CheckOutcome::Valid) {
        let dir = std::path::Path::new("target").join("linearizability");
        std::fs::create_dir_all(&dir).unwrap();
        let filename = format!("{}-{seed:x}.json", sanitize_filename(name));
        let path = dir.join(filename);
        RunReport { name: name.to_string(), seed, history, check: report.clone() }
            .write_json(&path)
            .unwrap();
        let key_summary = report
            .keys
            .iter()
            .map(|key| {
                format!(
                    "key={} calls={} outcome={:?}",
                    String::from_utf8_lossy(&key.key),
                    key.calls,
                    key.outcome
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        panic!(
            "history is not linearizable; report written to {}\n{}",
            path.display(),
            key_summary
        );
    }
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' { ch } else { '-' })
        .collect()
}

fn run_linearizability_test(future: impl std::future::Future<Output = ()> + Send) {
    ExecutorOwner::new(4).executor().block_on(future);
}

#[ignore]
#[test]
fn linearizable_kv_basic_moderate() {
    run_linearizability_test(async {
        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), false).await;
        for seed in [0xC0FFEE, 0xBADCAFE, 0x5ECA5] {
            let recorder = Recorder::new();
            let prefix = format!("linear-seed-{seed:x}");
            run_kv_workload(
                db.clone(),
                table_id,
                recorder.clone(),
                8,
                18,
                vec![
                    format!("{prefix}-a").into_bytes(),
                    format!("{prefix}-b").into_bytes(),
                    format!("{prefix}-c").into_bytes(),
                    format!("{prefix}-d").into_bytes(),
                ],
                seed,
            )
            .await;
            assert_linearizable(fn_name!(), seed, recorder.history().await);
        }
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_basic_stress() {
    run_linearizability_test(async {
        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), false).await;
        for seed in [0xC0FFEE, 0xBADCAFE, 0x5ECA5] {
            let recorder = Recorder::new();
            let prefix = format!("linear-stress-seed-{seed:x}");
            run_kv_workload(
                db.clone(),
                table_id,
                recorder.clone(),
                16,
                40,
                (0..16).map(|idx| format!("{prefix}-{idx:02}").into_bytes()).collect(),
                seed,
            )
            .await;
            assert_linearizable(fn_name!(), seed, recorder.history().await);
        }
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_with_leader_transfer() {
    run_linearizability_test(async {
        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), false).await;
        let key = b"linear-transfer-key".to_vec();
        let group = c.find_router_group_state_by_key(table_id, &key).await.unwrap().id;
        c.assert_num_group_voters(group, 3).await;

        let recorder = Recorder::new();
        let workload = run_kv_workload(
            db,
            table_id,
            recorder.clone(),
            16,
            32,
            numbered_keys("linear-transfer-key", 8),
            0xBAD5EED,
        );
        let nemesis = async {
            for _ in 0..20 {
                sekas_runtime::time::sleep(Duration::from_millis(10)).await;
                let _ = c.transfer_group_leader_randomly(group).await;
            }
        };
        tokio::join!(workload, nemesis);

        assert_linearizable(fn_name!(), 0xBAD5EED, recorder.history().await);
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_with_node_restart() {
    run_linearizability_test(async {
        let (mut ctx, c, db, table_id, nodes) =
            bootstrap_linearizability_cluster(fn_name!(), false).await;
        let root_addr = nodes.get(&0).unwrap().clone();
        let restarted_addr = nodes.get(&2).unwrap().clone();
        let recorder = Recorder::new();
        let workload = run_kv_workload(
            db,
            table_id,
            recorder.clone(),
            16,
            32,
            numbered_keys("linear-restart-key", 8),
            0xFACE,
        );
        let nemesis = async {
            sekas_runtime::time::sleep(Duration::from_millis(15)).await;
            ctx.stop_server(2).await;
            sekas_runtime::time::sleep(Duration::from_millis(80)).await;
            ctx.spawn_server(2, &restarted_addr, false, vec![root_addr]);
            node_client_with_retry(&restarted_addr).await;
        };
        tokio::join!(workload, nemesis);

        assert_linearizable(fn_name!(), 0xFACE, recorder.history().await);
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_with_shard_moving() {
    run_linearizability_test(async {
        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), true).await;
        c.assert_num_group_voters(2, 3).await;
        let key = b"linear-moving-key-00".to_vec();
        let recorder = Recorder::new();
        let workload = run_kv_workload(
            db,
            table_id,
            recorder.clone(),
            4,
            8,
            numbered_keys("linear-moving-key", 8),
            0x51A7E,
        );
        let nemesis = async {
            sekas_runtime::time::sleep(Duration::from_millis(10)).await;
            let source_state = c.find_router_group_state_by_key(table_id, &key).await.unwrap();
            if source_state.id != 2 {
                let shard_desc = c.get_shard_desc(table_id, &key).await.unwrap();
                let mut target = c.group(2);
                let result =
                    target.accept_shard(source_state.id, source_state.epoch, &shard_desc).await;
                info!("linearizability shard moving result: {result:?}");
            }
        };
        tokio::join!(workload, nemesis);

        assert_linearizable(fn_name!(), 0x51A7E, recorder.history().await);
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_with_shard_moving_stress() {
    run_linearizability_test(async {
        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), true).await;
        c.assert_num_group_voters(2, 3).await;
        let key = b"linear-moving-stress-key-00".to_vec();
        let recorder = Recorder::new();
        let workload = run_kv_workload(
            db,
            table_id,
            recorder.clone(),
            24,
            40,
            numbered_keys("linear-moving-stress-key", 12),
            0x51A7E,
        );
        let nemesis = async {
            sekas_runtime::time::sleep(Duration::from_millis(10)).await;
            let source_state = c.find_router_group_state_by_key(table_id, &key).await.unwrap();
            if source_state.id != 2 {
                let shard_desc = c.get_shard_desc(table_id, &key).await.unwrap();
                let mut target = c.group(2);
                let result =
                    target.accept_shard(source_state.id, source_state.epoch, &shard_desc).await;
                info!("linearizability shard moving stress result: {result:?}");
            }
        };
        tokio::join!(workload, nemesis);

        assert_linearizable(fn_name!(), 0x51A7E, recorder.history().await);
        drop(c);
        drop(ctx);
    });
}

#[ignore]
#[test]
fn linearizable_kv_with_shard_moving_long_run() {
    run_linearizability_test(async {
        let workers = env_usize("SEKAS_LINEAR_WORKERS", 32);
        let ops_per_worker = env_usize("SEKAS_LINEAR_OPS_PER_WORKER", 100);
        let keys = env_usize("SEKAS_LINEAR_KEYS", 64);
        let nemesis_rounds = env_usize("SEKAS_LINEAR_NEMESIS_ROUNDS", 8);
        let nemesis_interval_ms = env_u64("SEKAS_LINEAR_NEMESIS_INTERVAL_MS", 25);
        let seed = env_u64("SEKAS_LINEAR_SEED", 0x51A7E);

        let (ctx, c, db, table_id, _nodes) =
            bootstrap_linearizability_cluster(fn_name!(), true).await;
        c.assert_num_group_voters(2, 3).await;
        let target_key = b"linear-moving-long-key-00".to_vec();
        let recorder = Recorder::new();
        let workload = run_kv_workload(
            db,
            table_id,
            recorder.clone(),
            workers,
            ops_per_worker,
            numbered_keys("linear-moving-long-key", keys),
            seed,
        );
        let nemesis = async {
            for _ in 0..nemesis_rounds {
                sekas_runtime::time::sleep(Duration::from_millis(nemesis_interval_ms)).await;
                let source_state =
                    c.find_router_group_state_by_key(table_id, &target_key).await.unwrap();
                if source_state.id == 2 {
                    continue;
                }
                let shard_desc = c.get_shard_desc(table_id, &target_key).await.unwrap();
                let mut target = c.group(2);
                let result =
                    target.accept_shard(source_state.id, source_state.epoch, &shard_desc).await;
                info!("linearizability shard moving long-run result: {result:?}");
            }
        };
        tokio::join!(workload, nemesis);

        assert_linearizable(fn_name!(), seed, recorder.history().await);
        drop(c);
        drop(ctx);
    });
}
