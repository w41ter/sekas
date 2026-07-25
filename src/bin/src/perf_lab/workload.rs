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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::prelude::*;
use rand::rngs::SmallRng;
use sekas_client::{AppError, Database, Range, RangeRequest, WriteBuilder};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use super::report::HistogramSummary;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct WorkloadReport {
    pub(crate) name: String,
    pub(crate) operations: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
    pub(crate) duration_ms: u128,
    pub(crate) qps: f64,
    pub(crate) latency: HistogramSummary,
    pub(crate) errors: BTreeMap<String, u64>,
    pub(crate) phase_summaries: Vec<PhaseWorkloadSummary>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct PhaseWorkloadSummary {
    name: String,
    operations: u64,
    successes: u64,
    failures: u64,
    duration_ms: u128,
    qps: f64,
    latency: HistogramSummary,
    errors: BTreeMap<String, u64>,
}

pub(crate) struct WorkloadHandle {
    stats: Arc<Mutex<WorkloadStats>>,
    tasks: Vec<JoinHandle<()>>,
    started: Instant,
}

impl WorkloadHandle {
    pub(crate) async fn stop(self) -> WorkloadReport {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await;
        }
        let elapsed = self.started.elapsed();
        let stats = self.stats.lock().await;
        stats.report(elapsed)
    }

    pub(crate) async fn phase(&self, name: &str) {
        self.stats.lock().await.phase(name.to_owned());
    }
}

#[derive(Default)]
struct WorkloadStats {
    name: String,
    phase: String,
    operations: u64,
    successes: u64,
    failures: u64,
    latencies: Vec<u64>,
    errors: BTreeMap<String, u64>,
    phases: BTreeMap<String, PhaseStats>,
}

impl WorkloadStats {
    fn new(name: &str) -> Self {
        let mut stats =
            WorkloadStats { name: name.to_owned(), phase: "main".to_owned(), ..Default::default() };
        stats.phases.insert("main".to_owned(), PhaseStats::new());
        stats
    }

    fn phase(&mut self, phase: String) {
        if let Some(current) = self.phases.get_mut(&self.phase) {
            current.finish();
        }
        self.phases.entry(phase.clone()).or_insert_with(PhaseStats::new);
        self.phase = phase;
    }

    fn observe(&mut self, latency_us: u64, error: Option<String>) {
        self.operations += 1;
        let success = error.is_none();
        if success {
            self.successes += 1;
        } else {
            self.failures += 1;
        }
        if let Some(error) = error.as_ref() {
            *self.errors.entry(error.clone()).or_default() += 1;
        }
        self.latencies.push(latency_us);
        self.phases
            .entry(self.phase.clone())
            .or_insert_with(PhaseStats::new)
            .observe(latency_us, error.as_ref());
    }

    fn report(&self, elapsed: Duration) -> WorkloadReport {
        let seconds = elapsed.as_secs_f64().max(0.001);
        let phase_summaries = self.phases.iter().map(|(name, phase)| phase.report(name)).collect();
        WorkloadReport {
            name: self.name.clone(),
            operations: self.operations,
            successes: self.successes,
            failures: self.failures,
            duration_ms: elapsed.as_millis(),
            qps: self.operations as f64 / seconds,
            latency: HistogramSummary::from_latencies(&self.latencies),
            errors: self.errors.clone(),
            phase_summaries,
        }
    }
}

struct PhaseStats {
    started: Instant,
    finished: Option<Instant>,
    operations: u64,
    successes: u64,
    failures: u64,
    latencies: Vec<u64>,
    errors: BTreeMap<String, u64>,
}

impl PhaseStats {
    fn new() -> Self {
        PhaseStats {
            started: Instant::now(),
            finished: None,
            operations: 0,
            successes: 0,
            failures: 0,
            latencies: Vec::new(),
            errors: BTreeMap::new(),
        }
    }

    fn finish(&mut self) {
        self.finished.get_or_insert_with(Instant::now);
    }

    fn duration(&self) -> Duration {
        self.finished.unwrap_or_else(Instant::now).saturating_duration_since(self.started)
    }

    fn observe(&mut self, latency_us: u64, error: Option<&String>) {
        self.operations += 1;
        if let Some(error) = error {
            self.failures += 1;
            *self.errors.entry(error.clone()).or_default() += 1;
        } else {
            self.successes += 1;
        }
        self.latencies.push(latency_us);
    }

    fn report(&self, name: &str) -> PhaseWorkloadSummary {
        let duration = self.duration();
        let seconds = duration.as_secs_f64().max(0.001);
        PhaseWorkloadSummary {
            name: name.to_owned(),
            operations: self.operations,
            successes: self.successes,
            failures: self.failures,
            duration_ms: duration.as_millis(),
            qps: self.operations as f64 / seconds,
            latency: HistogramSummary::from_latencies(&self.latencies),
            errors: self.errors.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) enum WorkloadKind {
    FixedKeyPut { table: u64, key: Vec<u8> },
    RandomPut { table: u64, prefix: String },
    RandomGet { table: u64, prefix: String },
    MixedReadWrite { table: u64, prefix: String, write_ratio: f64 },
    PrefixScan { table: u64, prefix: Vec<u8>, limit: u64 },
    BatchTxnCommit { left_table: u64, right_table: u64 },
    TxnConflict { table: u64, guard_key: Vec<u8>, prefix: String },
    MultiKeyTxn { table: u64, prefix: String, keys_per_txn: usize },
}

pub(crate) fn spawn_workload(
    db: Database,
    name: &str,
    kind: WorkloadKind,
    concurrency: usize,
    value_size: usize,
    key_space: u64,
) -> WorkloadHandle {
    let stats = Arc::new(Mutex::new(WorkloadStats::new(name)));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(concurrency);
    for worker_id in 0..concurrency {
        let db = db.clone();
        let stats = stats.clone();
        let kind = kind.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = SmallRng::seed_from_u64(worker_id as u64 + 0x5ECA5);
            loop {
                let value = random_bytes(&mut rng, value_size);
                let start = Instant::now();
                let result = match &kind {
                    WorkloadKind::FixedKeyPut { table, key } => {
                        db.put(*table, key.clone(), value).await.map(|_| ())
                    }
                    WorkloadKind::RandomPut { table, prefix } => {
                        let key =
                            format!("{}{:020}", prefix, rng.gen_range(0..key_space)).into_bytes();
                        db.put(*table, key, value).await.map(|_| ())
                    }
                    WorkloadKind::RandomGet { table, prefix } => {
                        let key =
                            format!("{}{:020}", prefix, rng.gen_range(0..key_space)).into_bytes();
                        db.get(*table, key).await.map(|_| ())
                    }
                    WorkloadKind::MixedReadWrite { table, prefix, write_ratio } => {
                        let key =
                            format!("{}{:020}", prefix, rng.gen_range(0..key_space)).into_bytes();
                        if rng.gen_bool(write_ratio.clamp(0.0, 1.0)) {
                            db.put(*table, key, value).await.map(|_| ())
                        } else {
                            db.get(*table, key).await.map(|_| ())
                        }
                    }
                    WorkloadKind::PrefixScan { table, prefix, limit } => {
                        scan_prefix(&db, *table, prefix.clone(), *limit).await
                    }
                    WorkloadKind::BatchTxnCommit { left_table, right_table } => {
                        let suffix = rng.gen_range(0..key_space);
                        let mut txn = db.begin_txn();
                        txn.put(
                            *left_table,
                            WriteBuilder::new(format!("left-{suffix:020}").into_bytes())
                                .ensure_put(value.clone()),
                        );
                        txn.put(
                            *right_table,
                            WriteBuilder::new(format!("right-{suffix:020}").into_bytes())
                                .ensure_put(value),
                        );
                        txn.commit().await.map(|_| ())
                    }
                    WorkloadKind::TxnConflict { table, guard_key, prefix } => {
                        let suffix = rng.gen_range(0..key_space);
                        let mut txn = db.begin_txn();
                        txn.put(
                            *table,
                            WriteBuilder::new(guard_key.clone()).ensure_put(value.clone()),
                        );
                        txn.put(
                            *table,
                            WriteBuilder::new(format!("{}{:020}", prefix, suffix).into_bytes())
                                .ensure_put(value),
                        );
                        txn.commit().await.map(|_| ())
                    }
                    WorkloadKind::MultiKeyTxn { table, prefix, keys_per_txn } => {
                        let mut txn = db.begin_txn();
                        for _ in 0..*keys_per_txn {
                            let suffix = rng.gen_range(0..key_space);
                            txn.put(
                                *table,
                                WriteBuilder::new(format!("{}{:020}", prefix, suffix).into_bytes())
                                    .ensure_put(value.clone()),
                            );
                        }
                        txn.commit().await.map(|_| ())
                    }
                };
                let latency = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                stats.lock().await.observe(latency, result.err().map(classify_app_error));
            }
        }));
    }
    WorkloadHandle { stats, tasks, started }
}

fn classify_app_error(err: AppError) -> String {
    match err {
        AppError::NotFound(_) => "not_found",
        AppError::AlreadyExists(_) => "already_exists",
        AppError::InvalidArgument(_) => "invalid_argument",
        AppError::DeadlineExceeded(_) => "deadline_exceeded",
        AppError::CasFailed(..) => "cas_failed",
        AppError::TxnConflict => "txn_conflict",
        AppError::Network(status) => match status.code().description() {
            "unavailable" => "network_unavailable",
            "unknown" => "network_unknown",
            "deadline exceeded" => "network_deadline_exceeded",
            _ => "network_other",
        },
        AppError::Internal(_) => "internal",
    }
    .to_owned()
}

async fn scan_prefix(
    db: &Database,
    table: u64,
    prefix: Vec<u8>,
    limit: u64,
) -> sekas_client::AppResult<()> {
    let mut stream = db
        .range(RangeRequest {
            table_id: table,
            version: None,
            range: Range::Prefix(prefix),
            limit,
            limit_bytes: 0,
            buffered_requests: 1,
        })
        .await?;
    while let Some(batch) = stream.next().await {
        let _ = batch?;
    }
    Ok(())
}

fn random_bytes(rng: &mut SmallRng, size: usize) -> Vec<u8> {
    const BYTES: &[u8; 62] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        out.push(BYTES[rng.gen_range(0..BYTES.len())]);
    }
    out
}
