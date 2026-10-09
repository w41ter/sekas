// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

mod operation;
mod scheduler;
mod stats;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sekas_client::{AppError, Database, Router, SekasClient};
use stats::Stats;
pub(crate) use stats::WorkloadReport;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub(crate) struct Target {
    pub(crate) table: u64,
    pub(crate) prefix: String,
    pub(crate) keys: u64,
}
impl Target {
    pub(crate) fn key(&self, index: u64) -> Vec<u8> {
        format!("{}{:020}", self.prefix, index).into_bytes()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TxnMode {
    ReadOnly,
    Blind,
    ReadWrite,
}

#[derive(Clone)]
pub(crate) enum Operation {
    Read {
        present: bool,
        version: Option<u64>,
        expected: Option<Vec<u8>>,
    },
    Put,
    Churn,
    Insert,
    /// Finite pass over seeded keys, each owned by exactly one worker.
    Delete,
    Scan {
        limit: u64,
        version: Option<u64>,
        expected_rows: Option<u64>,
        expected: Option<Vec<u8>>,
    },
    Transaction {
        mode: TxnMode,
        keys: usize,
    },
    Metadata,
}

#[derive(Clone)]
pub(crate) struct Workload {
    pub(crate) name: String,
    pub(crate) operation: Operation,
    pub(crate) targets: Vec<Target>,
    pub(crate) concurrency: usize,
    pub(crate) value_size: usize,
    pub(crate) timeout: Duration,
    pub(crate) offered_qps: Option<f64>,
    pub(crate) hold: Duration,
    pub(crate) retries: usize,
    pub(crate) hot_probability: f64,
    pub(crate) hot_keys: u64,
    pub(crate) allow_conflicts: bool,
    pub(crate) allow_overload: bool,
    pub(crate) disjoint_keys: bool,
    pub(crate) router: Router,
}

pub(crate) struct WorkloadHandle {
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<Stats>>,
    tasks: Vec<JoinHandle<()>>,
}
impl WorkloadHandle {
    pub(crate) fn phase(&self, name: &str) {
        self.stats.lock().unwrap().phase(name);
    }
    pub(crate) fn finished(&self) -> bool {
        self.tasks.iter().all(|task| task.is_finished())
    }
    pub(crate) async fn stop(mut self) -> WorkloadReport {
        self.stop.store(true, Ordering::Release);
        // Requests have deadlines; drain them so slow requests remain in the tail.
        for task in self.tasks.drain(..) {
            task.await.expect("perf workload panicked");
        }
        self.stats.lock().unwrap().report()
    }
}
impl Drop for WorkloadHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for task in &self.tasks {
            task.abort();
        }
    }
}

pub(crate) fn spawn_workload(db: Database, client: SekasClient, spec: Workload) -> WorkloadHandle {
    let stop = Arc::new(AtomicBool::new(false));
    let mut initial = Stats::new(&spec.name, expected_errors(&spec));
    initial.parameters = parameters(&spec);
    let stats = Arc::new(Mutex::new(initial));
    let tasks = if let Some(qps) = spec.offered_qps {
        vec![scheduler::spawn_open_loop(db, client, spec, stop.clone(), stats.clone(), qps)]
    } else {
        scheduler::spawn_closed_loop(db, client, spec, stop.clone(), stats.clone())
    };
    WorkloadHandle { stop, stats, tasks }
}

#[derive(Default)]
struct Outcome {
    rows: u64,
    bytes: u64,
    attempts: u64,
    conflicts: u64,
}

fn parameters(spec: &Workload) -> BTreeMap<String, f64> {
    let mut params = BTreeMap::from([
        ("concurrency".to_owned(), spec.concurrency as f64),
        ("value_size".to_owned(), spec.value_size as f64),
        ("tables".to_owned(), spec.targets.len() as f64),
        ("live_key_space".to_owned(), spec.targets.iter().map(|t| t.keys as f64).sum()),
        ("hold_ms".to_owned(), spec.hold.as_secs_f64() * 1000.0),
        ("retry_limit".to_owned(), spec.retries as f64),
        ("hot_probability".to_owned(), spec.hot_probability),
        ("hot_keys".to_owned(), spec.hot_keys as f64),
    ]);
    if let Some(qps) = spec.offered_qps {
        params.insert("offered_qps".to_owned(), qps);
    }
    match spec.operation {
        Operation::Scan { limit, version, .. } => {
            params.insert("scan_limit".to_owned(), limit as f64);
            params.insert("historical".to_owned(), f64::from(version.is_some()));
        }
        Operation::Read { version, .. } => {
            params.insert("historical".to_owned(), f64::from(version.is_some()));
        }
        Operation::Transaction { keys, .. } => {
            params.insert("keys_per_txn".to_owned(), keys as f64);
        }
        _ => {}
    }
    params
}

fn expected_errors(spec: &Workload) -> Vec<String> {
    let mut errors = Vec::new();
    if spec.allow_conflicts {
        errors.push("txn_conflict".to_owned());
    }
    if spec.allow_overload {
        errors.extend(["overload".to_owned(), "deadline_exceeded".to_owned()]);
    }
    errors
}
fn classify_error(error: AppError) -> String {
    match error {
        AppError::TxnConflict => "txn_conflict",
        AppError::CasFailed(..) => "cas_failed",
        AppError::DeadlineExceeded(_) => "deadline_exceeded",
        AppError::Network(_) => "network",
        AppError::InvalidArgument(_) => "validation",
        AppError::Internal(_) => "internal",
        AppError::NotFound(_) => "not_found",
        AppError::AlreadyExists(_) => "already_exists",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_drains_an_in_flight_request_into_the_report() {
        let stats = Arc::new(Mutex::new(Stats::new("drain", vec![])));
        let worker_stats = stats.clone();
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            worker_stats.lock().unwrap().observe(
                "measurement",
                Duration::from_millis(20),
                Ok(()),
                Outcome::default(),
            );
        });
        let handle =
            WorkloadHandle { stop: Arc::new(AtomicBool::new(false)), stats, tasks: vec![task] };
        let report = handle.stop().await;
        assert_eq!(report.successes, 1);
        assert!(report.latency.max_us >= 20_000);
    }

    #[tokio::test]
    async fn dropping_a_workload_releases_in_flight_resources() {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (resource, released) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        let handle = WorkloadHandle {
            stop: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(Mutex::new(Stats::new("cancel", vec![]))),
            tasks: vec![task],
        };
        ready.await.unwrap();
        drop(handle);
        assert!(tokio::time::timeout(Duration::from_millis(100), released).await.is_ok());
    }
}
