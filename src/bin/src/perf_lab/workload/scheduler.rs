// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use sekas_client::{Database, SekasClient};
use tokio::task::JoinHandle;

use super::operation::execute;
use super::stats::Stats;
use super::{Operation, Outcome, Workload, classify_error};

pub(super) fn spawn_open_loop(
    db: Database,
    client: SekasClient,
    spec: Workload,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<Stats>>,
    qps: f64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let permits = Arc::new(tokio::sync::Semaphore::new(spec.concurrency));
        let mut running = tokio::task::JoinSet::new();
        let origin = Instant::now();
        let mut sequence = 0_u64;
        let mut rng = SmallRng::seed_from_u64(0x5ECA5);
        while !stop.load(Ordering::Acquire) {
            let scheduled = origin + Duration::from_secs_f64(sequence as f64 / qps);
            while Instant::now() < scheduled && !stop.load(Ordering::Acquire) {
                tokio::time::sleep(
                    scheduled
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(100)),
                )
                .await;
            }
            if stop.load(Ordering::Acquire) {
                break;
            }
            let phase = stats.lock().unwrap().current.clone();
            match permits.clone().try_acquire_owned() {
                Ok(permit) => {
                    let db = db.clone();
                    let client = client.clone();
                    let spec = spec.clone();
                    let stats = stats.clone();
                    let seed = rng.r#gen();
                    running.spawn(async move {
                        let _permit = permit;
                        let mut rng = SmallRng::seed_from_u64(seed);
                        run_one(
                            &db, &client, &spec, &mut rng, 0, sequence, scheduled, &phase, &stats,
                        )
                        .await;
                    });
                }
                Err(_) => stats.lock().unwrap().observe(
                    &phase,
                    scheduled.elapsed(),
                    Err("overload".to_owned()),
                    Outcome::default(),
                ),
            }
            sequence += 1;
            while let Some(result) = running.try_join_next() {
                result.expect("perf request panicked");
            }
        }
        while let Some(result) = running.join_next().await {
            result.expect("perf request panicked");
        }
    })
}

pub(super) fn spawn_closed_loop(
    db: Database,
    client: SekasClient,
    spec: Workload,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<Stats>>,
) -> Vec<JoinHandle<()>> {
    let mut tasks = Vec::new();
    for worker in 0..spec.concurrency {
        let db = db.clone();
        let client = client.clone();
        let spec = spec.clone();
        let stop = stop.clone();
        let stats = stats.clone();
        tasks.push(tokio::spawn(async move {
            let mut rng = SmallRng::seed_from_u64(worker as u64 + 0x5ECA5);
            let mut sequence = 0;
            while !stop.load(Ordering::Acquire) {
                if matches!(spec.operation, Operation::Delete)
                    && worker as u64 + sequence * spec.concurrency as u64 >= spec.targets[0].keys
                {
                    break;
                }
                let phase = stats.lock().unwrap().current.clone();
                run_one(
                    &db,
                    &client,
                    &spec,
                    &mut rng,
                    worker,
                    sequence,
                    Instant::now(),
                    &phase,
                    &stats,
                )
                .await;
                sequence += 1;
            }
        }));
    }
    tasks
}

#[allow(clippy::too_many_arguments)]
async fn run_one(
    db: &Database,
    client: &SekasClient,
    spec: &Workload,
    rng: &mut SmallRng,
    worker: usize,
    sequence: u64,
    started: Instant,
    phase: &str,
    stats: &Mutex<Stats>,
) {
    let mut outcome = Outcome::default();
    // Fixed payload is generated once per request only for writes. Reads allocate
    // no unused values.
    let result = tokio::time::timeout_at(
        (started + spec.timeout).into(),
        execute(db, client, spec, rng, worker, sequence, &mut outcome),
    )
    .await;
    let result = match result {
        Ok(result) => result.map_err(classify_error),
        Err(_) => Err("deadline_exceeded".to_owned()),
    };
    stats.lock().unwrap().observe(phase, started.elapsed(), result, outcome);
}
