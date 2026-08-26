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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::generator::{DeterministicGenerator, WorkloadConfig};
use crate::model::WriterModel;
use crate::progress::{ProgressSnapshot, WriterProgress};
use crate::workload::{
    ExecuteResult, Observation, OperationExecutor, OperationGenerator, OperationId, OperationPlan,
    ReconcileResult, WriterId,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkloadEventKind {
    OperationStarted(OperationPlan),
    ExecuteFinished { id: OperationId, result: ExecuteResult },
    Reconciled { id: OperationId, result: ReconcileResult },
    ReadVerified { reader: u32, writer: WriterId, completed: u64, keys: usize },
    ReadDiscarded { reader: u32, writer: WriterId },
    ObservationFailed { actor: String, error: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkloadEvent {
    pub sequence: u64,
    pub elapsed_micros: u64,
    pub kind: WorkloadEventKind,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub enum WorkloadFailure {
    #[error("invalid workload configuration: {0}")]
    Configuration(String),
    #[error("writer {writer:?} operation {sequence} failed fatally: {message}")]
    FatalOperation { writer: WriterId, sequence: u64, message: String },
    #[error("writer {writer:?} operation {sequence} violated its state contract: {message}")]
    Violation {
        writer: WriterId,
        sequence: u64,
        message: String,
        expected: Observation,
        observed: Observation,
    },
    #[error("{actor} could not reconcile state within its recovery budget: {message}")]
    Availability { actor: String, message: String },
    #[error("writer {writer:?} model error: {message}")]
    Model { writer: WriterId, message: String },
    #[error("writer {writer:?} stopped with an unstable operation at {completed}")]
    Inconclusive { writer: WriterId, completed: u64 },
    #[error("workload task failed: {0}")]
    Task(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WriterSnapshot {
    pub writer: WriterId,
    pub completed: u64,
    pub stable: bool,
    pub expected: Observation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkloadStats {
    pub operations: u64,
    pub retries: u64,
    pub reconciliations: u64,
    pub reader_checks: u64,
    pub reader_discards: u64,
    pub observation_errors: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkloadReport {
    pub config: WorkloadConfig,
    pub failures: Vec<WorkloadFailure>,
    pub writers: Vec<WriterSnapshot>,
    pub events: Vec<WorkloadEvent>,
    pub stats: WorkloadStats,
    pub dropped_events: u64,
    pub final_verification_complete: bool,
}

impl WorkloadReport {
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty() && self.final_verification_complete
    }
}

#[derive(Default)]
struct AtomicStats {
    operations: AtomicU64,
    retries: AtomicU64,
    reconciliations: AtomicU64,
    reader_checks: AtomicU64,
    reader_discards: AtomicU64,
    observation_errors: AtomicU64,
}

impl AtomicStats {
    fn snapshot(&self) -> WorkloadStats {
        WorkloadStats {
            operations: self.operations.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            reconciliations: self.reconciliations.load(Ordering::Relaxed),
            reader_checks: self.reader_checks.load(Ordering::Relaxed),
            reader_discards: self.reader_discards.load(Ordering::Relaxed),
            observation_errors: self.observation_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct StopSignal {
    requested: AtomicBool,
}

struct RetryBudget {
    deadline: Instant,
    backoff: Duration,
    request_timeout: Duration,
    maximum_backoff: Duration,
}

impl RetryBudget {
    fn new(config: &WorkloadConfig) -> Self {
        Self {
            deadline: Instant::now() + config.reconciliation_timeout,
            backoff: config.retry_initial_backoff,
            request_timeout: config.request_timeout,
            maximum_backoff: config.retry_max_backoff,
        }
    }

    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    async fn sleep(&mut self) {
        let remaining = self.remaining();
        if !remaining.is_zero() {
            tokio::time::sleep(self.backoff.min(remaining)).await;
            self.backoff = self.backoff.saturating_mul(2).min(self.maximum_backoff);
        }
    }
}

impl StopSignal {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

struct EventLog {
    start: Instant,
    next_sequence: AtomicU64,
    capacity: usize,
    dropped: AtomicU64,
    events: Mutex<VecDeque<WorkloadEvent>>,
}

impl EventLog {
    fn new(capacity: usize) -> Self {
        Self {
            start: Instant::now(),
            next_sequence: AtomicU64::new(0),
            capacity,
            dropped: AtomicU64::new(0),
            events: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    fn push(&self, kind: WorkloadEventKind) {
        let event = WorkloadEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
            elapsed_micros: self.start.elapsed().as_micros() as u64,
            kind,
        };
        let mut events = self.events.lock().unwrap();
        if events.len() == self.capacity {
            events.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        events.push_back(event);
    }

    fn take(&self) -> Vec<WorkloadEvent> {
        let mut events =
            std::mem::take(&mut *self.events.lock().unwrap()).into_iter().collect::<Vec<_>>();
        events.sort_by_key(|event| event.sequence);
        events
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

struct WriterShared {
    id: WriterId,
    progress: WriterProgress,
    model: RwLock<WriterModel>,
    recent: Mutex<VecDeque<OperationPlan>>,
}

impl WriterShared {
    fn snapshot(&self) -> WriterSnapshot {
        let progress = self.progress.load();
        let model = self.model.read().unwrap();
        WriterSnapshot {
            writer: self.id,
            completed: progress.completed,
            stable: progress.stable && model.completed() == progress.completed,
            expected: model.all(),
        }
    }
}

pub struct WorkloadRunner {
    config: Arc<WorkloadConfig>,
    executor: Arc<dyn OperationExecutor>,
    generator: Arc<dyn OperationGenerator>,
}

impl WorkloadRunner {
    pub fn new(
        config: WorkloadConfig,
        executor: Arc<dyn OperationExecutor>,
    ) -> Result<Self, WorkloadFailure> {
        config.validate().map_err(WorkloadFailure::Configuration)?;
        if config.operation_weights.add_i64 > 0 && !executor.capabilities().resolvable_add_i64 {
            return Err(WorkloadFailure::Configuration(
                "AddI64 is enabled but the executor cannot resolve ambiguous add results"
                    .to_string(),
            ));
        }
        let generator = Arc::new(DeterministicGenerator::new(config.clone()));
        Ok(Self { config: Arc::new(config), executor, generator })
    }

    pub fn with_generator(
        config: WorkloadConfig,
        executor: Arc<dyn OperationExecutor>,
        generator: Arc<dyn OperationGenerator>,
    ) -> Result<Self, WorkloadFailure> {
        config.validate().map_err(WorkloadFailure::Configuration)?;
        if config.operation_weights.add_i64 > 0 && !executor.capabilities().resolvable_add_i64 {
            return Err(WorkloadFailure::Configuration(
                "AddI64 is enabled but the executor cannot resolve ambiguous add results"
                    .to_string(),
            ));
        }
        Ok(Self { config: Arc::new(config), executor, generator })
    }

    pub fn start(self) -> WorkloadHandle {
        let stop = Arc::new(StopSignal::default());
        let events = Arc::new(EventLog::new(self.config.max_recorded_events));
        let stats = Arc::new(AtomicStats::default());
        let writers = (0..self.config.writers)
            .map(|writer| {
                Arc::new(WriterShared {
                    id: WriterId(writer as u32),
                    progress: WriterProgress::new(),
                    model: RwLock::new(WriterModel::new(
                        &self.config.run_id,
                        WriterId(writer as u32),
                        &self.config.tables,
                        self.config.slots_per_writer,
                    )),
                    recent: Mutex::new(VecDeque::new()),
                })
            })
            .collect::<Vec<_>>();

        let writer_tasks = writers
            .iter()
            .map(|writer| {
                tokio::spawn(run_writer(
                    writer.clone(),
                    self.config.clone(),
                    self.generator.clone(),
                    self.executor.clone(),
                    stop.clone(),
                    events.clone(),
                    stats.clone(),
                ))
            })
            .collect();
        let reader_tasks = (0..self.config.readers)
            .map(|reader| {
                tokio::spawn(run_reader(
                    reader as u32,
                    writers.clone(),
                    self.config.clone(),
                    self.executor.clone(),
                    stop.clone(),
                    events.clone(),
                    stats.clone(),
                ))
            })
            .collect();

        WorkloadHandle {
            config: self.config,
            executor: self.executor,
            stop,
            events,
            stats,
            writers,
            writer_tasks,
            reader_tasks,
        }
    }
}

pub struct WorkloadHandle {
    config: Arc<WorkloadConfig>,
    executor: Arc<dyn OperationExecutor>,
    stop: Arc<StopSignal>,
    events: Arc<EventLog>,
    stats: Arc<AtomicStats>,
    writers: Vec<Arc<WriterShared>>,
    writer_tasks: Vec<JoinHandle<Option<WorkloadFailure>>>,
    reader_tasks: Vec<JoinHandle<Option<WorkloadFailure>>>,
}

impl WorkloadHandle {
    /// Requests that writers stop at their next stable boundary. This does not
    /// wait or perform final verification, allowing the cluster to be restored
    /// first by the chaos orchestrator.
    pub fn request_stop(&self) {
        self.stop.request();
    }

    /// Requests a stable-boundary stop and then performs final verification.
    pub async fn stop(self) -> WorkloadReport {
        self.request_stop();
        self.finish().await
    }

    /// Waits for finite writers, stops readers, and performs final
    /// verification. This should only be used when `operations_per_writer`
    /// is configured.
    pub async fn wait(self) -> WorkloadReport {
        self.finish().await
    }

    pub async fn finish(self) -> WorkloadReport {
        let WorkloadHandle {
            config,
            executor,
            stop,
            events,
            stats,
            writers,
            writer_tasks,
            reader_tasks,
        } = self;
        let mut failures = Vec::new();
        for task in writer_tasks {
            match task.await {
                Ok(Some(failure)) => failures.push(failure),
                Ok(None) => {}
                Err(err) => failures.push(WorkloadFailure::Task(err.to_string())),
            }
        }
        stop.request();
        for task in reader_tasks {
            match task.await {
                Ok(Some(failure)) => failures.push(failure),
                Ok(None) => {}
                Err(err) => failures.push(WorkloadFailure::Task(err.to_string())),
            }
        }

        let final_verification_complete =
            final_verify(&writers, &config, &executor, &events, &stats, &mut failures).await;
        let writer_snapshots = writers.iter().map(|writer| writer.snapshot()).collect();
        WorkloadReport {
            config: (*config).clone(),
            failures,
            writers: writer_snapshots,
            dropped_events: events.dropped(),
            events: events.take(),
            stats: stats.snapshot(),
            final_verification_complete,
        }
    }
}

async fn run_writer(
    writer: Arc<WriterShared>,
    config: Arc<WorkloadConfig>,
    generator: Arc<dyn OperationGenerator>,
    executor: Arc<dyn OperationExecutor>,
    stop: Arc<StopSignal>,
    events: Arc<EventLog>,
    stats: Arc<AtomicStats>,
) -> Option<WorkloadFailure> {
    loop {
        if stop.requested() {
            return None;
        }
        let completed = writer.progress.load().completed;
        if config.operations_per_writer.is_some_and(|limit| completed >= limit) {
            return None;
        }
        let id = OperationId { writer: writer.id, sequence: completed };
        let plan = {
            let model = writer.model.read().unwrap();
            generator.generate(config.seed, id, &model)
        };
        writer.progress.begin();
        events.push(WorkloadEventKind::OperationStarted(plan.clone()));

        if let Err(failure) =
            execute_until_stable(&writer, &plan, &config, &executor, &events, &stats).await
        {
            stop.request();
            return Some(failure);
        }

        {
            let mut model = writer.model.write().unwrap();
            if let Err(message) = model.apply(&plan) {
                stop.request();
                return Some(WorkloadFailure::Model { writer: writer.id, message });
            }
        }
        {
            let mut recent = writer.recent.lock().unwrap();
            if plan.operation.is_compound() {
                recent.push_back(plan.clone());
                while recent.len() > config.recent_operations {
                    recent.pop_front();
                }
            }
        }
        writer.progress.finish();
        stats.operations.fetch_add(1, Ordering::Relaxed);
    }
}

async fn execute_until_stable(
    writer: &WriterShared,
    plan: &OperationPlan,
    config: &WorkloadConfig,
    executor: &Arc<dyn OperationExecutor>,
    events: &EventLog,
    stats: &AtomicStats,
) -> Result<(), WorkloadFailure> {
    let mut budget = RetryBudget::new(config);
    loop {
        let remaining = budget.remaining();
        if remaining.is_zero() {
            return Err(WorkloadFailure::Availability {
                actor: format!("writer {:?}", writer.id),
                message: format!("operation {} did not converge", plan.id.sequence),
            });
        }
        let result = match tokio::time::timeout(
            budget.request_timeout.min(remaining),
            executor.execute(plan),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => ExecuteResult::Unknown("operation attempt timed out".to_string()),
        };
        events.push(WorkloadEventKind::ExecuteFinished { id: plan.id, result: result.clone() });
        match result {
            ExecuteResult::Succeeded => {
                if plan.operation.is_compound() {
                    let observed = observe_with_retry(
                        format!("writer {:?}", writer.id),
                        &plan.verification(),
                        executor,
                        &mut budget,
                        events,
                        stats,
                    )
                    .await?;
                    let reconciled = plan.reconcile(&observed);
                    events.push(WorkloadEventKind::Reconciled {
                        id: plan.id,
                        result: reconciled.clone(),
                    });
                    match reconciled {
                        ReconcileResult::Applied => {}
                        ReconcileResult::NotApplied => {
                            return Err(violation(
                                plan,
                                observed,
                                "acknowledged compound operation is not visible".to_string(),
                            ));
                        }
                        ReconcileResult::Violation(message) => {
                            return Err(violation(plan, observed, message));
                        }
                    }
                }
                return Ok(());
            }
            ExecuteResult::Fatal(message) => {
                writer.progress.abort();
                return Err(WorkloadFailure::FatalOperation {
                    writer: writer.id,
                    sequence: plan.id.sequence,
                    message,
                });
            }
            ExecuteResult::Unknown(_) => {
                let observed = observe_with_retry(
                    format!("writer {:?}", writer.id),
                    &plan.verification(),
                    executor,
                    &mut budget,
                    events,
                    stats,
                )
                .await?;
                stats.reconciliations.fetch_add(1, Ordering::Relaxed);
                let reconciled = plan.reconcile(&observed);
                events.push(WorkloadEventKind::Reconciled {
                    id: plan.id,
                    result: reconciled.clone(),
                });
                match reconciled {
                    ReconcileResult::Applied => return Ok(()),
                    ReconcileResult::NotApplied => {
                        stats.retries.fetch_add(1, Ordering::Relaxed);
                        budget.sleep().await;
                    }
                    ReconcileResult::Violation(message) => {
                        return Err(violation(plan, observed, message));
                    }
                }
            }
        }
    }
}

fn violation(plan: &OperationPlan, observed: Observation, message: String) -> WorkloadFailure {
    WorkloadFailure::Violation {
        writer: plan.id.writer,
        sequence: plan.id.sequence,
        message,
        expected: plan.after.clone(),
        observed,
    }
}

async fn observe_with_retry(
    actor: String,
    target: &crate::workload::VerificationTarget,
    executor: &Arc<dyn OperationExecutor>,
    budget: &mut RetryBudget,
    events: &EventLog,
    stats: &AtomicStats,
) -> Result<Observation, WorkloadFailure> {
    loop {
        let remaining = budget.remaining();
        if remaining.is_zero() {
            return Err(WorkloadFailure::Availability {
                actor,
                message: "observation timed out".to_string(),
            });
        }
        match tokio::time::timeout(budget.request_timeout.min(remaining), executor.observe(target))
            .await
        {
            Ok(Ok(observed)) => return Ok(observed),
            Err(_) => {
                events.push(WorkloadEventKind::ObservationFailed {
                    actor: actor.clone(),
                    error: "observation attempt timed out".to_string(),
                });
                stats.observation_errors.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(err)) => {
                let message = err.to_string();
                events.push(WorkloadEventKind::ObservationFailed {
                    actor: actor.clone(),
                    error: message.clone(),
                });
                stats.observation_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        let remaining = budget.remaining();
        if remaining.is_zero() {
            return Err(WorkloadFailure::Availability {
                actor,
                message: "observation did not recover".to_string(),
            });
        }
        budget.sleep().await;
    }
}

async fn run_reader(
    reader: u32,
    writers: Vec<Arc<WriterShared>>,
    config: Arc<WorkloadConfig>,
    executor: Arc<dyn OperationExecutor>,
    stop: Arc<StopSignal>,
    events: Arc<EventLog>,
    stats: Arc<AtomicStats>,
) -> Option<WorkloadFailure> {
    let mut rng = SmallRng::seed_from_u64(reader_seed(config.seed, reader));
    while !stop.requested() {
        let writer = &writers[rng.gen_range(0..writers.len())];
        let before = writer.progress.load();
        if !before.stable {
            tokio::task::yield_now().await;
            continue;
        }
        let expected = select_reader_target(writer, &config, &mut rng, before);
        let Some(expected) = expected else {
            stats.reader_discards.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let target = expected.target(true);
        let observed =
            match tokio::time::timeout(config.request_timeout, executor.observe(&target)).await {
                Ok(Ok(observed)) => observed,
                Ok(Err(err)) => {
                    stats.observation_errors.fetch_add(1, Ordering::Relaxed);
                    events.push(WorkloadEventKind::ObservationFailed {
                        actor: format!("reader {reader}"),
                        error: err.to_string(),
                    });
                    tokio::time::sleep(config.reader_pause).await;
                    continue;
                }
                Err(_) => {
                    stats.observation_errors.fetch_add(1, Ordering::Relaxed);
                    events.push(WorkloadEventKind::ObservationFailed {
                        actor: format!("reader {reader}"),
                        error: "observation attempt timed out".to_string(),
                    });
                    continue;
                }
            };
        if !writer.progress.unchanged_since(before) {
            stats.reader_discards.fetch_add(1, Ordering::Relaxed);
            events.push(WorkloadEventKind::ReadDiscarded { reader, writer: writer.id });
            continue;
        }
        if observed != expected {
            stop.request();
            return Some(WorkloadFailure::Violation {
                writer: writer.id,
                sequence: before.completed,
                message: format!("reader {reader} observed an invalid stable state"),
                expected,
                observed,
            });
        }
        stats.reader_checks.fetch_add(1, Ordering::Relaxed);
        events.push(WorkloadEventKind::ReadVerified {
            reader,
            writer: writer.id,
            completed: before.completed,
            keys: target.keys.len(),
        });
        tokio::time::sleep(config.reader_pause).await;
    }
    None
}

fn select_reader_target(
    writer: &WriterShared,
    config: &WorkloadConfig,
    rng: &mut SmallRng,
    progress: ProgressSnapshot,
) -> Option<Observation> {
    let model = writer.model.read().unwrap();
    if model.completed() != progress.completed {
        return None;
    }
    let recent_keys = if rng.gen_ratio(1, 5) {
        let recent = writer.recent.lock().unwrap();
        if recent.is_empty() {
            None
        } else {
            Some(
                recent[rng.gen_range(0..recent.len())]
                    .after
                    .values
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        }
    } else {
        None
    };
    if let Some(keys) = recent_keys {
        Some(model.observe_keys(keys))
    } else {
        let width = if config.verify_width.min == config.verify_width.max {
            config.verify_width.min
        } else {
            rng.gen_range(config.verify_width.min..=config.verify_width.max)
        };
        let start = rng.gen_range(0..=model.slot_count() - width);
        Some(model.range(start, width))
    }
}

async fn final_verify(
    writers: &[Arc<WriterShared>],
    config: &WorkloadConfig,
    executor: &Arc<dyn OperationExecutor>,
    events: &EventLog,
    stats: &AtomicStats,
    failures: &mut Vec<WorkloadFailure>,
) -> bool {
    let mut complete = true;
    for writer in writers {
        let progress = writer.progress.load();
        let snapshot = writer.snapshot();
        if !snapshot.stable {
            complete = false;
            failures.push(WorkloadFailure::Inconclusive {
                writer: writer.id,
                completed: progress.completed,
            });
            continue;
        }
        let keys = snapshot.expected.values.keys().cloned().collect::<Vec<_>>();
        for chunk in keys.chunks(config.verify_width.max) {
            let expected = Observation {
                values: chunk
                    .iter()
                    .map(|key| (key.clone(), snapshot.expected.values[key].clone()))
                    .collect(),
            };
            let mut budget = RetryBudget::new(config);
            let observed = match observe_with_retry(
                format!("final verifier for writer {:?}", writer.id),
                &expected.target(true),
                executor,
                &mut budget,
                events,
                stats,
            )
            .await
            {
                Ok(observed) => observed,
                Err(failure) => {
                    complete = false;
                    failures.push(failure);
                    break;
                }
            };
            if observed != expected {
                complete = false;
                failures.push(WorkloadFailure::Violation {
                    writer: writer.id,
                    sequence: progress.completed,
                    message: "final verification observed an invalid state".to_string(),
                    expected,
                    observed,
                });
                break;
            }
        }
    }
    complete
}

fn reader_seed(seed: u64, reader: u32) -> u64 {
    seed ^ 0x7265_6164_6572_0000u64 ^ (reader as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{WorkloadConfig, WorkloadFailure, WorkloadRunner};
    use crate::cluster::BoxFuture;
    use crate::generator::{OperationWeights, SizeRange};
    use crate::model::{WriterModel, encode_value};
    use crate::workload::{
        ExecuteResult, KeyValue, Mutation, Observation, Operation, OperationExecutor,
        OperationGenerator, OperationId, OperationPlan, OperationResult, VerificationTarget,
    };

    #[derive(Default)]
    struct MemoryExecutor {
        values: Mutex<BTreeMap<crate::LogicalKey, Option<Vec<u8>>>>,
        return_unknown_once: AtomicBool,
        apply_unknown: bool,
        partial_unknown: bool,
    }

    impl MemoryExecutor {
        fn unknown_once(partial_unknown: bool) -> Self {
            Self {
                values: Mutex::new(BTreeMap::new()),
                return_unknown_once: AtomicBool::new(true),
                apply_unknown: true,
                partial_unknown,
            }
        }

        fn not_applied_once() -> Self {
            Self {
                values: Mutex::new(BTreeMap::new()),
                return_unknown_once: AtomicBool::new(true),
                apply_unknown: false,
                partial_unknown: false,
            }
        }
    }

    impl OperationExecutor for MemoryExecutor {
        fn execute<'a>(&'a self, plan: &'a OperationPlan) -> BoxFuture<'a, ExecuteResult> {
            Box::pin(async move {
                let unknown = self.return_unknown_once.swap(false, Ordering::AcqRel);
                let mut values = self.values.lock().unwrap();
                if unknown && self.partial_unknown {
                    if let Some((key, value)) = plan.after.values.iter().next() {
                        values.insert(key.clone(), value.clone());
                    }
                } else if !unknown || self.apply_unknown {
                    values.extend(plan.after.values.clone());
                }
                if unknown {
                    ExecuteResult::Unknown("injected ambiguous result".to_string())
                } else {
                    ExecuteResult::Succeeded
                }
            })
        }

        fn observe<'a>(
            &'a self,
            target: &'a VerificationTarget,
        ) -> BoxFuture<'a, OperationResult<Observation>> {
            Box::pin(async move {
                let values = self.values.lock().unwrap();
                Ok(Observation {
                    values: target
                        .keys
                        .iter()
                        .cloned()
                        .map(|key| {
                            let value = values.get(&key).cloned().unwrap_or_default();
                            (key, value)
                        })
                        .collect(),
                })
            })
        }
    }

    struct TwoKeyTransaction;

    impl OperationGenerator for TwoKeyTransaction {
        fn generate(&self, _seed: u64, id: OperationId, model: &WriterModel) -> OperationPlan {
            let first = model.slot(0).clone();
            let second = model.slot(1).clone();
            let first_value = encode_value(id.writer, id.sequence, 0, b"first");
            let second_value = encode_value(id.writer, id.sequence, 1, b"second");
            let mutations = vec![
                Mutation::Put(KeyValue { key: first.clone(), value: first_value.clone() }),
                Mutation::Put(KeyValue { key: second.clone(), value: second_value.clone() }),
            ];
            OperationPlan {
                id,
                operation: Operation::Transaction(mutations),
                before: model.observe_keys([first.clone(), second.clone()]),
                after: Observation {
                    values: BTreeMap::from([
                        (first, Some(first_value)),
                        (second, Some(second_value)),
                    ]),
                },
            }
        }
    }

    fn config() -> WorkloadConfig {
        WorkloadConfig {
            run_id: "runner-test".to_string(),
            tables: vec![1, 2],
            writers: 1,
            readers: 1,
            slots_per_writer: 16,
            operations_per_writer: Some(20),
            value_size: SizeRange::new(8, 16),
            verify_width: SizeRange::new(1, 4),
            batch_width: SizeRange::new(2, 4),
            transaction_width: SizeRange::new(2, 4),
            reader_pause: Duration::ZERO,
            operation_weights: OperationWeights {
                put: 1,
                delete: 1,
                batch: 1,
                transaction: 1,
                add_i64: 0,
            },
            ..WorkloadConfig::default()
        }
    }

    #[tokio::test]
    async fn ambiguous_applied_operation_is_reconciled() {
        let executor = Arc::new(MemoryExecutor::unknown_once(false));
        let report = WorkloadRunner::new(config(), executor).unwrap().start().wait().await;
        assert!(report.is_valid(), "{:?}", report.failures);
        assert_eq!(report.stats.operations, 20);
        assert!(report.stats.reconciliations >= 1);
    }

    #[tokio::test]
    async fn ambiguous_not_applied_operation_is_retried() {
        let executor = Arc::new(MemoryExecutor::not_applied_once());
        let mut config = config();
        config.operation_weights =
            OperationWeights { put: 1, delete: 0, batch: 0, transaction: 0, add_i64: 0 };
        let report = WorkloadRunner::new(config, executor).unwrap().start().wait().await;
        assert!(report.is_valid(), "{:?}", report.failures);
        assert_eq!(report.stats.operations, 20);
        assert_eq!(report.stats.retries, 1);
    }

    #[tokio::test]
    async fn partial_transaction_is_reported_before_repair() {
        let mut config = config();
        config.readers = 0;
        config.operations_per_writer = Some(1);
        let executor = Arc::new(MemoryExecutor::unknown_once(true));
        let runner =
            WorkloadRunner::with_generator(config, executor, Arc::new(TwoKeyTransaction)).unwrap();
        let report = runner.start().wait().await;
        assert!(matches!(report.failures.first(), Some(WorkloadFailure::Violation { .. })));
        assert_eq!(report.stats.operations, 0);
    }
}
