// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::Outcome;
use crate::perf_lab::report::HistogramSummary;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct WorkloadReport {
    pub(crate) name: String,
    pub(crate) operations: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
    pub(crate) duration_ms: u128,
    /// Successful operations per second. Attempts and overloads are separate.
    pub(crate) qps: f64,
    pub(crate) attempt_qps: f64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) attempts: u64,
    pub(crate) conflicts: u64,
    pub(crate) max_success_gap_us: u64,
    pub(crate) latency: HistogramSummary,
    pub(crate) success_latency: HistogramSummary,
    pub(crate) errors: BTreeMap<String, u64>,
    pub(crate) phase_summaries: Vec<PhaseWorkloadSummary>,
    pub(crate) expected_errors: Vec<String>,
    pub(crate) parameters: BTreeMap<String, f64>,
}

impl WorkloadReport {
    pub(crate) fn unexpected_failures(&self) -> u64 {
        let expected: u64 = self
            .expected_errors
            .iter()
            .map(|kind| self.errors.get(kind).copied().unwrap_or(0))
            .sum();
        self.failures.saturating_sub(expected)
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct PhaseWorkloadSummary {
    pub(crate) name: String,
    pub(crate) operations: u64,
    pub(crate) successes: u64,
    pub(crate) failures: u64,
    pub(crate) duration_ms: u128,
    pub(crate) qps: f64,
    pub(crate) latency: HistogramSummary,
    pub(crate) errors: BTreeMap<String, u64>,
}

/// Bounded histogram: exact below 128us; upper bucket bounds within 1/64 above
/// it.
#[derive(Default)]
struct Latencies {
    buckets: BTreeMap<u64, u64>,
    count: u64,
    sum: u128,
    max: u64,
}
impl Latencies {
    fn observe(&mut self, us: u64) {
        let shift = (64 - us.leading_zeros()).saturating_sub(7);
        let width = 1_u64 << shift;
        let bound = us.saturating_add(width - 1) / width * width;
        *self.buckets.entry(bound).or_default() += 1;
        self.count += 1;
        self.sum += u128::from(us);
        self.max = self.max.max(us);
    }
    fn summary(&self) -> HistogramSummary {
        if self.count == 0 {
            return HistogramSummary::default();
        }
        let percentile = |p: f64| {
            let rank = (self.count as f64 * p).ceil() as u64;
            let mut n = 0;
            for (bound, count) in &self.buckets {
                n += count;
                if n >= rank {
                    return (*bound).min(self.max);
                }
            }
            self.max
        };
        HistogramSummary {
            count: self.count,
            avg_us: (self.sum / u128::from(self.count)) as u64,
            p50_us: percentile(0.5),
            p95_us: percentile(0.95),
            p99_us: percentile(0.99),
            p999_us: percentile(0.999),
            max_us: self.max,
        }
    }
}
struct Phase {
    started: Instant,
    finished: Option<Instant>,
    operations: u64,
    successes: u64,
    latencies: Latencies,
    errors: BTreeMap<String, u64>,
}
impl Phase {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            finished: None,
            operations: 0,
            successes: 0,
            latencies: Latencies::default(),
            errors: BTreeMap::new(),
        }
    }
    fn report(&self, name: &str) -> PhaseWorkloadSummary {
        let elapsed = self.finished.unwrap_or_else(Instant::now).duration_since(self.started);
        PhaseWorkloadSummary {
            name: name.to_owned(),
            operations: self.operations,
            successes: self.successes,
            failures: self.operations - self.successes,
            duration_ms: elapsed.as_millis(),
            qps: self.successes as f64 / elapsed.as_secs_f64().max(0.001),
            latency: self.latencies.summary(),
            errors: self.errors.clone(),
        }
    }
}
pub(super) struct Stats {
    pub(super) parameters: BTreeMap<String, f64>,
    expected_errors: Vec<String>,
    name: String,
    started: Instant,
    pub(super) current: String,
    phases: BTreeMap<String, Phase>,
    latencies: Latencies,
    success_latency: Latencies,
    rows: u64,
    bytes: u64,
    attempts: u64,
    conflicts: u64,
    last_success: Instant,
    max_gap: Duration,
}
impl Stats {
    pub(super) fn new(name: &str, expected_errors: Vec<String>) -> Self {
        let now = Instant::now();
        Self {
            parameters: BTreeMap::new(),
            expected_errors,
            name: name.to_owned(),
            started: now,
            current: "measurement".to_owned(),
            phases: BTreeMap::from([("measurement".to_owned(), Phase::new())]),
            latencies: Latencies::default(),
            success_latency: Latencies::default(),
            rows: 0,
            bytes: 0,
            attempts: 0,
            conflicts: 0,
            last_success: now,
            max_gap: Duration::ZERO,
        }
    }
    pub(super) fn phase(&mut self, name: &str) {
        if name == self.current {
            return;
        }
        self.phases.get_mut(&self.current).unwrap().finished = Some(Instant::now());
        assert!(!self.phases.contains_key(name), "phase names must be unique");
        self.phases.insert(name.to_owned(), Phase::new());
        self.current = name.to_owned();
    }
    pub(super) fn observe(
        &mut self,
        phase: &str,
        elapsed: Duration,
        result: Result<(), String>,
        outcome: Outcome,
    ) {
        let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.latencies.observe(us);
        self.attempts += outcome.attempts;
        self.conflicts += outcome.conflicts;
        let phase = self.phases.get_mut(phase).unwrap();
        phase.operations += 1;
        phase.latencies.observe(us);
        match result {
            Ok(()) => {
                phase.successes += 1;
                self.rows += outcome.rows;
                self.bytes += outcome.bytes;
                self.success_latency.observe(us);
                let now = Instant::now();
                self.max_gap = self.max_gap.max(now.duration_since(self.last_success));
                self.last_success = now;
            }
            Err(error) => {
                *phase.errors.entry(error).or_default() += 1;
            }
        }
    }
    pub(super) fn report(&self) -> WorkloadReport {
        let elapsed = self.started.elapsed();
        let phases: Vec<_> = self
            .phases
            .iter()
            .filter(|(_, p)| p.operations > 0)
            .map(|(name, p)| p.report(name))
            .collect();
        let successes = phases.iter().map(|p| p.successes).sum();
        let operations = phases.iter().map(|p| p.operations).sum();
        let mut errors = BTreeMap::new();
        for phase in &phases {
            for (name, count) in &phase.errors {
                *errors.entry(name.clone()).or_default() += count;
            }
        }
        WorkloadReport {
            name: self.name.clone(),
            operations,
            successes,
            failures: operations - successes,
            duration_ms: elapsed.as_millis(),
            qps: successes as f64 / elapsed.as_secs_f64().max(0.001),
            attempt_qps: operations as f64 / elapsed.as_secs_f64().max(0.001),
            rows: self.rows,
            bytes: self.bytes,
            attempts: self.attempts,
            conflicts: self.conflicts,
            max_success_gap_us: self.max_gap.max(self.last_success.elapsed()).as_micros() as u64,
            latency: self.latencies.summary(),
            success_latency: self.success_latency.summary(),
            errors,
            phase_summaries: phases,
            expected_errors: self.expected_errors.clone(),
            parameters: self.parameters.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn failures_do_not_increase_success_throughput() {
        let mut stats = Stats::new("test", vec![]);
        stats.observe("measurement", Duration::from_micros(10), Ok(()), Outcome::default());
        stats.phase("disturbance");
        stats.observe(
            "disturbance",
            Duration::from_micros(30),
            Err("network".into()),
            Outcome::default(),
        );
        let report = stats.report();
        assert_eq!((report.operations, report.successes, report.failures), (2, 1, 1));
        assert_eq!(report.attempt_qps, report.qps * 2.0);
        assert_eq!(report.latency.avg_us, 20);
        assert_eq!(report.success_latency.avg_us, 10);
    }
    #[test]
    fn histogram_is_bounded_and_preserves_tail() {
        let mut histogram = Latencies::default();
        for v in 0..100_000 {
            histogram.observe(v);
        }
        assert!(histogram.buckets.len() < 1000);
        let summary = histogram.summary();
        assert_eq!(summary.avg_us, 49_999);
        assert!((98_999..=100_000).contains(&summary.p99_us));
    }
}
