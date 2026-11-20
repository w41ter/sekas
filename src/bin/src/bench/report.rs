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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use prometheus::core::Metric;
use prometheus::{Histogram, HistogramOpts, exponential_buckets, proto};
use tokio::time::Instant;

use super::workload::OperationKind;

pub(super) struct Metrics {
    operations: [OperationMetrics; OperationKind::ALL.len()],
}

struct OperationMetrics {
    latency: Histogram,
    rejected: AtomicU64,
    failures: AtomicU64,
}

pub(super) enum Outcome {
    Success,
    Rejected,
    Error,
}

#[derive(Clone)]
pub(super) struct Snapshot {
    time: Instant,
    operations: [OperationSnapshot; OperationKind::ALL.len()],
}

#[derive(Clone)]
struct OperationSnapshot {
    latency: proto::Metric,
    rejected: u64,
    failures: u64,
}

impl Metrics {
    pub fn new() -> Self {
        let opts = HistogramOpts::new("benchmark_latency_seconds", "Successful operation latency")
            .buckets(exponential_buckets(0.00005, 1.8, 26).unwrap());
        Self {
            operations: OperationKind::ALL.map(|_| OperationMetrics {
                latency: Histogram::with_opts(opts.clone()).unwrap(),
                rejected: AtomicU64::new(0),
                failures: AtomicU64::new(0),
            }),
        }
    }

    pub fn record(&self, kind: OperationKind, outcome: Outcome, elapsed: Duration) {
        let operation = &self.operations[kind as usize];
        match outcome {
            Outcome::Success => operation.latency.observe(elapsed.as_secs_f64()),
            Outcome::Rejected => {
                operation.rejected.fetch_add(1, Ordering::Relaxed);
            }
            Outcome::Error => {
                operation.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            time: Instant::now(),
            operations: self.operations.each_ref().map(|operation| OperationSnapshot {
                latency: operation.latency.metric(),
                rejected: operation.rejected.load(Ordering::Relaxed),
                failures: operation.failures.load(Ordering::Relaxed),
            }),
        }
    }
}

impl Snapshot {
    pub fn failures(&self) -> u64 {
        self.operations.iter().map(|operation| operation.failures).sum()
    }

    pub fn display_since(&self, previous: &Self) {
        let elapsed = (self.time - previous.time).as_secs_f64();
        for kind in OperationKind::ALL {
            let current = &self.operations[kind as usize];
            let prior = &previous.operations[kind as usize];
            let failures = current.failures - prior.failures;
            let rejected = current.rejected - prior.rejected;
            let current = current.latency.get_histogram();
            let prior = prior.latency.get_histogram();
            let successes = current.get_sample_count() - prior.get_sample_count();
            if successes == 0 && failures == 0 && rejected == 0 {
                continue;
            }
            let ops_per_sec = successes as f64 / elapsed.max(f64::EPSILON);
            print!(
                "{} elapsed={elapsed:.3}s success={successes} rejected={rejected} errors={failures} ops/s={ops_per_sec:.1}",
                kind.name()
            );
            if successes > 0 {
                let avg = (current.get_sample_sum() - prior.get_sample_sum()) / successes as f64;
                let p99_rank = successes - successes / 100;
                let p99 = current
                    .get_bucket()
                    .iter()
                    .zip(prior.get_bucket())
                    .find(|(current, prior)| {
                        current.cumulative_count() - prior.cumulative_count() >= p99_rank
                    })
                    .map_or(f64::INFINITY, |(bucket, _)| bucket.upper_bound());
                print!(" avg={:.0}us p99={:.0}us", avg * 1e6, p99 * 1e6);
            }
            println!();
        }
    }
}
