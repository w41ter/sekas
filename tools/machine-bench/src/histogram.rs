use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Bucket {
    pub(crate) upper_bound_ns: u64,
    pub(crate) count: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Summary {
    pub(crate) samples: u64,
    pub(crate) operations: u64,
    pub(crate) elapsed_secs: f64,
    pub(crate) avg_us: f64,
    pub(crate) p50_us: f64,
    pub(crate) p99_us: f64,
    pub(crate) p999_us: f64,
    pub(crate) max_us: f64,
    pub(crate) ops_per_sec: f64,
    pub(crate) mib_per_sec: f64,
    pub(crate) buckets: Vec<Bucket>,
}

#[derive(Default)]
pub(crate) struct Histogram {
    buckets: BTreeMap<u64, u64>,
    samples: u64,
    sum_ns: u128,
    max_ns: u64,
}

impl Histogram {
    pub(crate) fn observe_n(&mut self, ns: u64, count: u64) {
        // Exact to 16 ns below 1 us, then keep the upper bound within 1/64.
        let shift = (64 - ns.leading_zeros()).saturating_sub(6);
        let width = (1_u64 << shift).max(16);
        let bound = ns.saturating_add(width - 1) / width * width;
        *self.buckets.entry(bound).or_default() += count;
        self.samples += count;
        self.sum_ns += u128::from(ns) * u128::from(count);
        self.max_ns = self.max_ns.max(ns);
    }

    fn percentile_ns(&self, percentile: f64) -> u64 {
        let rank = (self.samples as f64 * percentile).ceil() as u64;
        let mut seen = 0;
        for (bound, count) in &self.buckets {
            seen += count;
            if seen >= rank {
                return (*bound).min(self.max_ns);
            }
        }
        self.max_ns
    }

    pub(crate) fn summarize(
        self,
        operations: u64,
        elapsed: Duration,
        bytes_per_operation: u64,
    ) -> Summary {
        let elapsed_secs = elapsed.as_secs_f64();
        let ops_per_sec = operations as f64 / elapsed_secs;
        Summary {
            samples: self.samples,
            operations,
            elapsed_secs,
            avg_us: self.sum_ns as f64 / self.samples.max(1) as f64 / 1_000.0,
            p50_us: self.percentile_ns(0.50) as f64 / 1_000.0,
            p99_us: self.percentile_ns(0.99) as f64 / 1_000.0,
            p999_us: self.percentile_ns(0.999) as f64 / 1_000.0,
            max_us: self.max_ns as f64 / 1_000.0,
            ops_per_sec,
            mib_per_sec: ops_per_sec * bytes_per_operation as f64 / (1024.0 * 1024.0),
            buckets: self
                .buckets
                .into_iter()
                .map(|(upper_bound_ns, count)| Bucket { upper_bound_ns, count })
                .collect(),
        }
    }
}

pub(crate) fn measure<F>(
    warmup: Duration,
    duration: Duration,
    bytes_per_operation: u64,
    mut operation: F,
) -> Result<Summary>
where
    F: FnMut() -> Result<()>,
{
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        operation()?;
    }

    let mut histogram = Histogram::default();
    let started = Instant::now();
    let deadline = started + duration;
    let mut operations = 0;
    while Instant::now() < deadline {
        let before = Instant::now();
        operation()?;
        let elapsed = before.elapsed();
        histogram.observe_n(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64, 1);
        operations += 1;
    }
    Ok(histogram.summarize(operations, started.elapsed(), bytes_per_operation))
}

pub(crate) fn measure_batched<F>(
    warmup: Duration,
    duration: Duration,
    batch_size: u64,
    bytes_per_operation: u64,
    mut batch: F,
) -> Result<Summary>
where
    F: FnMut(u64) -> Result<()>,
{
    let warmup_deadline = Instant::now() + warmup;
    while Instant::now() < warmup_deadline {
        batch(batch_size)?;
    }

    let mut histogram = Histogram::default();
    let started = Instant::now();
    let deadline = started + duration;
    let mut operations = 0;
    while Instant::now() < deadline {
        let before = Instant::now();
        batch(batch_size)?;
        let per_operation = before.elapsed().as_nanos() / u128::from(batch_size);
        histogram.observe_n(per_operation.min(u128::from(u64::MAX)) as u64, batch_size);
        operations += batch_size;
    }
    Ok(histogram.summarize(operations, started.elapsed(), bytes_per_operation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_preserves_count_and_tail() {
        let mut histogram = Histogram::default();
        histogram.observe_n(1_000, 99);
        histogram.observe_n(1_000_000, 1);
        let summary = histogram.summarize(100, Duration::from_secs(1), 4096);
        assert_eq!(summary.samples, 100);
        assert!((1.0..=1.016).contains(&summary.p50_us));
        assert!((1.0..=1.016).contains(&summary.p99_us));
        assert_eq!(summary.max_us, 1_000.0);
    }
}
