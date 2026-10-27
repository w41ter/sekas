// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::fs;

use prometheus::proto::MetricFamily;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct HistogramSnapshot {
    pub(crate) count: u64,
    pub(crate) sum: f64,
    pub(crate) buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HistogramBucket {
    pub(crate) upper_bound: f64,
    pub(crate) cumulative_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HistogramDelta {
    #[serde(flatten)]
    pub(crate) samples: HistogramSnapshot,
    pub(crate) reset: bool,
    pub(crate) avg: f64,
    // Upper bounds, not interpolated quantiles. None means overflow of the
    // highest finite bucket. Units follow the original metric.
    pub(crate) p50_upper_bound: Option<f64>,
    pub(crate) p99_upper_bound: Option<f64>,
    pub(crate) p999_upper_bound: Option<f64>,
}

impl HistogramSnapshot {
    pub(crate) fn delta(&self, start: Option<&Self>) -> HistogramDelta {
        let reset = start.is_some_and(|s| {
            self.count < s.count
                || self.sum < s.sum
                || self.buckets.len() != s.buckets.len()
                || self.buckets.iter().zip(&s.buckets).any(|(end, start)| {
                    end.upper_bound != start.upper_bound
                        || end.cumulative_count < start.cumulative_count
                })
        });
        let start = start.filter(|_| !reset);
        let count = self.count - start.map_or(0, |s| s.count);
        let sum = (self.sum - start.map_or(0.0, |s| s.sum)).max(0.0);
        let buckets: Vec<_> = self
            .buckets
            .iter()
            .enumerate()
            .map(|(i, bucket)| HistogramBucket {
                upper_bound: bucket.upper_bound,
                cumulative_count: bucket.cumulative_count
                    - start.map_or(0, |s| s.buckets[i].cumulative_count),
            })
            .collect();
        let quantile = |q: f64| {
            if count == 0 {
                return None;
            }
            let rank = (count as f64 * q).ceil() as u64;
            buckets.iter().find(|b| b.cumulative_count >= rank).map(|b| b.upper_bound)
        };
        HistogramDelta {
            reset,
            avg: if count == 0 { 0.0 } else { sum / count as f64 },
            p50_upper_bound: quantile(0.5),
            p99_upper_bound: quantile(0.99),
            p999_upper_bound: quantile(0.999),
            samples: HistogramSnapshot { count, sum, buckets },
        }
    }
}

pub(crate) fn histograms(families: &[MetricFamily]) -> BTreeMap<String, HistogramSnapshot> {
    families
        .iter()
        .flat_map(|family| {
            family.get_metric().iter().filter_map(|metric| {
                let histogram = metric.histogram.as_ref()?;
                Some((
                    super::report::metric_key(family.name(), metric),
                    HistogramSnapshot {
                        count: histogram.sample_count(),
                        sum: histogram.sample_sum(),
                        buckets: histogram
                            .get_bucket()
                            .iter()
                            .filter_map(|bucket| {
                                bucket.upper_bound().is_finite().then(|| HistogramBucket {
                                    upper_bound: bucket.upper_bound(),
                                    cumulative_count: bucket.cumulative_count(),
                                })
                            })
                            .collect(),
                    },
                ))
            })
        })
        .collect()
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct ProcessSnapshot {
    // Raw clock ticks; convert with the host's getconf CLK_TCK.
    pub(crate) user_ticks: u64,
    pub(crate) system_ticks: u64,
    pub(crate) threads: u64,
    pub(crate) rss_kib: u64,
    pub(crate) rss_anon_kib: u64,
}

pub(crate) fn process_snapshot() -> Option<ProcessSnapshot> {
    let stat = fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(") ")?.1.split_whitespace().collect();
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let value = |name: &str| -> Option<u64> {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    };
    Some(ProcessSnapshot {
        user_ticks: fields.get(11)?.parse().ok()?,
        system_ticks: fields.get(12)?.parse().ok()?,
        threads: fields.get(17)?.parse().ok()?,
        rss_kib: value("VmRSS:")?,
        rss_anon_kib: value("RssAnon:")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(count: u64, sum: f64, counts: &[u64]) -> HistogramSnapshot {
        HistogramSnapshot {
            count,
            sum,
            buckets: counts
                .iter()
                .enumerate()
                .map(|(i, count)| HistogramBucket {
                    upper_bound: (i + 1) as f64,
                    cumulative_count: *count,
                })
                .collect(),
        }
    }

    #[test]
    fn delta_uses_window_samples_and_reports_overflow() {
        let start = snapshot(100, 150.0, &[50, 90, 100]);
        let end = snapshot(110, 170.0, &[55, 98, 109]);
        let delta = end.delta(Some(&start));
        assert_eq!(delta.samples.count, 10);
        assert_eq!(delta.avg, 2.0);
        assert_eq!(delta.p50_upper_bound, Some(1.0));
        assert_eq!(delta.p99_upper_bound, None);
        assert_eq!(delta.samples.buckets[2].cumulative_count, 9);
        assert!(!delta.reset);
        let empty = end.delta(Some(&end));
        assert_eq!(empty.samples.count, 0);
        assert_eq!(empty.p50_upper_bound, None);
    }

    #[test]
    fn reset_and_new_series_use_only_current_samples() {
        let start = snapshot(100, 150.0, &[50, 90, 100]);
        let end = snapshot(5, 8.0, &[3, 4, 5]);
        let reset = end.delta(Some(&start));
        assert!(reset.reset);
        assert_eq!(reset.samples.count, 5);
        assert_eq!(reset.p99_upper_bound, Some(3.0));
        let new = end.delta(None);
        assert!(!new.reset);
        assert_eq!(new.samples.count, 5);
        assert_eq!(new.samples.sum, 8.0);
    }
}
