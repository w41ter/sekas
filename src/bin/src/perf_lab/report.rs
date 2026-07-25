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
use std::fs;
use std::path::Path;

use anyhow::{Context as _, Result};
use prometheus::proto::{Metric, MetricFamily};
use serde::{Deserialize, Serialize};

use super::config::LabConfig;
use super::{LabContext, WorkloadReport};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MetricInterval {
    from: String,
    to: String,
    duration_ms: u128,
    counters: BTreeMap<String, f64>,
    histograms: BTreeMap<String, HistogramSummary>,
}

impl MetricInterval {
    fn from_marks(start: &MetricMark, end: &MetricMark) -> Self {
        let start_map = flatten_metric_families(&start.metrics);
        let end_map = flatten_metric_families(&end.metrics);
        let mut counters = BTreeMap::new();
        let mut histograms = BTreeMap::new();
        for (key, current) in end_map {
            match current {
                FlatMetric::Counter(v) => {
                    let prev = start_map.get(&key).and_then(FlatMetric::counter).unwrap_or(0.0);
                    counters.insert(key, (v - prev).max(0.0));
                }
                FlatMetric::Gauge(v) => {
                    counters.insert(format!("gauge:{key}"), v);
                }
                FlatMetric::Histogram(h) => {
                    let prev = start_map.get(&key).and_then(FlatMetric::histogram);
                    histograms.insert(key, h.diff(prev));
                }
            }
        }
        MetricInterval {
            from: start.name.clone(),
            to: end.name.clone(),
            duration_ms: end.at_unix_ms.saturating_sub(start.at_unix_ms),
            counters,
            histograms,
        }
    }
}

#[derive(Default)]
pub(crate) struct MetricsRecorder {
    marks: Vec<MetricMark>,
}

impl MetricsRecorder {
    pub(crate) fn mark(&mut self, name: String) -> Result<()> {
        self.marks.push(MetricMark {
            name,
            at_unix_ms: crate::perf_lab::unix_millis(),
            metrics: prometheus::gather(),
        });
        Ok(())
    }

    pub(crate) fn intervals(&self) -> Vec<MetricInterval> {
        self.marks.windows(2).map(|pair| MetricInterval::from_marks(&pair[0], &pair[1])).collect()
    }
}

struct MetricMark {
    name: String,
    at_unix_ms: u128,
    metrics: Vec<MetricFamily>,
}

#[derive(Clone)]
enum FlatMetric {
    Counter(f64),
    Gauge(f64),
    Histogram(FlatHistogram),
}

impl FlatMetric {
    fn counter(&self) -> Option<f64> {
        match self {
            FlatMetric::Counter(v) => Some(*v),
            FlatMetric::Gauge(_) | FlatMetric::Histogram(_) => None,
        }
    }

    fn histogram(&self) -> Option<&FlatHistogram> {
        match self {
            FlatMetric::Histogram(v) => Some(v),
            FlatMetric::Counter(_) | FlatMetric::Gauge(_) => None,
        }
    }
}

#[derive(Clone)]
struct FlatHistogram {
    sample_count: u64,
    sample_sum: f64,
    buckets: Vec<(f64, u64)>,
}

impl FlatHistogram {
    fn diff(&self, previous: Option<&FlatHistogram>) -> HistogramSummary {
        let prev_count = previous.map(|v| v.sample_count).unwrap_or_default();
        let prev_sum = previous.map(|v| v.sample_sum).unwrap_or_default();
        let buckets = self
            .buckets
            .iter()
            .enumerate()
            .map(|(idx, (upper, count))| {
                let prev = previous
                    .and_then(|h| h.buckets.get(idx))
                    .map(|(_, count)| *count)
                    .unwrap_or_default();
                (*upper, count.saturating_sub(prev))
            })
            .collect::<Vec<_>>();
        HistogramSummary::from_buckets(
            self.sample_count.saturating_sub(prev_count),
            (self.sample_sum - prev_sum).max(0.0),
            &buckets,
        )
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct HistogramSummary {
    pub(crate) count: u64,
    pub(crate) avg_us: u64,
    pub(crate) p50_us: u64,
    pub(crate) p95_us: u64,
    pub(crate) p99_us: u64,
    pub(crate) p999_us: u64,
    pub(crate) max_us: u64,
}

impl HistogramSummary {
    pub(crate) fn from_latencies(latencies: &[u64]) -> Self {
        if latencies.is_empty() {
            return HistogramSummary::default();
        }
        let mut values = latencies.to_vec();
        values.sort_unstable();
        let sum = values.iter().sum::<u64>();
        HistogramSummary {
            count: values.len() as u64,
            avg_us: sum / values.len() as u64,
            p50_us: percentile_sorted(&values, 0.50),
            p95_us: percentile_sorted(&values, 0.95),
            p99_us: percentile_sorted(&values, 0.99),
            p999_us: percentile_sorted(&values, 0.999),
            max_us: *values.last().unwrap(),
        }
    }

    fn from_buckets(count: u64, sample_sum_seconds: f64, buckets: &[(f64, u64)]) -> Self {
        if count == 0 {
            return HistogramSummary::default();
        }
        let value = |percentile: f64| -> u64 {
            let target = (percentile * count as f64).ceil() as u64;
            buckets
                .iter()
                .find(|(_, cumulative)| *cumulative >= target)
                .map(|(upper, _)| seconds_to_us(*upper))
                .unwrap_or_default()
        };
        let max = buckets
            .iter()
            .find(|(_, cumulative)| *cumulative >= count)
            .map(|(upper, _)| seconds_to_us(*upper))
            .unwrap_or_default();
        HistogramSummary {
            count,
            avg_us: seconds_to_us(sample_sum_seconds / count as f64),
            p50_us: value(0.50),
            p95_us: value(0.95),
            p99_us: value(0.99),
            p999_us: value(0.999),
            max_us: max,
        }
    }
}

fn flatten_metric_families(metrics: &[MetricFamily]) -> BTreeMap<String, FlatMetric> {
    let mut out = BTreeMap::new();
    for family in metrics {
        for metric in family.get_metric() {
            let key = metric_key(family.get_name(), metric);
            if metric.has_counter() {
                out.insert(key, FlatMetric::Counter(metric.get_counter().get_value()));
            } else if metric.has_gauge() {
                out.insert(key, FlatMetric::Gauge(metric.get_gauge().get_value()));
            } else if metric.has_histogram() {
                let h = metric.get_histogram();
                let buckets = h
                    .get_bucket()
                    .iter()
                    .map(|bucket| (bucket.get_upper_bound(), bucket.get_cumulative_count()))
                    .collect();
                out.insert(
                    key,
                    FlatMetric::Histogram(FlatHistogram {
                        sample_count: h.get_sample_count(),
                        sample_sum: h.get_sample_sum(),
                        buckets,
                    }),
                );
            }
        }
    }
    out
}

fn metric_key(name: &str, metric: &Metric) -> String {
    let labels = metric
        .get_label()
        .iter()
        .map(|label| format!("{}={}", label.get_name(), label.get_value()))
        .collect::<Vec<_>>();
    if labels.is_empty() { name.to_owned() } else { format!("{name}{{{}}}", labels.join(",")) }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CaseReport {
    pub(crate) case: String,
    pub(crate) run_id: String,
    pub(crate) config: LabConfig,
    pub(crate) workloads: Vec<WorkloadReport>,
    pub(crate) derived: BTreeMap<String, f64>,
    pub(crate) metric_intervals: Vec<MetricInterval>,
}

pub(crate) fn case_report(
    lab: &LabContext,
    name: &str,
    workloads: Vec<WorkloadReport>,
    mut derived: BTreeMap<String, f64>,
) -> CaseReport {
    for workload in &workloads {
        derived.insert(format!("{}.qps", workload.name), workload.qps);
        derived.insert(format!("{}.p99_us", workload.name), workload.latency.p99_us as f64);
        derived.insert(format!("{}.failure_rate", workload.name), failure_rate(workload));
    }
    CaseReport {
        case: name.to_owned(),
        run_id: lab.run_id.clone(),
        config: lab.config.clone(),
        workloads,
        derived,
        metric_intervals: lab.metrics.intervals(),
    }
}

pub(crate) fn compare_with_baseline(
    current: &CaseReport,
    baseline_path: &Path,
    fail_on_regression: bool,
) -> Result<ComparisonReport> {
    let baseline: CaseReport = serde_json::from_slice(
        &fs::read(baseline_path)
            .with_context(|| format!("read baseline {}", baseline_path.display()))?,
    )
    .with_context(|| format!("parse baseline {}", baseline_path.display()))?;
    let mut checks = Vec::new();
    for (metric, value) in &current.derived {
        let Some(base) = baseline.derived.get(metric) else {
            continue;
        };
        if metric.ends_with(".qps") {
            let drop_percent =
                if *base <= f64::EPSILON { 0.0 } else { ((*base - *value) / *base) * 100.0 };
            checks.push(ComparisonCheck {
                metric: metric.clone(),
                baseline: *base,
                current: *value,
                delta_percent: drop_percent,
                threshold_percent: current.config.report.max_qps_drop_percent,
                failed: drop_percent > current.config.report.max_qps_drop_percent,
                direction: "drop".to_owned(),
            });
        } else if metric.ends_with(".p99_us") {
            let increase_percent =
                if *base <= f64::EPSILON { 0.0 } else { ((*value - *base) / *base) * 100.0 };
            checks.push(ComparisonCheck {
                metric: metric.clone(),
                baseline: *base,
                current: *value,
                delta_percent: increase_percent,
                threshold_percent: current.config.report.max_latency_increase_percent,
                failed: increase_percent > current.config.report.max_latency_increase_percent,
                direction: "increase".to_owned(),
            });
        }
    }
    let failed = fail_on_regression && checks.iter().any(|check| check.failed);
    Ok(ComparisonReport { baseline: baseline_path.display().to_string(), failed, checks })
}

#[derive(Debug, Serialize)]
pub(crate) struct ComparisonReport {
    baseline: String,
    failed: bool,
    checks: Vec<ComparisonCheck>,
}

impl ComparisonReport {
    pub(crate) fn failed(&self) -> bool {
        self.failed
    }
}

#[derive(Debug, Serialize)]
struct ComparisonCheck {
    metric: String,
    baseline: f64,
    current: f64,
    delta_percent: f64,
    threshold_percent: f64,
    failed: bool,
    direction: String,
}

fn percentile_sorted(values: &[u64], percentile: f64) -> u64 {
    let idx = ((values.len() as f64 * percentile).ceil() as usize).saturating_sub(1);
    values[idx.min(values.len() - 1)]
}

fn seconds_to_us(seconds: f64) -> u64 {
    (seconds * 1_000_000.0).max(0.0) as u64
}

fn failure_rate(report: &WorkloadReport) -> f64 {
    if report.operations == 0 { 0.0 } else { report.failures as f64 / report.operations as f64 }
}
