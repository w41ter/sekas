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

use anyhow::{Context as _, Result, anyhow, bail};
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
    pub(crate) fn mark(&mut self, name: String) {
        self.marks.push(MetricMark {
            name,
            at_unix_ms: crate::perf_lab::unix_millis(),
            metrics: prometheus::gather(),
        });
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SuiteReport {
    pub(crate) schema_version: u32,
    pub(crate) run_id: String,
    pub(crate) reports: Vec<CaseReport>,
    #[serde(default)]
    pub(crate) errors: BTreeMap<String, String>,
}

impl CaseReport {
    pub(crate) fn counter_delta_contains(&self, name: &str) -> f64 {
        self.metric_intervals
            .iter()
            .flat_map(|interval| interval.counters.iter())
            .filter(|(key, _)| key.contains(name))
            .map(|(_, value)| *value)
            .sum()
    }
}

pub(crate) fn case_report(
    lab: &LabContext,
    name: &str,
    workloads: Vec<WorkloadReport>,
    mut derived: BTreeMap<String, f64>,
) -> CaseReport {
    for workload in &workloads {
        let prefix = &workload.name;
        for (key, value) in &workload.parameters {
            derived.insert(format!("{prefix}.params.{key}"), *value);
        }
        derived.insert(format!("{prefix}.qps"), workload.qps);
        derived.insert(format!("{prefix}.attempt_qps"), workload.attempt_qps);
        derived.insert(format!("{prefix}.avg_us"), workload.latency.avg_us as f64);
        derived.insert(format!("{prefix}.p99_us"), workload.latency.p99_us as f64);
        derived.insert(format!("{prefix}.success.avg_us"), workload.success_latency.avg_us as f64);
        derived.insert(format!("{prefix}.success.p99_us"), workload.success_latency.p99_us as f64);
        derived.insert(format!("{prefix}.max_success_gap_us"), workload.max_success_gap_us as f64);
        derived.insert(format!("{prefix}.failure_rate"), failure_rate(workload));
        derived.insert(
            format!("{prefix}.unexpected_failure_rate"),
            workload.unexpected_failures() as f64 / workload.operations.max(1) as f64,
        );
        derived.insert(
            format!("{prefix}.conflict_rate"),
            workload.conflicts as f64 / workload.attempts.max(1) as f64,
        );
        derived.insert(
            format!("{prefix}.attempts_per_success"),
            workload.attempts as f64 / workload.successes.max(1) as f64,
        );
        let seconds = (workload.duration_ms as f64 / 1000.0).max(0.001);
        derived.insert(format!("{prefix}.rows_per_sec"), workload.rows as f64 / seconds);
        derived.insert(format!("{prefix}.bytes_per_sec"), workload.bytes as f64 / seconds);
        for phase in &workload.phase_summaries {
            let phase_prefix = format!("{prefix}.phase.{}", phase.name);
            derived.insert(format!("{phase_prefix}.qps"), phase.qps);
            derived.insert(format!("{phase_prefix}.avg_us"), phase.latency.avg_us as f64);
            derived.insert(format!("{phase_prefix}.p99_us"), phase.latency.p99_us as f64);
        }
        for (kind, count) in &workload.errors {
            derived.insert(format!("{prefix}.errors.{kind}"), *count as f64);
        }
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
) -> Result<Option<ComparisonReport>> {
    let Some(baseline) = read_baseline_for_case(baseline_path, &current.case)? else {
        return Ok(None);
    };
    let comparable = |config: &LabConfig| {
        serde_json::json!({
            "build_profile": config.build_profile,
            "runner_threads": config.runner_threads, "cluster": config.cluster,
            "workload": config.workload
        })
    };
    if comparable(&current.config) != comparable(&baseline.config) {
        bail!(
            "{} baseline workload/cluster configuration differs; regenerate baseline with the same profile",
            current.case
        );
    }
    let mut checks = Vec::new();
    for (metric, value) in &current.derived {
        let Some(base) = baseline.derived.get(metric) else {
            continue;
        };
        if metric.contains(".params.") && value != base {
            bail!(
                "{} workload parameter {} differs from baseline; regenerate baseline",
                current.case,
                metric
            );
        }
        if (metric.ends_with(".qps") && !metric.ends_with(".attempt_qps"))
            || metric.ends_with(".rows_per_sec")
            || metric.ends_with(".bytes_per_sec")
        {
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
        } else if metric.ends_with(".avg_us")
            || metric.ends_with(".p99_us")
            || metric.ends_with("_duration_ms")
            || metric.ends_with(".max_success_gap_us")
        {
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
        } else if metric.ends_with(".unexpected_failure_rate") {
            checks.push(ComparisonCheck {
                metric: metric.clone(),
                baseline: *base,
                current: *value,
                delta_percent: (*value - *base) * 100.0,
                threshold_percent: current.config.report.max_failure_rate * 100.0,
                failed: *value > current.config.report.max_failure_rate,
                direction: "absolute".to_owned(),
            });
        }
    }
    let failed = checks.iter().any(|check| check.failed);
    Ok(Some(ComparisonReport { baseline: baseline_path.display().to_string(), failed, checks }))
}

pub(crate) fn read_baseline_reports(path: &Path) -> Result<Vec<CaseReport>> {
    let bytes = fs::read(path).with_context(|| format!("read baseline {}", path.display()))?;
    let suite: SuiteReport = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse baseline suite {}", path.display()))?;
    if suite.reports.is_empty() {
        bail!("baseline {} contains no reports", path.display());
    }
    if suite.schema_version != 2 {
        bail!("perf-lab baseline schema changed; regenerate a version 2 baseline");
    }
    if !suite.errors.is_empty() {
        bail!("perf-lab baseline contains failed cases; use a successful suite");
    }
    Ok(suite.reports)
}

fn read_baseline_for_case(path: &Path, case: &str) -> Result<Option<CaseReport>> {
    let mut matched = read_baseline_reports(path)?
        .into_iter()
        .filter(|report| report.case == case)
        .collect::<Vec<_>>();
    match matched.len() {
        0 => Ok(None),
        1 => Ok(Some(matched.remove(0))),
        _ => Err(anyhow!(
            "baseline {} contains multiple reports for case '{}'",
            path.display(),
            case
        )),
    }
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

fn seconds_to_us(seconds: f64) -> u64 {
    (seconds * 1_000_000.0).max(0.0) as u64
}

fn failure_rate(report: &WorkloadReport) -> f64 {
    if report.operations == 0 { 0.0 } else { report.failures as f64 / report.operations as f64 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_checks_failure_rates_event_durations_and_profiles() {
        let config = LabConfig::default();
        let report = CaseReport {
            case: "leader-transfer".into(),
            run_id: "test".into(),
            config,
            workloads: vec![],
            derived: BTreeMap::from([
                ("write.unexpected_failure_rate".into(), 0.0),
                ("event_duration_ms".into(), 100.0),
            ]),
            metric_intervals: vec![],
        };
        let path =
            std::env::temp_dir().join(format!("perf-lab-checks-{}.json", std::process::id()));
        fs::write(
            &path,
            serde_json::to_vec(&SuiteReport {
                schema_version: 2,
                run_id: "test".into(),
                reports: vec![report.clone()],
                errors: BTreeMap::new(),
            })
            .unwrap(),
        )
        .unwrap();
        let mut current = report;
        current.derived.insert("write.unexpected_failure_rate".into(), 0.01);
        current.derived.insert("event_duration_ms".into(), 120.0);
        let comparison = compare_with_baseline(&current, &path).unwrap().unwrap();
        assert_eq!(comparison.checks.len(), 2);
        assert!(comparison.checks.iter().all(|check| check.failed));
        current.config.workload.concurrency += 1;
        assert!(compare_with_baseline(&current, &path).is_err());
        let failed_suite = SuiteReport {
            schema_version: 2,
            run_id: "failed".into(),
            reports: vec![current],
            errors: BTreeMap::from([("gc-backlog".into(), "GC did not reclaim versions".into())]),
        };
        fs::write(&path, serde_json::to_vec(&failed_suite).unwrap()).unwrap();
        assert!(read_baseline_reports(&path).is_err());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn average_latency_uses_latency_regression_threshold() {
        let baseline = CaseReport {
            case: "point-read".to_owned(),
            run_id: "baseline".to_owned(),
            config: LabConfig::default(),
            workloads: vec![],
            derived: BTreeMap::from([
                ("point_read.avg_us".to_owned(), 100.0),
                ("point_read.p99_us".to_owned(), 200.0),
                ("point_read.qps".to_owned(), 1000.0),
            ]),
            metric_intervals: vec![],
        };
        let path = std::env::temp_dir().join(format!(
            "sekas-perf-lab-avg-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        let mut suite = SuiteReport {
            schema_version: 2,
            run_id: "baseline".to_owned(),
            reports: vec![baseline],
            errors: BTreeMap::new(),
        };
        fs::write(&path, serde_json::to_vec(&suite).unwrap()).unwrap();
        let mut current = suite.reports[0].clone();
        let mut comparisons = Vec::new();
        for avg in [90.0, 110.0, 111.0] {
            current.derived.insert("point_read.avg_us".to_owned(), avg);
            comparisons.push(compare_with_baseline(&current, &path).unwrap().unwrap());
        }

        // Compact baselines created before avg_us was added still compare QPS and P99.
        suite.reports[0].derived.remove("point_read.avg_us");
        fs::write(&path, serde_json::to_vec(&suite).unwrap()).unwrap();
        let legacy_comparison = compare_with_baseline(&current, &path).unwrap().unwrap();
        fs::remove_file(&path).unwrap();

        for (comparison, (delta, failed)) in
            comparisons.iter().zip([(-10.0, false), (10.0, false), (11.0, true)])
        {
            let check = comparison.checks.iter().find(|c| c.metric == "point_read.avg_us").unwrap();
            assert_eq!(check.baseline, 100.0);
            assert_eq!(check.delta_percent, delta);
            assert_eq!(check.threshold_percent, 10.0);
            assert_eq!(check.direction, "increase");
            assert_eq!(check.failed, failed);
            assert_eq!(comparison.failed(), failed);
            assert_eq!(comparison.checks.len(), 3);
        }
        assert_eq!(legacy_comparison.checks.len(), 2);
        assert!(!legacy_comparison.failed());
    }
}
