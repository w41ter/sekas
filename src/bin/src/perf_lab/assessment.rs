// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use super::machine::{MachineProfile, Primitive};
use super::report::CaseReport;
use super::workload::WorkloadReport;

pub(super) const STANDARD_ID: &str = "machine-modeled-v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AssessmentStatus {
    Good,
    Acceptable,
    NeedsWork,
    Unknown,
}

impl AssessmentStatus {
    fn severity(self) -> u8 {
        match self {
            Self::Good => 0,
            Self::Acceptable => 1,
            Self::Unknown => 2,
            Self::NeedsWork => 3,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Good => "GOOD",
            Self::Acceptable => "ACCEPTABLE",
            Self::NeedsWork => "NEEDS_WORK",
            Self::Unknown => "UNKNOWN",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CaseAssessment {
    pub(crate) standard: String,
    pub(crate) standard_version: u32,
    pub(crate) status: AssessmentStatus,
    pub(crate) checks: Vec<AssessmentCheck>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) note: Option<String>,
}

impl CaseAssessment {
    pub(super) fn unavailable(note: impl Into<String>) -> Self {
        Self {
            standard: STANDARD_ID.to_owned(),
            standard_version: 1,
            status: AssessmentStatus::Unknown,
            checks: Vec::new(),
            note: Some(note.into()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct AssessmentCheck {
    pub(crate) id: String,
    pub(crate) workload: String,
    pub(crate) metric: String,
    pub(crate) unit: String,
    pub(crate) status: AssessmentStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) observed: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) estimated: Option<ValueRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) allowed: Option<ValueRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) relation_to_estimated: Option<f64>,
    pub(crate) reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<EvaluatedCost>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct ValueRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) min: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max: Option<f64>,
}

impl ValueRange {
    fn between(min: f64, max: f64) -> Self {
        Self { min: Some(min), max: Some(max) }
    }

    fn at_most(max: f64) -> Self {
        Self { min: None, max: Some(max) }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct EvaluatedCost {
    pub(crate) formula: String,
    pub(crate) estimate_us: ValueRange,
    pub(crate) components: Vec<CostComponent>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CostComponent {
    pub(crate) name: String,
    pub(crate) count: f64,
    pub(crate) min_us: f64,
    pub(crate) max_us: f64,
}

#[derive(Clone, Debug)]
pub(super) enum CostExpr {
    Primitive { primitive: Primitive, count: f64 },
    Sequential(Vec<CostExpr>),
    ParallelMax(Vec<CostExpr>),
    Repeat { count: u64, cost: Box<CostExpr> },
    Budget { min_us: f64, max_us: f64, reason: &'static str },
}

pub(super) fn primitive(primitive: Primitive, count: f64) -> CostExpr {
    CostExpr::Primitive { primitive, count }
}

pub(super) fn sequential(costs: Vec<CostExpr>) -> CostExpr {
    CostExpr::Sequential(costs)
}

#[allow(dead_code)]
pub(super) fn parallel_max(costs: Vec<CostExpr>) -> CostExpr {
    CostExpr::ParallelMax(costs)
}

#[allow(dead_code)]
pub(super) fn repeat(count: u64, cost: CostExpr) -> CostExpr {
    CostExpr::Repeat { count, cost: Box::new(cost) }
}

pub(super) fn budget(min_us: f64, max_us: f64, reason: &'static str) -> CostExpr {
    CostExpr::Budget { min_us, max_us, reason }
}

impl CostExpr {
    fn evaluate(&self, profile: &MachineProfile) -> Result<EvaluatedCost> {
        match self {
            Self::Primitive { primitive, count } => {
                if !count.is_finite() || *count < 0.0 {
                    bail!("primitive {} has invalid count {count}", primitive.name());
                }
                let value = profile.p50_us(*primitive)? * count;
                Ok(EvaluatedCost {
                    formula: if (*count - 1.0).abs() <= f64::EPSILON {
                        primitive.name().to_owned()
                    } else {
                        format!("{}*{count}", primitive.name())
                    },
                    estimate_us: ValueRange::between(value, value),
                    components: vec![CostComponent {
                        name: primitive.name().to_owned(),
                        count: *count,
                        min_us: value,
                        max_us: value,
                    }],
                })
            }
            Self::Budget { min_us, max_us, reason } => {
                validate_range(*min_us, *max_us)?;
                Ok(EvaluatedCost {
                    formula: format!("budget({reason})"),
                    estimate_us: ValueRange::between(*min_us, *max_us),
                    components: vec![CostComponent {
                        name: format!("budget:{reason}"),
                        count: 1.0,
                        min_us: *min_us,
                        max_us: *max_us,
                    }],
                })
            }
            Self::Sequential(costs) => {
                if costs.is_empty() {
                    bail!("sequential cost model is empty");
                }
                let evaluated =
                    costs.iter().map(|cost| cost.evaluate(profile)).collect::<Result<Vec<_>>>()?;
                combine(evaluated, " + ", |values| values.iter().sum())
            }
            Self::ParallelMax(costs) => {
                if costs.is_empty() {
                    bail!("parallel cost model is empty");
                }
                let evaluated =
                    costs.iter().map(|cost| cost.evaluate(profile)).collect::<Result<Vec<_>>>()?;
                combine(evaluated, " || ", |values| values.iter().copied().fold(0.0_f64, f64::max))
            }
            Self::Repeat { count, cost } => {
                let mut evaluated = cost.evaluate(profile)?;
                let count = *count as f64;
                let min = evaluated.estimate_us.min.unwrap_or_default() * count;
                let max = evaluated.estimate_us.max.unwrap_or_default() * count;
                evaluated.formula = format!("{}*{count}", evaluated.formula);
                evaluated.estimate_us = ValueRange::between(min, max);
                for component in &mut evaluated.components {
                    component.count *= count;
                    component.min_us *= count;
                    component.max_us *= count;
                }
                Ok(evaluated)
            }
        }
    }
}

fn combine(
    evaluated: Vec<EvaluatedCost>,
    separator: &str,
    aggregate: impl Fn(&[f64]) -> f64,
) -> Result<EvaluatedCost> {
    let mins =
        evaluated.iter().map(|item| item.estimate_us.min.unwrap_or_default()).collect::<Vec<_>>();
    let maxs =
        evaluated.iter().map(|item| item.estimate_us.max.unwrap_or_default()).collect::<Vec<_>>();
    let min = aggregate(&mins);
    let max = aggregate(&maxs);
    validate_range(min, max)?;
    Ok(EvaluatedCost {
        formula: format!(
            "({})",
            evaluated.iter().map(|item| item.formula.as_str()).collect::<Vec<_>>().join(separator)
        ),
        estimate_us: ValueRange::between(min, max),
        components: evaluated.into_iter().flat_map(|item| item.components).collect(),
    })
}

fn validate_range(min: f64, max: f64) -> Result<()> {
    if !min.is_finite() || !max.is_finite() || min < 0.0 || max < min {
        bail!("invalid cost range {min}..{max}");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(super) enum Metric {
    Qps,
    P50Us,
    P99Us,
    P999Us,
    MaxSuccessGapUs,
    Conflicts,
    AttemptsPerSuccess,
    RowsPerSecond,
    BytesPerSecond,
    UnexpectedFailures,
}

impl Metric {
    fn name(self) -> &'static str {
        match self {
            Self::Qps => "qps",
            Self::P50Us => "p50_us",
            Self::P99Us => "p99_us",
            Self::P999Us => "p999_us",
            Self::MaxSuccessGapUs => "max_success_gap_us",
            Self::Conflicts => "conflicts",
            Self::AttemptsPerSuccess => "attempts_per_success",
            Self::RowsPerSecond => "rows_per_second",
            Self::BytesPerSecond => "bytes_per_second",
            Self::UnexpectedFailures => "unexpected_failures",
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Self::Qps | Self::RowsPerSecond => "ops/s",
            Self::P50Us | Self::P99Us | Self::P999Us | Self::MaxSuccessGapUs => "us",
            Self::Conflicts | Self::UnexpectedFailures => "count",
            Self::AttemptsPerSuccess => "x",
            Self::BytesPerSecond => "B/s",
        }
    }

    fn value(self, report: &WorkloadReport) -> f64 {
        match self {
            Self::Qps => report.qps,
            Self::P50Us => report.latency.p50_us as f64,
            Self::P99Us => report.latency.p99_us as f64,
            Self::P999Us => report.latency.p999_us as f64,
            Self::MaxSuccessGapUs => report.max_success_gap_us as f64,
            Self::Conflicts => report.conflicts as f64,
            Self::AttemptsPerSuccess => report.attempts as f64 / report.successes.max(1) as f64,
            Self::RowsPerSecond => {
                report.rows as f64 / (report.duration_ms as f64 / 1000.0).max(0.001)
            }
            Self::BytesPerSecond => {
                report.bytes as f64 / (report.duration_ms as f64 / 1000.0).max(0.001)
            }
            Self::UnexpectedFailures => report.unexpected_failures() as f64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct MetricRef {
    workload: &'static str,
    metric: Metric,
}

impl MetricRef {
    pub(super) const fn new(workload: &'static str, metric: Metric) -> Self {
        Self { workload, metric }
    }
}

#[derive(Clone, Debug)]
pub(super) enum Expectation {
    ModeledLatency {
        id: &'static str,
        metric: MetricRef,
        model: CostExpr,
        allowed_multiplier: f64,
        fixed_jitter_us: f64,
    },
    RatioUpperBound {
        id: &'static str,
        numerator: MetricRef,
        denominator: MetricRef,
        estimated_max: f64,
        allowed_max: f64,
    },
    RatioLowerBound {
        id: &'static str,
        numerator: MetricRef,
        denominator: MetricRef,
        estimated_min: f64,
        allowed_min: f64,
    },
    AtMost {
        id: &'static str,
        metric: MetricRef,
        estimated_max: f64,
        allowed_max: f64,
    },
    AtLeast {
        id: &'static str,
        metric: MetricRef,
        estimated_min: f64,
        allowed_min: f64,
    },
    DerivedAtMost {
        id: &'static str,
        metric: &'static str,
        unit: &'static str,
        estimated_max: f64,
        allowed_max: f64,
    },
    DerivedAtLeast {
        id: &'static str,
        metric: &'static str,
        unit: &'static str,
        estimated_min: f64,
        allowed_min: f64,
    },
}

#[derive(Clone, Debug)]
pub(super) struct CaseExpectations {
    pub(super) standard_version: u32,
    pub(super) checks: Vec<Expectation>,
}

pub(super) fn assess(
    report: &CaseReport,
    expectations: CaseExpectations,
    profile: &MachineProfile,
) -> CaseAssessment {
    let specialized = !expectations.checks.is_empty();
    let mut checks = generic_checks(report);
    checks.extend(
        expectations
            .checks
            .into_iter()
            .map(|expectation| evaluate_expectation(report, expectation, profile))
            .collect::<Vec<_>>(),
    );
    let status = checks
        .iter()
        .map(|check| check.status)
        .max_by_key(|status| status.severity())
        .unwrap_or(AssessmentStatus::Unknown);
    CaseAssessment {
        standard: STANDARD_ID.to_owned(),
        standard_version: expectations.standard_version,
        status,
        checks,
        note: (!specialized).then(|| "generic reliability and tail standards only".to_owned()),
    }
}

fn generic_checks(report: &CaseReport) -> Vec<AssessmentCheck> {
    let mut checks = Vec::new();
    for workload in &report.workloads {
        let unexpected = workload.unexpected_failures() as f64;
        checks.push(AssessmentCheck {
            id: "unexpected-failures".to_owned(),
            workload: workload.name.clone(),
            metric: Metric::UnexpectedFailures.name().to_owned(),
            unit: Metric::UnexpectedFailures.unit().to_owned(),
            status: upper_bound_status(unexpected, 0.0, 0.0),
            observed: Some(unexpected),
            estimated: Some(ValueRange::at_most(0.0)),
            allowed: Some(ValueRange::at_most(0.0)),
            relation_to_estimated: None,
            reason: format!("observed {unexpected:.0}, allowed at most 0"),
            model: None,
        });

        let continuous = matches!(
            report.category,
            super::cases::CaseCategory::Sentinel | super::cases::CaseCategory::Scenario
        ) && workload.parameters.get("load.once").copied().unwrap_or_default()
            == 0.0
            && workload.operations >= 100
            && workload.latency.p50_us > 0;
        if continuous {
            let observed = workload.latency.p999_us as f64 / workload.latency.p50_us as f64;
            checks.push(AssessmentCheck {
                id: "tail-amplification".to_owned(),
                workload: workload.name.clone(),
                metric: "p999_us/p50_us".to_owned(),
                unit: "x".to_owned(),
                status: upper_bound_status(observed, 5.0, 10.0),
                observed: Some(observed),
                estimated: Some(ValueRange::at_most(5.0)),
                allowed: Some(ValueRange::at_most(10.0)),
                relation_to_estimated: Some(observed / 5.0),
                reason: format!(
                    "p999_us {} / p50_us {} = {observed:.3}x",
                    workload.latency.p999_us, workload.latency.p50_us
                ),
                model: None,
            });
        }
    }
    checks
}

fn evaluate_expectation(
    report: &CaseReport,
    expectation: Expectation,
    profile: &MachineProfile,
) -> AssessmentCheck {
    match expectation {
        Expectation::ModeledLatency { id, metric, model, allowed_multiplier, fixed_jitter_us } => {
            let Some(observed) = metric_value(report, metric) else {
                return unknown(id, metric, "workload is missing from the case report");
            };
            let evaluated = match model.evaluate(profile) {
                Ok(evaluated) => evaluated,
                Err(error) => return unknown(id, metric, &format!("cost model: {error:#}")),
            };
            let estimated_max = evaluated.estimate_us.max.unwrap_or_default();
            if !allowed_multiplier.is_finite()
                || allowed_multiplier < 1.0
                || !fixed_jitter_us.is_finite()
                || fixed_jitter_us < 0.0
            {
                return unknown(id, metric, "expectation has invalid tolerance");
            }
            let allowed_max = estimated_max * allowed_multiplier + fixed_jitter_us;
            let status = upper_bound_status(observed, estimated_max, allowed_max);
            AssessmentCheck {
                id: id.to_owned(),
                workload: metric.workload.to_owned(),
                metric: metric.metric.name().to_owned(),
                unit: metric.metric.unit().to_owned(),
                status,
                observed: Some(observed),
                estimated: Some(evaluated.estimate_us),
                allowed: Some(ValueRange::at_most(allowed_max)),
                relation_to_estimated: (estimated_max > 0.0).then_some(observed / estimated_max),
                reason: format!(
                    "observed {observed:.3} us, model upper {estimated_max:.3} us, allowed {allowed_max:.3} us"
                ),
                model: Some(evaluated),
            }
        }
        Expectation::RatioUpperBound { id, numerator, denominator, estimated_max, allowed_max } => {
            let Some(numerator_value) = metric_value(report, numerator) else {
                return unknown(
                    id,
                    numerator,
                    "numerator workload is missing from the case report",
                );
            };
            let Some(denominator_value) = metric_value(report, denominator) else {
                return unknown(
                    id,
                    denominator,
                    "denominator workload is missing from the case report",
                );
            };
            if denominator_value <= f64::EPSILON {
                return unknown(id, denominator, "denominator is zero");
            }
            let observed = numerator_value / denominator_value;
            let status = upper_bound_status(observed, estimated_max, allowed_max);
            AssessmentCheck {
                id: id.to_owned(),
                workload: numerator.workload.to_owned(),
                metric: format!("{}/{}", numerator.metric.name(), denominator.metric.name()),
                unit: "x".to_owned(),
                status,
                observed: Some(observed),
                estimated: Some(ValueRange::at_most(estimated_max)),
                allowed: Some(ValueRange::at_most(allowed_max)),
                relation_to_estimated: (estimated_max > 0.0).then_some(observed / estimated_max),
                reason: format!(
                    "{} {:.3} / {} {:.3} = {observed:.3}x",
                    numerator.metric.name(),
                    numerator_value,
                    denominator.metric.name(),
                    denominator_value
                ),
                model: None,
            }
        }
        Expectation::RatioLowerBound { id, numerator, denominator, estimated_min, allowed_min } => {
            let Some(numerator_value) = metric_value(report, numerator) else {
                return unknown(
                    id,
                    numerator,
                    "numerator workload is missing from the case report",
                );
            };
            let Some(denominator_value) = metric_value(report, denominator) else {
                return unknown(
                    id,
                    denominator,
                    "denominator workload is missing from the case report",
                );
            };
            if denominator_value <= f64::EPSILON {
                return unknown(id, denominator, "denominator is zero");
            }
            let observed = numerator_value / denominator_value;
            let status = lower_bound_status(observed, estimated_min, allowed_min);
            AssessmentCheck {
                id: id.to_owned(),
                workload: numerator.workload.to_owned(),
                metric: format!("{}/{}", numerator.metric.name(), denominator.metric.name()),
                unit: "x".to_owned(),
                status,
                observed: Some(observed),
                estimated: Some(ValueRange { min: Some(estimated_min), max: None }),
                allowed: Some(ValueRange { min: Some(allowed_min), max: None }),
                relation_to_estimated: (estimated_min > 0.0).then_some(observed / estimated_min),
                reason: format!(
                    "{} {:.3} / {} {:.3} = {observed:.3}x",
                    numerator.metric.name(),
                    numerator_value,
                    denominator.metric.name(),
                    denominator_value
                ),
                model: None,
            }
        }
        Expectation::AtMost { id, metric, estimated_max, allowed_max } => {
            let Some(observed) = metric_value(report, metric) else {
                return unknown(id, metric, "workload is missing from the case report");
            };
            let status = upper_bound_status(observed, estimated_max, allowed_max);
            AssessmentCheck {
                id: id.to_owned(),
                workload: metric.workload.to_owned(),
                metric: metric.metric.name().to_owned(),
                unit: metric.metric.unit().to_owned(),
                status,
                observed: Some(observed),
                estimated: Some(ValueRange::at_most(estimated_max)),
                allowed: Some(ValueRange::at_most(allowed_max)),
                relation_to_estimated: None,
                reason: format!("observed {observed:.3}, allowed at most {allowed_max:.3}"),
                model: None,
            }
        }
        Expectation::AtLeast { id, metric, estimated_min, allowed_min } => {
            let Some(observed) = metric_value(report, metric) else {
                return unknown(id, metric, "workload is missing from the case report");
            };
            let status = lower_bound_status(observed, estimated_min, allowed_min);
            AssessmentCheck {
                id: id.to_owned(),
                workload: metric.workload.to_owned(),
                metric: metric.metric.name().to_owned(),
                unit: metric.metric.unit().to_owned(),
                status,
                observed: Some(observed),
                estimated: Some(ValueRange { min: Some(estimated_min), max: None }),
                allowed: Some(ValueRange { min: Some(allowed_min), max: None }),
                relation_to_estimated: (estimated_min > 0.0).then_some(observed / estimated_min),
                reason: format!("observed {observed:.3}, allowed at least {allowed_min:.3}"),
                model: None,
            }
        }
        Expectation::DerivedAtMost { id, metric, unit, estimated_max, allowed_max } => {
            evaluate_derived_bound(report, id, metric, unit, estimated_max, allowed_max, true)
        }
        Expectation::DerivedAtLeast { id, metric, unit, estimated_min, allowed_min } => {
            evaluate_derived_bound(report, id, metric, unit, estimated_min, allowed_min, false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn evaluate_derived_bound(
    report: &CaseReport,
    id: &str,
    metric: &str,
    unit: &str,
    estimated: f64,
    allowed: f64,
    upper: bool,
) -> AssessmentCheck {
    let Some(observed) = report.derived.get(metric).copied() else {
        return AssessmentCheck {
            id: id.to_owned(),
            workload: "-".to_owned(),
            metric: metric.to_owned(),
            unit: unit.to_owned(),
            status: AssessmentStatus::Unknown,
            observed: None,
            estimated: None,
            allowed: None,
            relation_to_estimated: None,
            reason: "derived metric is missing from the case report".to_owned(),
            model: None,
        };
    };
    let status = if upper {
        upper_bound_status(observed, estimated, allowed)
    } else {
        lower_bound_status(observed, estimated, allowed)
    };
    let estimated_range = if upper {
        ValueRange::at_most(estimated)
    } else {
        ValueRange { min: Some(estimated), max: None }
    };
    let allowed_range = if upper {
        ValueRange::at_most(allowed)
    } else {
        ValueRange { min: Some(allowed), max: None }
    };
    AssessmentCheck {
        id: id.to_owned(),
        workload: "-".to_owned(),
        metric: metric.to_owned(),
        unit: unit.to_owned(),
        status,
        observed: Some(observed),
        estimated: Some(estimated_range),
        allowed: Some(allowed_range),
        relation_to_estimated: (estimated.abs() > f64::EPSILON).then_some(observed / estimated),
        reason: format!("observed {observed:.3} {unit}"),
        model: None,
    }
}

fn metric_value(report: &CaseReport, metric: MetricRef) -> Option<f64> {
    report
        .workloads
        .iter()
        .find(|workload| workload.name == metric.workload)
        .map(|workload| metric.metric.value(workload))
}

fn upper_bound_status(observed: f64, estimated_max: f64, allowed_max: f64) -> AssessmentStatus {
    if !observed.is_finite()
        || !estimated_max.is_finite()
        || !allowed_max.is_finite()
        || estimated_max < 0.0
        || allowed_max < estimated_max
    {
        AssessmentStatus::Unknown
    } else if observed <= estimated_max {
        AssessmentStatus::Good
    } else if observed <= allowed_max {
        AssessmentStatus::Acceptable
    } else {
        AssessmentStatus::NeedsWork
    }
}

fn lower_bound_status(observed: f64, estimated_min: f64, allowed_min: f64) -> AssessmentStatus {
    if !observed.is_finite()
        || !estimated_min.is_finite()
        || !allowed_min.is_finite()
        || allowed_min > estimated_min
    {
        AssessmentStatus::Unknown
    } else if observed >= estimated_min {
        AssessmentStatus::Good
    } else if observed >= allowed_min {
        AssessmentStatus::Acceptable
    } else {
        AssessmentStatus::NeedsWork
    }
}

fn unknown(id: &str, metric: MetricRef, reason: &str) -> AssessmentCheck {
    AssessmentCheck {
        id: id.to_owned(),
        workload: metric.workload.to_owned(),
        metric: metric.metric.name().to_owned(),
        unit: metric.metric.unit().to_owned(),
        status: AssessmentStatus::Unknown,
        observed: None,
        estimated: None,
        allowed: None,
        relation_to_estimated: None,
        reason: reason.to_owned(),
        model: None,
    }
}

pub(super) fn print_case(assessment: &CaseAssessment, case: &str) {
    println!(
        "perf-lab assessment: case={case} standard={} version={} status={}",
        assessment.standard,
        assessment.standard_version,
        assessment.status.label()
    );
    if let Some(note) = &assessment.note {
        println!("  {note}");
    }
    let display_checks =
        assessment.checks.iter().filter(|check| should_display_check(check)).collect::<Vec<_>>();
    let hidden = assessment.checks.len().saturating_sub(display_checks.len());
    if hidden > 0 {
        println!("  hidden passing checks: {hidden}");
    }
    if display_checks.is_empty() {
        return;
    }
    let headers =
        ["CHECK", "WORKLOAD", "METRIC", "OBSERVED", "ESTIMATED", "ALLOWED", "STATUS", "DETAIL"];
    let rows = display_checks
        .iter()
        .map(|check| {
            [
                check.id.clone(),
                check.workload.clone(),
                check.metric.clone(),
                format_optional(check.observed, &check.unit),
                format_range(check.estimated, &check.unit),
                format_range(check.allowed, &check.unit),
                check.status.label().to_owned(),
                check.reason.clone(),
            ]
        })
        .collect::<Vec<_>>();
    let widths = std::array::from_fn::<_, 8, _>(|column| {
        rows.iter()
            .map(|row| row[column].len())
            .chain(std::iter::once(headers[column].len()))
            .max()
            .unwrap_or_default()
    });
    print_assessment_row(&headers, &widths);
    for row in &rows {
        print_assessment_row(row, &widths);
    }
    for check in display_checks {
        if let Some(model) = &check.model {
            println!("  model {}: {}", check.id, model.formula);
        }
    }
}

fn should_display_check(check: &AssessmentCheck) -> bool {
    !(check.id == "unexpected-failures" && check.status == AssessmentStatus::Good)
}

fn print_assessment_row<T: AsRef<str>>(row: &[T; 8], widths: &[usize; 8]) {
    println!(
        "{:<w0$}    {:<w1$}    {:<w2$}    {:>w3$}    {:>w4$}    {:>w5$}    {:<w6$}    {:<w7$}",
        row[0].as_ref(),
        row[1].as_ref(),
        row[2].as_ref(),
        row[3].as_ref(),
        row[4].as_ref(),
        row[5].as_ref(),
        row[6].as_ref(),
        row[7].as_ref(),
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
        w4 = widths[4],
        w5 = widths[5],
        w6 = widths[6],
        w7 = widths[7],
    );
}

pub(super) fn print_suite(reports: &[CaseReport]) {
    let assessments = reports.iter().map(|report| &report.assessment).collect::<Vec<_>>();
    if assessments.is_empty() {
        return;
    }
    let count = |status| assessments.iter().filter(|item| item.status == status).count();
    println!();
    println!("perf-lab assessment summary");
    println!("  GOOD         {}", count(AssessmentStatus::Good));
    println!("  ACCEPTABLE   {}", count(AssessmentStatus::Acceptable));
    println!("  NEEDS_WORK   {}", count(AssessmentStatus::NeedsWork));
    println!("  UNKNOWN      {}", count(AssessmentStatus::Unknown));
    println!();
    for report in reports {
        print_case(&report.assessment, &report.case);
    }
}

fn format_optional(value: Option<f64>, unit: &str) -> String {
    value.map(|value| format_number(value, unit)).unwrap_or_else(|| "-".to_owned())
}

fn format_range(range: Option<ValueRange>, unit: &str) -> String {
    let Some(range) = range else { return "-".to_owned() };
    match (range.min, range.max) {
        (Some(min), Some(max)) if (min - max).abs() <= f64::EPSILON => format_number(min, unit),
        (Some(min), Some(max)) => format!("{min:.3}-{max:.3} {unit}"),
        (Some(min), None) => format!(">={}", format_number(min, unit)),
        (None, Some(max)) => format!("<={}", format_number(max, unit)),
        (None, None) => "-".to_owned(),
    }
}

fn format_number(value: f64, unit: &str) -> String {
    if value.abs() >= 1000.0 { format!("{value:.0} {unit}") } else { format!("{value:.3} {unit}") }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::perf_lab::RunProfile;
    use crate::perf_lab::cases::CaseCategory;
    use crate::perf_lab::config::LabConfig;

    fn machine_profile() -> MachineProfile {
        let result = |category: &str, benchmark: &str, parameter: &str, p50_us: f64| {
            serde_json::json!({
                "category": category,
                "benchmark": benchmark,
                "parameter": parameter,
                "status": "ok",
                "summary": {
                    "samples": 10,
                    "operations": 10,
                    "elapsed_secs": 1.0,
                    "avg_us": p50_us,
                    "p50_us": p50_us,
                    "p99_us": p50_us,
                    "p999_us": p50_us,
                    "max_us": p50_us,
                    "ops_per_sec": 10.0,
                    "mib_per_sec": 0.0,
                    "buckets": []
                }
            })
        };
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "tool_version": "test",
            "duration_secs": 1,
            "warmup_secs": 0,
            "environment": {
                "hostname": "test", "kernel": "test", "cpu_model": "test",
                "logical_cpus": 8, "cpu_governor": "performance",
                "load_average": "0 0 0", "cgroup": "test", "disk_dir": "/tmp",
                "disk_device": 1, "filesystem_type": "test", "filesystem_block_size": 4096
            },
            "results": [
                result("network", "tcp_loopback", "64 B", 20.0),
                result("network", "tcp_loopback", "4 KiB", 22.0),
                result("scheduler", "tokio_task_ping_pong", "2 tasks", 1.0),
                result("storage", "page_cache_read", "4 KiB random", 1.0),
                result("storage", "odirect_write", "4 KiB", 15.0)
            ]
        }))
        .unwrap()
    }

    fn case_report(p50_us: u64, p999_us: u64) -> CaseReport {
        let mut workload = WorkloadReport::default();
        workload.name = "latency_c1".to_owned();
        workload.operations = 10;
        workload.successes = 10;
        workload.qps = 0.8;
        workload.latency.count = 10;
        workload.latency.p50_us = p50_us;
        workload.latency.p999_us = p999_us;
        workload.success_latency = workload.latency.clone();
        let mut config = LabConfig::default();
        config.profile = RunProfile::Regression;
        CaseReport {
            case: "test".to_owned(),
            scenario_version: 1,
            category: CaseCategory::Sentinel,
            description: "test".to_owned(),
            run_id: "test".to_owned(),
            config,
            semantic_parameters: BTreeMap::new(),
            regression_checks: Vec::new(),
            workloads: vec![workload],
            derived: BTreeMap::new(),
            metric_intervals: Vec::new(),
            assessment: CaseAssessment::unavailable("test"),
        }
    }

    #[test]
    fn modeled_latency_distinguishes_good_acceptable_and_needs_work() {
        let expectations = |allowed_multiplier| CaseExpectations {
            standard_version: 1,
            checks: vec![Expectation::ModeledLatency {
                id: "latency",
                metric: MetricRef::new("latency_c1", Metric::P50Us),
                model: sequential(vec![
                    primitive(Primitive::TcpLoopback64B, 1.0),
                    budget(10.0, 20.0, "software"),
                ]),
                allowed_multiplier,
                fixed_jitter_us: 0.0,
            }],
        };
        let profile = machine_profile();
        assert_eq!(
            assess(&case_report(35, 35), expectations(2.0), &profile).status,
            AssessmentStatus::Good
        );
        assert_eq!(
            assess(&case_report(60, 60), expectations(2.0), &profile).status,
            AssessmentStatus::Acceptable
        );
        assert_eq!(
            assess(&case_report(100, 100), expectations(2.0), &profile).status,
            AssessmentStatus::NeedsWork
        );
    }

    #[test]
    fn ratio_and_missing_primitive_are_reported() {
        let mut report = case_report(10, 200);
        let ratio = CaseExpectations {
            standard_version: 1,
            checks: vec![Expectation::RatioUpperBound {
                id: "tail",
                numerator: MetricRef::new("latency_c1", Metric::P999Us),
                denominator: MetricRef::new("latency_c1", Metric::P50Us),
                estimated_max: 5.0,
                allowed_max: 10.0,
            }],
        };
        assert_eq!(assess(&report, ratio, &machine_profile()).status, AssessmentStatus::NeedsWork);

        report.workloads[0].latency.p50_us = 50;
        let missing = CaseExpectations {
            standard_version: 1,
            checks: vec![Expectation::ModeledLatency {
                id: "missing",
                metric: MetricRef::new("latency_c1", Metric::P50Us),
                model: primitive(Primitive::TcpLoopback4KiB, 1.0),
                allowed_multiplier: 2.0,
                fixed_jitter_us: 0.0,
            }],
        };
        let mut profile = machine_profile();
        profile.results.retain(|result| result.parameter != "4 KiB");
        assert_eq!(assess(&report, missing, &profile).status, AssessmentStatus::Unknown);
    }

    #[test]
    fn generic_and_derived_expectations_cover_unmodeled_cases() {
        let mut report = case_report(10, 30);
        report.derived.insert("ratio".to_owned(), 0.8);
        let generic = assess(
            &report,
            CaseExpectations { standard_version: 1, checks: Vec::new() },
            &machine_profile(),
        );
        assert_eq!(generic.status, AssessmentStatus::Good);
        assert_eq!(generic.checks.len(), 1);
        assert!(generic.note.is_some());

        let specialized = assess(
            &report,
            CaseExpectations {
                standard_version: 1,
                checks: vec![
                    Expectation::AtLeast {
                        id: "qps-floor",
                        metric: MetricRef::new("latency_c1", Metric::Qps),
                        estimated_min: 1.0,
                        allowed_min: 0.5,
                    },
                    Expectation::DerivedAtLeast {
                        id: "derived-floor",
                        metric: "ratio",
                        unit: "x",
                        estimated_min: 0.9,
                        allowed_min: 0.7,
                    },
                ],
            },
            &machine_profile(),
        );
        assert_eq!(specialized.status, AssessmentStatus::Acceptable);
        assert!(specialized.checks.iter().any(|check| check.id == "derived-floor"));
    }
}
