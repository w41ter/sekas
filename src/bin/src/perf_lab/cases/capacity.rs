// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};

use super::support::*;
use crate::perf_lab::assessment::{CaseExpectations, Expectation, Metric, MetricRef};
use crate::perf_lab::config::LabConfig;
use crate::perf_lab::report::{
    CaseReport, case_report, latency_check, throughput_check, workload_checks,
};
use crate::perf_lab::workload::{Operation, TxnMode};
use crate::perf_lab::{LabContext, RunProfile};

// Capacity case parameters; scale cases do not borrow the concurrency matrix.
const CONCURRENCY_LEVELS: &[usize] = &[1, 4, 16, 64];
const OFFERED_QPS: &[f64] = &[100.0, 1000.0, 5000.0];
const GROUP_SCALE_GROUP_COUNTS: &[u64] = &[1, 2, 4];
const GROUP_SCALE_CONCURRENCY: usize = 64;
const NODE_SCALE_GROUP_COUNT: usize = 4;
const NODE_SCALE_CONCURRENCY: usize = 64;
const METADATA_COUNTS: &[u64] = &[1, 10, 100];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CapacityCase {
    Concurrency,
    OfferedLoad,
    GroupScale,
    NodeScale,
    MetadataScale,
}

impl CapacityCase {
    pub(super) fn expectations(self, profile: RunProfile) -> CaseExpectations {
        if profile != RunProfile::Regression {
            return CaseExpectations { standard_version: 1, checks: Vec::new() };
        }
        let checks = match self {
            Self::Concurrency => concurrency_expectations(),
            Self::OfferedLoad => offered_load_expectations(),
            Self::GroupScale => vec![Expectation::RatioLowerBound {
                id: "group-scale-g4-vs-g1-qps",
                numerator: MetricRef::new("groups_4", Metric::Qps),
                denominator: MetricRef::new("groups_1", Metric::Qps),
                estimated_min: 2.0,
                allowed_min: 1.0,
            }],
            Self::NodeScale => vec![
                Expectation::DerivedAtLeast {
                    id: "node-scale-throughput",
                    metric: "scale_out.throughput_ratio",
                    unit: "x",
                    estimated_min: 1.2,
                    allowed_min: 0.9,
                },
                Expectation::DerivedAtMost {
                    id: "node-scale-duration",
                    metric: "scale_out.event_duration_ms",
                    unit: "ms",
                    estimated_max: 10_000.0,
                    allowed_max: 60_000.0,
                },
            ],
            Self::MetadataScale => vec![
                ratio_upper(
                    "metadata-100-vs-1-p99",
                    "metadata_100",
                    "metadata_1",
                    Metric::P99Us,
                    3.0,
                    10.0,
                ),
                ratio_upper(
                    "data-read-100-vs-1-p99",
                    "data_read_100",
                    "data_read_1",
                    Metric::P99Us,
                    1.5,
                    3.0,
                ),
                Expectation::DerivedAtMost {
                    id: "metadata-100-create",
                    metric: "metadata_100.create_duration_ms",
                    unit: "ms",
                    estimated_max: 2_000.0,
                    allowed_max: 10_000.0,
                },
            ],
        };
        CaseExpectations { standard_version: 1, checks }
    }

    pub(super) fn configure(self, cfg: &mut LabConfig) {
        if self == Self::NodeScale {
            cfg.cluster.node.replica.testing_knobs.disable_scheduler_durable_task = true;
        }
    }

    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        let fixture_spec = fixture_spec(
            lab,
            FixtureSpec { keys: 512, value_size: 1024 },
            FixtureSpec { keys: 8192, value_size: 4096 },
            FixtureSpec { keys: 32_768, value_size: 8192 },
            FixtureSpec { keys: 131_072, value_size: 8192 },
        );
        match self {
            Self::Concurrency | Self::OfferedLoad => {
                let mut isolation = MatrixIsolation::default();
                for (name, operation) in [
                    ("read", Operation::Read { present: true, version: None, expected: None }),
                    ("update", Operation::Put),
                    ("distributed_txn", Operation::Transaction { mode: TxnMode::Blind, keys: 2 }),
                ] {
                    if self == Self::Concurrency {
                        for &concurrency in CONCURRENCY_LEVELS {
                            isolation.start_item(lab).await?;
                            let item = format!("{name}_concurrency_{concurrency}");
                            let (db, targets) = layout(lab, &item, 2, fixture_spec).await?;
                            let mut spec = workload(
                                lab,
                                &item,
                                targets[0].clone(),
                                operation.clone(),
                                fixture_spec.value_size,
                                closed_loop(concurrency),
                            );
                            if name == "distributed_txn" {
                                spec.targets = targets;
                            }
                            spec.allow_overload = true;
                            let report = measure(lab, &db, spec).await?;
                            ensure!(
                                !report.errors.contains_key("validation")
                                    && !report.errors.contains_key("internal"),
                                "capacity probe encountered invalid data"
                            );
                            reports.push(report);
                        }
                    } else {
                        // Overload/deadline errors are expected in this capacity probe, but
                        // validation errors are not.

                        for &qps in OFFERED_QPS {
                            isolation.start_item(lab).await?;
                            let item = format!("{name}_offered_{qps}");
                            let (db, targets) = layout(lab, &item, 2, fixture_spec).await?;
                            let mut spec = workload(
                                lab,
                                &item,
                                targets[0].clone(),
                                operation.clone(),
                                fixture_spec.value_size,
                                open_loop(qps, 128),
                            );
                            if name == "distributed_txn" {
                                spec.targets = targets;
                            }
                            spec.allow_overload = true;
                            let report = measure(lab, &db, spec).await?;
                            ensure!(
                                !report.errors.contains_key("validation")
                                    && !report.errors.contains_key("internal"),
                                "capacity probe encountered invalid data"
                            );
                            derived.insert(format!("{}.offered_qps", report.name), qps);
                            reports.push(report);
                        }
                    }
                }
            }
            Self::GroupScale => {
                let mut isolation = MatrixIsolation::default();
                for &groups in GROUP_SCALE_GROUP_COUNTS {
                    isolation.start_item(lab).await?;
                    let (db, targets) =
                        layout(lab, &format!("scale_{groups}"), groups as usize, fixture_spec)
                            .await?;
                    let mut spec = workload(
                        lab,
                        &format!("groups_{groups}"),
                        targets[0].clone(),
                        Operation::Put,
                        fixture_spec.value_size,
                        closed_loop(GROUP_SCALE_CONCURRENCY),
                    );
                    spec.targets = targets;
                    reports.push(measure(lab, &db, spec).await?);
                }
            }
            Self::NodeScale => {
                let (db, targets) =
                    layout(lab, "node_scale", NODE_SCALE_GROUP_COUNT, fixture_spec).await?;
                let mut spec = workload(
                    lab,
                    "before_scale_out",
                    targets[0].clone(),
                    Operation::Put,
                    fixture_spec.value_size,
                    closed_loop(NODE_SCALE_CONCURRENCY),
                );
                spec.targets = targets.clone();
                let before = measure(lab, &db, spec.clone()).await?;
                let started = Instant::now();
                let node = lab.add_server().await?;
                // Explicit redistribution makes scale-out reproducible without a heuristic
                // scheduler.
                for target in targets.iter().step_by(2) {
                    let group = lab.group_for_key(target.table, &target.key(0)).await?.0;
                    let old_leader_node = lab.group_leader_node(group).await?;
                    lab.add_group_replica(group, node).await?;
                    let replica = lab
                        .router
                        .find_group(group)?
                        .replicas
                        .values()
                        .find(|r| r.node_id == node)
                        .unwrap()
                        .id;
                    lab.group(group).transfer_leader(replica).await?;
                    lab.wait_group_leader(group, replica).await?;
                    lab.remove_extra_replica(
                        group,
                        old_leader_node,
                        Duration::from_secs(lab.config.execution.event_timeout_secs),
                    )
                    .await?;
                }
                derived.insert(
                    "scale_out.event_duration_ms".into(),
                    started.elapsed().as_secs_f64() * 1000.0,
                );
                spec.name = "after_scale_out".into();
                let after = measure(lab, &db, spec).await?;
                derived.insert("scale_out.throughput_ratio".into(), after.qps / before.qps);
                reports.extend([before, after]);
            }
            Self::MetadataScale => {
                let mut isolation = MatrixIsolation::default();
                for &count in METADATA_COUNTS {
                    isolation.start_item(lab).await?;
                    let db = lab.client.create_database(format!("metadata_scale_{count}")).await?;
                    for i in 0..count {
                        lab.table(&db, &format!("table_{i}")).await?;
                    }
                    ensure!(
                        db.list_table().await?.len() == count as usize,
                        "metadata fixture count mismatch"
                    );
                    let table = db.open_table("table_0".into()).await?;
                    let target = crate::perf_lab::workload::Target {
                        table: table.id,
                        prefix: "key-".into(),
                        keys: fixture_spec.keys,
                    };
                    seed_target(lab, &db, &target, &vec![b's'; fixture_spec.value_size]).await?;
                    let spec = workload(
                        lab,
                        &format!("metadata_{count}"),
                        target.clone(),
                        Operation::Metadata,
                        fixture_spec.value_size,
                        closed_loop(16),
                    );
                    reports.push(measure(lab, &db, spec).await?);
                    let spec = workload(
                        lab,
                        &format!("data_read_{count}"),
                        target,
                        Operation::Read { present: true, version: None, expected: None },
                        fixture_spec.value_size,
                        closed_loop(16),
                    );
                    reports.push(measure(lab, &db, spec).await?);
                    let deadline = Instant::now()
                        + Duration::from_secs(lab.config.execution.event_timeout_secs);
                    let started = Instant::now();
                    lab.table(&db, "new_table").await?;
                    if Instant::now() > deadline {
                        bail!("create table exceeded event timeout");
                    }
                    derived.insert(
                        format!("metadata_{count}.create_duration_ms"),
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                    for table in db.list_table().await? {
                        db.delete_table(table.name).await?;
                    }
                }
            }
        }
        let mut checks = workload_checks(&reports, false, false);
        match self {
            Self::Concurrency | Self::OfferedLoad => {
                let peak = reports.iter().map(|report| report.qps).fold(0.0, f64::max);
                derived.insert("capacity.peak_qps".into(), peak);
                checks.push(throughput_check("capacity.peak_qps"));
            }
            Self::GroupScale => {
                for report in &reports {
                    checks.push(throughput_check(format!("{}.qps", report.name)));
                }
            }
            Self::NodeScale => {
                checks.push(throughput_check("scale_out.throughput_ratio"));
                checks.push(latency_check("scale_out.event_duration_ms"));
            }
            Self::MetadataScale => {
                for report in &reports {
                    checks.push(latency_check(format!("{}.p99_us", report.name)));
                }
                checks.extend(
                    derived
                        .keys()
                        .filter(|metric| metric.ends_with(".create_duration_ms"))
                        .map(|metric| latency_check(metric.clone())),
                );
            }
        }
        Ok(case_report(lab, name, reports, derived, checks))
    }
}

fn concurrency_expectations() -> Vec<Expectation> {
    vec![
        ratio_lower(
            "read-c64-vs-c16-qps",
            "read_concurrency_64",
            "read_concurrency_16",
            Metric::Qps,
            1.0,
            0.9,
        ),
        ratio_lower(
            "update-c64-vs-c16-qps",
            "update_concurrency_64",
            "update_concurrency_16",
            Metric::Qps,
            1.2,
            1.0,
        ),
        ratio_lower(
            "distributed-c64-vs-c16-qps",
            "distributed_txn_concurrency_64",
            "distributed_txn_concurrency_16",
            Metric::Qps,
            1.5,
            1.0,
        ),
        tail("read-c64-tail", "read_concurrency_64", 5.0, 10.0),
        tail("update-c64-tail", "update_concurrency_64", 10.0, 50.0),
        tail("distributed-c64-tail", "distributed_txn_concurrency_64", 10.0, 50.0),
    ]
}

fn offered_load_expectations() -> Vec<Expectation> {
    let mut checks = Vec::new();
    for (operation, offered) in [
        ("read", 100),
        ("read", 1000),
        ("read", 5000),
        ("update", 100),
        ("update", 1000),
        ("update", 5000),
        ("distributed_txn", 100),
        ("distributed_txn", 1000),
        ("distributed_txn", 5000),
    ] {
        let (id, workload) = match (operation, offered) {
            ("read", 100) => ("read-offered-100", "read_offered_100"),
            ("read", 1000) => ("read-offered-1000", "read_offered_1000"),
            ("read", 5000) => ("read-offered-5000", "read_offered_5000"),
            ("update", 100) => ("update-offered-100", "update_offered_100"),
            ("update", 1000) => ("update-offered-1000", "update_offered_1000"),
            ("update", 5000) => ("update-offered-5000", "update_offered_5000"),
            ("distributed_txn", 100) => ("distributed-offered-100", "distributed_txn_offered_100"),
            ("distributed_txn", 1000) => {
                ("distributed-offered-1000", "distributed_txn_offered_1000")
            }
            ("distributed_txn", 5000) => {
                ("distributed-offered-5000", "distributed_txn_offered_5000")
            }
            _ => unreachable!(),
        };
        checks.push(Expectation::AtLeast {
            id,
            metric: MetricRef::new(workload, Metric::Qps),
            estimated_min: offered as f64 * 0.9,
            allowed_min: offered as f64 * 0.7,
        });
    }
    checks
}

fn ratio_lower(
    id: &'static str,
    numerator: &'static str,
    denominator: &'static str,
    metric: Metric,
    estimated_min: f64,
    allowed_min: f64,
) -> Expectation {
    Expectation::RatioLowerBound {
        id,
        numerator: MetricRef::new(numerator, metric),
        denominator: MetricRef::new(denominator, metric),
        estimated_min,
        allowed_min,
    }
}

fn ratio_upper(
    id: &'static str,
    numerator: &'static str,
    denominator: &'static str,
    metric: Metric,
    estimated_max: f64,
    allowed_max: f64,
) -> Expectation {
    Expectation::RatioUpperBound {
        id,
        numerator: MetricRef::new(numerator, metric),
        denominator: MetricRef::new(denominator, metric),
        estimated_max,
        allowed_max,
    }
}

fn tail(
    id: &'static str,
    workload: &'static str,
    estimated_max: f64,
    allowed_max: f64,
) -> Expectation {
    Expectation::RatioUpperBound {
        id,
        numerator: MetricRef::new(workload, Metric::P999Us),
        denominator: MetricRef::new(workload, Metric::P50Us),
        estimated_max,
        allowed_max,
    }
}
