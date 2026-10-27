// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::report::{
    CaseReport, case_report, failure_check, latency_check, throughput_check,
};
use crate::perf_lab::workload::{LoadModel, Operation};
use crate::perf_lab::{LabContext, RunProfile};

// Range-scan case parameters.
const SCAN_LIMITS: &[u64] = &[16, 128, 1024];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BasicCase {
    PointHit,
    PointMiss,
    Insert,
    Update,
    Delete,
    RangeScan,
    CrossShardScan,
}

impl BasicCase {
    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let read_fixture = fixture_spec(
            lab,
            FixtureSpec { keys: 512, value_size: 1024 },
            FixtureSpec { keys: 32_768, value_size: 8192 },
            FixtureSpec { keys: 131_072, value_size: 8192 },
            FixtureSpec { keys: 262_144, value_size: 8192 },
        );
        let write_fixture = fixture_spec(
            lab,
            FixtureSpec { keys: 512, value_size: 1024 },
            FixtureSpec { keys: 4096, value_size: 4096 },
            FixtureSpec { keys: 32_768, value_size: 8192 },
            FixtureSpec { keys: 131_072, value_size: 8192 },
        );
        let fixture_spec =
            if matches!(self, Self::PointHit | Self::RangeScan | Self::CrossShardScan) {
                read_fixture
            } else {
                write_fixture
            };
        let (mut db, mut target) =
            fixture(lab, name, self != Self::PointMiss && self != Self::Insert, fixture_spec)
                .await?;
        if matches!(self, Self::PointHit | Self::RangeScan | Self::CrossShardScan) {
            validate_read_fixture(lab, &target, fixture_spec)?;
        }
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        derived.insert("fixture.live_keys".into(), target.keys as f64);
        derived.insert(
            "fixture.payload_bytes".into(),
            (target.keys as f64) * fixture_spec.value_size as f64,
        );
        if matches!(self, Self::RangeScan | Self::CrossShardScan) {
            if self == Self::CrossShardScan {
                ensure_groups(lab, 2).await?;
                ensure!(target.keys >= 2, "cross-shard-scan needs at least two keys");
                lab.split_shard_for_key(target.table, &target.key(target.keys / 2)).await?;
                lab.migrate_shard_to_new_group(target.table, &target.key(target.keys / 2)).await?;
                lab.wait_cluster_stable().await?;
                let left = lab.group_for_key(target.table, &target.key(0)).await?.0;
                let right = lab.group_for_key(target.table, &target.key(target.keys - 1)).await?.0;
                ensure!(left != right, "cross-shard scan requires two groups");
                let spec = workload(
                    lab,
                    "cross_shard_full_scan",
                    target.clone(),
                    Operation::Scan {
                        limit: target.keys,
                        version: None,
                        expected_rows: Some(target.keys),
                        expected: None,
                    },
                    fixture_spec.value_size,
                    LoadModel::Once,
                );
                reports.push(measure(lab, &db, spec).await?);
            } else {
                let mut isolation = MatrixIsolation::default();
                for &limit in SCAN_LIMITS {
                    if !isolation.start_item(lab).await? {
                        (db, target) = fixture(lab, name, true, fixture_spec).await?;
                        validate_read_fixture(lab, &target, fixture_spec)?;
                    }
                    let spec = workload(
                        lab,
                        &format!("scan_{limit}"),
                        target.clone(),
                        Operation::Scan {
                            limit,
                            version: None,
                            expected_rows: Some(limit.min(target.keys)),
                            expected: None,
                        },
                        fixture_spec.value_size,
                        LoadModel::Once,
                    );
                    reports.push(measure(lab, &db, spec).await?);
                }
            }
        } else {
            let operation = match self {
                Self::PointHit => Operation::Read { present: true, version: None, expected: None },
                Self::PointMiss => {
                    Operation::Read { present: false, version: None, expected: None }
                }
                Self::Insert => Operation::Insert,
                Self::Update => Operation::Put,
                Self::Delete => Operation::Delete,
                Self::RangeScan | Self::CrossShardScan => {
                    unreachable!("scans handled above")
                }
            };
            let concurrency = match lab.config.profile {
                RunProfile::Smoke => 2,
                RunProfile::Regression => 16,
                RunProfile::Characterization => 16,
                RunProfile::Stress => 64,
            };
            if self == Self::Delete {
                let report_target = target.clone();
                let spec = workload(
                    lab,
                    "delete_finite_pass",
                    target,
                    operation,
                    fixture_spec.value_size,
                    closed_loop(concurrency.min(fixture_spec.keys as usize)),
                );
                let report = measure(lab, &db, spec).await?;
                ensure!(
                    report.successes == fixture_spec.keys
                        && db.get(report_target.table, report_target.key(0)).await?.is_none(),
                    "deleted key remained visible"
                );
                reports.push(report);
            } else {
                let mut latency_target = target.clone();
                if self == Self::Insert {
                    latency_target.prefix.push_str("latency-");
                }
                let latency = workload(
                    lab,
                    "latency_c1",
                    latency_target,
                    operation.clone(),
                    fixture_spec.value_size,
                    closed_loop(1),
                );
                reports.push(measure(lab, &db, latency).await?);
                lab.restart_isolated().await?;
                (db, target) = fixture(
                    lab,
                    name,
                    self != Self::PointMiss && self != Self::Insert,
                    fixture_spec,
                )
                .await?;
                if self == Self::PointHit {
                    validate_read_fixture(lab, &target, fixture_spec)?;
                }
                if self == Self::Insert {
                    target.prefix.push_str("throughput-");
                }
                let throughput = workload(
                    lab,
                    &format!("throughput_c{concurrency}"),
                    target,
                    operation,
                    fixture_spec.value_size,
                    closed_loop(concurrency),
                );
                reports.push(measure(lab, &db, throughput).await?);
            }
        }
        let mut checks = Vec::new();
        for report in &reports {
            checks.push(failure_check(format!("{}.unexpected_failure_rate", report.name)));
            if report.name.starts_with("latency_") || report.name.starts_with("scan_") {
                checks.push(latency_check(format!("{}.p99_us", report.name)));
            }
            if report.name.starts_with("throughput_")
                || report.name == "delete_finite_pass"
                || report.name == "cross_shard_full_scan"
            {
                checks.push(throughput_check(format!("{}.qps", report.name)));
            }
            if report.name == "cross_shard_full_scan" {
                checks.push(latency_check(format!("{}.p99_us", report.name)));
            }
        }
        Ok(case_report(lab, name, reports, derived, checks))
    }
}

fn validate_read_fixture(
    lab: &LabContext,
    target: &crate::perf_lab::workload::Target,
    fixture_spec: FixtureSpec,
) -> Result<()> {
    let db_config = &lab.config.cluster.db;
    let bytes = target.keys as u128 * fixture_spec.value_size as u128;
    let memory_budget = db_config.block_cache_size as u128 * 4
        + db_config.write_buffer_size as u128 * db_config.max_write_buffer_number as u128 * 2;
    ensure!(
        bytes > memory_budget,
        "read case requires a working set larger than cache and write buffers"
    );
    ensure!(
        lab.storage_stats()?.sst_bytes > db_config.block_cache_size as u64,
        "read fixture did not flush beyond block cache"
    );
    Ok(())
}
