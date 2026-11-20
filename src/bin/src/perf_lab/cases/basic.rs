// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::assessment::{
    CaseExpectations, Expectation, Metric, MetricRef, budget, primitive, sequential,
};
use crate::perf_lab::machine::Primitive;
use crate::perf_lab::report::{
    CaseReport, case_report, failure_check, latency_check, throughput_check,
};
use crate::perf_lab::workload::{LoadModel, Operation};
use crate::perf_lab::{LabContext, RunProfile};

// Range-scan case parameters.
const SCAN_LIMITS: &[u64] = &[16, 128, 1024];
const READ_WORKING_SET_CACHE_MULTIPLIER: u128 = 4;
const DISK_SIZED_READ_BLOCK_CACHE_SIZE: usize = 32 << 20;

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
    pub(super) fn configure(self, cfg: &mut crate::perf_lab::config::LabConfig) {
        if cfg.profile != RunProfile::Smoke
            && matches!(self, Self::PointHit | Self::RangeScan | Self::CrossShardScan)
        {
            cfg.cluster.db.block_cache_size = DISK_SIZED_READ_BLOCK_CACHE_SIZE;
        }
    }

    pub(super) fn expectations(self, profile: RunProfile) -> CaseExpectations {
        if profile != RunProfile::Regression {
            return CaseExpectations { standard_version: 1, checks: Vec::new() };
        }
        let checks = match self {
            Self::PointHit => point_hit_expectations(),
            Self::PointMiss => point_miss_expectations(),
            Self::Insert => insert_expectations(),
            Self::Update => update_expectations(),
            Self::RangeScan => range_scan_expectations(),
            Self::CrossShardScan => vec![Expectation::AtLeast {
                id: "cross-shard-scan-rows",
                metric: MetricRef::new("cross_shard_full_scan", Metric::RowsPerSecond),
                estimated_min: 40_000.0,
                allowed_min: 20_000.0,
            }],
            Self::Delete => Vec::new(),
        };
        CaseExpectations { standard_version: 1, checks }
    }

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

fn point_hit_expectations() -> Vec<Expectation> {
    vec![Expectation::ModeledLatency {
        id: "point-hit-c1-p50",
        metric: MetricRef::new("latency_c1", Metric::P50Us),
        model: sequential(vec![
            primitive(Primitive::TcpLoopback64B, 1.0),
            primitive(Primitive::TcpLoopback4KiB, 1.0),
            primitive(Primitive::TokioTaskPingPong, 4.0),
            primitive(Primitive::PageCacheRead4KiB, 1.0),
            budget(15.0, 35.0, "rpc codec, routing and rocksdb lookup"),
        ]),
        allowed_multiplier: 3.0,
        fixed_jitter_us: 10.0,
    }]
}

fn point_miss_expectations() -> Vec<Expectation> {
    vec![Expectation::ModeledLatency {
        id: "point-miss-c1-p50",
        metric: MetricRef::new("latency_c1", Metric::P50Us),
        model: sequential(vec![
            primitive(Primitive::TcpLoopback64B, 2.0),
            primitive(Primitive::TokioTaskPingPong, 4.0),
            primitive(Primitive::PageCacheRead4KiB, 1.0),
            budget(12.0, 28.0, "rpc codec, routing and negative rocksdb lookup"),
        ]),
        allowed_multiplier: 3.0,
        fixed_jitter_us: 10.0,
    }]
}

fn insert_expectations() -> Vec<Expectation> {
    vec![Expectation::ModeledLatency {
        id: "insert-c1-p50",
        metric: MetricRef::new("latency_c1", Metric::P50Us),
        model: sequential(vec![
            primitive(Primitive::TcpLoopback64B, 1.0),
            primitive(Primitive::TcpLoopback4KiB, 2.0),
            primitive(Primitive::TokioTaskPingPong, 6.0),
            // This is a stable storage-stack reference, not a claim that
            // each Raft log append uses O_DIRECT or waits for durability.
            primitive(Primitive::ODirectWrite4KiB, 1.0),
            budget(15.0, 35.0, "root allocation, raft bookkeeping and wakeups"),
        ]),
        allowed_multiplier: 2.5,
        fixed_jitter_us: 0.0,
    }]
}

fn update_expectations() -> Vec<Expectation> {
    vec![Expectation::ModeledLatency {
        id: "update-c1-p50",
        metric: MetricRef::new("latency_c1", Metric::P50Us),
        model: sequential(vec![
            primitive(Primitive::TcpLoopback64B, 1.0),
            primitive(Primitive::TcpLoopback4KiB, 2.0),
            primitive(Primitive::TokioTaskPingPong, 6.0),
            primitive(Primitive::ODirectWrite4KiB, 1.0),
            budget(20.0, 45.0, "root allocation, existing-key evaluation and raft wakeups"),
        ]),
        allowed_multiplier: 2.5,
        fixed_jitter_us: 0.0,
    }]
}

fn range_scan_expectations() -> Vec<Expectation> {
    vec![
        Expectation::RatioUpperBound {
            id: "range-scan-128-vs-16",
            numerator: MetricRef::new("scan_128", Metric::P50Us),
            denominator: MetricRef::new("scan_16", Metric::P50Us),
            estimated_max: 8.0,
            allowed_max: 12.0,
        },
        Expectation::RatioUpperBound {
            id: "range-scan-1024-vs-128",
            numerator: MetricRef::new("scan_1024", Metric::P50Us),
            denominator: MetricRef::new("scan_128", Metric::P50Us),
            estimated_max: 8.0,
            allowed_max: 12.0,
        },
        Expectation::AtLeast {
            id: "range-scan-1024-bandwidth",
            metric: MetricRef::new("scan_1024", Metric::BytesPerSecond),
            estimated_min: 200.0 * 1024.0 * 1024.0,
            allowed_min: 100.0 * 1024.0 * 1024.0,
        },
    ]
}

fn validate_read_fixture(
    lab: &LabContext,
    target: &crate::perf_lab::workload::Target,
    fixture_spec: FixtureSpec,
) -> Result<()> {
    let db_config = &lab.config.cluster.db;
    let bytes = target.keys as u128 * fixture_spec.value_size as u128;
    ensure!(
        bytes > db_config.block_cache_size as u128 * READ_WORKING_SET_CACHE_MULTIPLIER,
        "read case requires a working set larger than block cache"
    );
    ensure!(
        lab.storage_stats()?.sst_bytes > db_config.block_cache_size as u64,
        "read fixture did not flush beyond block cache"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_sized_read_cases_override_only_block_cache() {
        for case in [BasicCase::PointHit, BasicCase::RangeScan, BasicCase::CrossShardScan] {
            let production = sekas_server::DbConfig::default();
            let mut cfg = crate::perf_lab::config::LabConfig::default();
            case.configure(&mut cfg);
            assert_eq!(cfg.cluster.db.block_cache_size, DISK_SIZED_READ_BLOCK_CACHE_SIZE);
            assert_eq!(cfg.cluster.db.write_buffer_size, production.write_buffer_size);
            assert_eq!(cfg.cluster.db.max_write_buffer_number, production.max_write_buffer_number);
        }

        let mut smoke: crate::perf_lab::config::LabConfig =
            toml::from_str(include_str!("../../../../../conf/perf-lab-smoke.toml")).unwrap();
        let smoke_cache = smoke.cluster.db.block_cache_size;
        BasicCase::PointHit.configure(&mut smoke);
        assert_eq!(smoke.cluster.db.block_cache_size, smoke_cache);
    }
}
