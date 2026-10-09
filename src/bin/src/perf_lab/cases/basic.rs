// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::Operation;

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
        let (db, target) =
            fixture(lab, name, self != Self::PointMiss && self != Self::Insert).await?;
        if lab.config.workload.read_working_set == crate::perf_lab::config::ReadWorkingSet::Disk
            && matches!(self, Self::PointHit | Self::RangeScan | Self::CrossShardScan)
        {
            let db_config = &lab.config.cluster.db;
            let bytes = target.keys as u128 * lab.config.workload.value_size as u128;
            let memory_budget = db_config.block_cache_size as u128 * 4
                + db_config.write_buffer_size as u128
                    * db_config.max_write_buffer_number as u128
                    * 2;
            ensure!(
                bytes > memory_budget,
                "disk read profile requires a working set larger than cache and write buffers"
            );
            ensure!(
                lab.storage_stats()?.sst_bytes > db_config.block_cache_size as u64,
                "disk read fixture did not flush beyond block cache"
            );
        }
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        derived.insert("fixture.live_keys".into(), target.keys as f64);
        derived.insert(
            "fixture.payload_bytes".into(),
            (target.keys as f64) * lab.config.workload.value_size as f64,
        );
        if matches!(self, Self::RangeScan | Self::CrossShardScan) {
            if self == Self::CrossShardScan {
                ensure_groups(lab, 2).await?;
                ensure!(target.keys >= 2, "cross-shard-scan needs at least two keys");
                lab.split_shard_for_key(target.table, &target.key(target.keys / 2)).await?;
                lab.migrate_shard_to_new_group(target.table, &target.key(target.keys / 2)).await?;
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
                );
                reports.push(measure(lab, &db, spec).await?);
            } else {
                for limit in lab.config.workload.scan_limits.clone() {
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
            let report_target = target.clone();
            let mut spec = workload(lab, name, target, operation);
            if self == Self::Delete {
                spec.concurrency = spec.concurrency.min(spec.targets[0].keys as usize);
            }
            let report = measure(lab, &db, spec).await?;
            if self == Self::Delete && report.successes == lab.config.workload.key_space {
                ensure!(
                    db.get(report_target.table, report_target.key(0)).await?.is_none(),
                    "deleted key remained visible"
                );
            }
            reports.push(report);
        }
        Ok(case_report(lab, name, reports, derived))
    }
}
