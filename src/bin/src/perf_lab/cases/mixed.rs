// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;

use super::support::*;
use crate::perf_lab::report::{
    CaseReport, case_report, latency_check, throughput_check, workload_checks,
};
use crate::perf_lab::workload::{Operation, TxnMode, spawn_workload};
use crate::perf_lab::{LabContext, RunProfile};

// Each interference case owns its background load parameters.
const READ_UPDATE_WRITE_RATIOS: &[f64] = &[0.05, 0.5];
const READ_SCAN_LIMIT: u64 = 1024;
const SMALL_LARGE_TXN_KEY_COUNTS: &[u64] = &[64, 256, 1024];
const INTERFERENCE_RATIOS: &[f64] = &[0.5];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MixedCase {
    ReadUpdate,
    ReadScan,
    SmallLargeTxn,
    HotCold,
    ImportOnline,
}

impl MixedCase {
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
        let total_qps = match lab.config.profile {
            RunProfile::Smoke => 50.0,
            RunProfile::Regression => 500.0,
            RunProfile::Characterization => 2000.0,
            RunProfile::Stress => 5000.0,
        };
        let ratios =
            if self == Self::ReadUpdate { READ_UPDATE_WRITE_RATIOS } else { INTERFERENCE_RATIOS };
        let mut isolation = MatrixIsolation::default();
        for &ratio in ratios {
            isolation.start_item(lab).await?;
            let stage = format!("ratio_{ratio}");
            let (db, target) = fixture(lab, &stage, true, fixture_spec).await?;
            let background_target = if matches!(self, Self::ReadUpdate | Self::HotCold) {
                target.clone()
            } else {
                // Scan/import use a separate table in the same group: resource interference
                // without changing foreground data.
                let (_, background_target) =
                    fixture(lab, &format!("background_{stage}"), true, fixture_spec).await?;
                lab.ensure_same_group(
                    (target.table, &target.key(0)),
                    (background_target.table, &background_target.key(0)),
                )
                .await?;
                background_target
            };
            let foreground_op = if self == Self::SmallLargeTxn {
                Operation::Transaction { mode: TxnMode::Blind, keys: 1 }
            } else {
                Operation::Read { present: true, version: None, expected: None }
            };
            let foreground_qps = total_qps * (1.0 - ratio);
            let background_qps = total_qps * ratio;
            let mut foreground = workload(
                lab,
                &format!("foreground_{stage}"),
                target.clone(),
                foreground_op,
                fixture_spec.value_size,
                open_loop(foreground_qps, 64),
            );
            let mut baseline_spec = foreground.clone();
            baseline_spec.name = format!("baseline_{stage}");
            let baseline = measure(lab, &db, baseline_spec).await?;
            derived.insert(format!("{stage}.baseline_qps"), baseline.qps);
            derived.insert(format!("{stage}.baseline_p99_us"), baseline.latency.p99_us as f64);
            let operation = match self {
                Self::ReadScan => Operation::Scan {
                    limit: READ_SCAN_LIMIT,
                    version: None,
                    expected_rows: None,
                    expected: None,
                },
                Self::SmallLargeTxn => Operation::Transaction {
                    mode: TxnMode::Blind,
                    keys: SMALL_LARGE_TXN_KEY_COUNTS
                        .iter()
                        .copied()
                        .filter(|keys| *keys <= background_target.keys)
                        .max()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "small-large-txn needs a large transaction fitting the key space"
                            )
                        })? as usize,
                },
                Self::ImportOnline => Operation::Insert,
                Self::ReadUpdate | Self::HotCold => Operation::Put,
            };
            let mut background = workload(
                lab,
                &format!("background_{stage}"),
                background_target,
                operation,
                fixture_spec.value_size,
                open_loop(background_qps, 64),
            );
            if self == Self::HotCold {
                // Foreground samples cold keys outside the hot prefix, background writes a
                // small hot subset.
                background.targets[0].keys = background.targets[0].keys.min(16);
                let (_, cold) = fixture(lab, &format!("cold_{stage}"), true, fixture_spec).await?;
                lab.ensure_same_group((target.table, &target.key(0)), (cold.table, &cold.key(0)))
                    .await?;
                foreground.targets = vec![cold];
                background.hot_probability = 0.9;
                background.hot_keys = 1;
            }
            let handle = spawn_workload(db.clone(), background);
            let result = measure(lab, &db, foreground).await;
            let background_report = handle.stop().await;
            validate(lab, &background_report)?;
            let foreground_report = result?;
            derived.insert(
                format!("{stage}.foreground_qps_ratio"),
                foreground_report.qps / baseline.qps,
            );
            reports.push(baseline);
            reports.push(foreground_report);
            reports.push(background_report);
            tokio::time::sleep(Duration::from_secs(lab.config.execution.cooldown_secs)).await;
        }
        let mut checks = workload_checks(&reports, false, false);
        checks.extend(
            reports
                .iter()
                .filter(|report| report.name.starts_with("foreground_"))
                .map(|report| latency_check(format!("{}.p99_us", report.name))),
        );
        checks.extend(
            derived
                .keys()
                .filter(|metric| metric.ends_with(".foreground_qps_ratio"))
                .map(|metric| throughput_check(metric.clone())),
        );
        Ok(case_report(lab, name, reports, derived, checks))
    }
}
