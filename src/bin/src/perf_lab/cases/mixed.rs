// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, TxnMode, spawn_workload};

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
        let ratios = if self == Self::ReadUpdate {
            lab.config.workload.write_ratios.clone()
        } else {
            vec![0.5]
        };
        for ratio in ratios {
            let stage = format!("ratio_{ratio}");
            let (db, target) = fixture(lab, &stage, true).await?;
            let (_, background_target) = fixture(lab, &format!("background_{stage}"), true).await?;
            // Scan/import use separate tables in the same group: resource interference
            // without changing foreground data.
            lab.ensure_same_group(
                (target.table, &target.key(0)),
                (background_target.table, &background_target.key(0)),
            )
            .await?;
            let foreground_op = if self == Self::SmallLargeTxn {
                Operation::Transaction { mode: TxnMode::Blind, keys: 1 }
            } else {
                Operation::Read { present: true, version: None, expected: None }
            };
            let mut foreground =
                workload(lab, &format!("foreground_{stage}"), target.clone(), foreground_op);
            let mut baseline_spec = foreground.clone();
            baseline_spec.name = format!("baseline_{stage}");
            let baseline = measure(lab, &db, baseline_spec).await?;
            derived.insert(format!("{stage}.baseline_qps"), baseline.qps);
            derived.insert(format!("{stage}.baseline_p99_us"), baseline.latency.p99_us as f64);
            let operation = match self {
                Self::ReadScan => Operation::Scan {
                    limit: lab.config.workload.scan_limits.iter().copied().max().unwrap(),
                    version: None,
                    expected_rows: None,
                    expected: None,
                },
                Self::SmallLargeTxn => Operation::Transaction {
                    mode: TxnMode::Blind,
                    keys: lab
                        .config
                        .workload
                        .large_txn_key_counts
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
                if self == Self::ReadUpdate || self == Self::HotCold {
                    target.clone()
                } else {
                    background_target
                },
                operation,
            );
            if self == Self::ReadUpdate {
                // Offered request counts define the ratio; achieved read/write rates remain
                // separate.
                let total = lab.config.workload.offered_qps[0];
                foreground.offered_qps = Some(total * (1.0 - ratio));
                background.offered_qps = Some(total * ratio);
                if ratio == 0.0 || ratio == 1.0 {
                    continue;
                }
            }
            if self == Self::HotCold {
                // Foreground samples cold keys outside the hot prefix, background writes a
                // small hot subset.
                background.targets[0].keys = background.targets[0].keys.min(16);
                let (_, cold) = fixture(lab, &format!("cold_{stage}"), true).await?;
                lab.ensure_same_group((target.table, &target.key(0)), (cold.table, &cold.key(0)))
                    .await?;
                foreground.targets = vec![cold];
                background.hot_probability = 0.9;
                background.hot_keys = 1;
            }
            let handle = spawn_workload(db.clone(), lab.client.clone(), background);
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
            tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        }
        Ok(case_report(lab, name, reports, derived))
    }
}
