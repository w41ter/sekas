// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::report::{CaseReport, case_report, workload_checks};
use crate::perf_lab::workload::{Operation, TxnMode, spawn_workload};
use crate::perf_lab::{LabContext, RunProfile};

// Hot-key matrix shared by the contention cases.
const HOTSET_SIZES: &[u64] = &[1, 16, 1024];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ContentionCase {
    Hotset,
    SiConflict,
    SiConflictRetry,
    LongShortTxn,
}

impl ContentionCase {
    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        let fixture_spec = fixture_spec(
            lab,
            FixtureSpec { keys: 1024, value_size: 1024 },
            FixtureSpec { keys: 1024, value_size: 4096 },
            FixtureSpec { keys: 4096, value_size: 8192 },
            FixtureSpec { keys: 16_384, value_size: 8192 },
        );
        let (concurrency, conflict_hold_ms, retry_limit) = match lab.config.profile {
            RunProfile::Smoke => (2, 1, 2),
            RunProfile::Regression => (8, 5, 8),
            RunProfile::Characterization => (16, 5, 8),
            RunProfile::Stress => (64, 10, 16),
        };
        for &hotset in HOTSET_SIZES {
            let (db, mut target) =
                fixture(lab, &format!("hotset_{hotset}"), false, fixture_spec).await?;
            target.keys = hotset;
            seed_target(lab, &db, &target, &vec![b's'; fixture_spec.value_size]).await?;
            let conflict = self != Self::Hotset;
            let mut spec = workload(
                lab,
                &format!("hotset_{hotset}"),
                target.clone(),
                if conflict {
                    Operation::Transaction { mode: TxnMode::ReadWrite, keys: 1 }
                } else {
                    Operation::Put
                },
                fixture_spec.value_size,
                closed_loop(concurrency),
            );
            if conflict {
                spec.allow_conflicts = true;
                spec.hold = Duration::from_millis(conflict_hold_ms);
                if self == Self::SiConflictRetry {
                    spec.retries = retry_limit;
                }
            }
            let report = if self == Self::LongShortTxn {
                let mut short = spec.clone();
                short.name = format!("short_hotset_{hotset}");
                short.hold = Duration::ZERO;
                let background = spawn_workload(db.clone(), short);
                spec.name = format!("long_hotset_{hotset}");
                spec.hold = Duration::from_millis(conflict_hold_ms * 10);
                spec.load = closed_loop(1);
                let result = measure(lab, &db, spec).await;
                let short_report = background.stop().await;
                validate(lab, &short_report)?;
                reports.push(short_report);
                result?
            } else {
                measure(lab, &db, spec).await?
            };
            if conflict && hotset == 1 {
                ensure!(report.conflicts > 0, "SI conflict scenario observed no conflicts");
            }
            derived.insert(format!("{}.hotset_keys", report.name), hotset as f64);
            reports.push(report);
        }
        let checks = workload_checks(&reports, true, true);
        Ok(case_report(lab, name, reports, derived, checks))
    }
}
