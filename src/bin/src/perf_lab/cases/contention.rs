// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, TxnMode, spawn_workload};

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
        for hotset in lab.config.workload.hotset_sizes.clone() {
            let (db, mut target) = fixture(lab, &format!("hotset_{hotset}"), false).await?;
            target.keys = hotset;
            seed_target(&db, &target, &vec![b's'; lab.config.workload.value_size]).await?;
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
            );
            if conflict {
                spec.concurrency = spec.concurrency.max(2);
                spec.allow_conflicts = true;
                spec.hold = Duration::from_millis(lab.config.workload.conflict_hold_ms.max(1));
                if self == Self::SiConflictRetry {
                    spec.retries = lab.config.workload.retry_limit;
                }
            }
            let report = if self == Self::LongShortTxn {
                let mut short = spec.clone();
                short.name = format!("short_hotset_{hotset}");
                short.hold = Duration::ZERO;
                let background = spawn_workload(db.clone(), lab.client.clone(), short);
                spec.name = format!("long_hotset_{hotset}");
                spec.hold = Duration::from_millis(lab.config.workload.conflict_hold_ms.max(1) * 10);
                spec.concurrency = 1;
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
        Ok(case_report(lab, name, reports, derived))
    }
}
