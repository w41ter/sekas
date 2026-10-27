// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::Result;

use super::support::*;
use crate::perf_lab::report::{CaseReport, case_report, workload_checks};
use crate::perf_lab::workload::{Operation, TxnMode};
use crate::perf_lab::{LabContext, RunProfile};

// Transaction case matrices. Key counts are totals across all participants.
const LOCAL_GROUP_COUNTS: &[u64] = &[1];
const DISTRIBUTED_GROUP_COUNTS: &[u64] = &[2, 4];
const LARGE_TXN_GROUP_COUNTS: &[u64] = &[1, 2, 4];
const TXN_KEY_COUNTS: &[u64] = &[1, 8, 32];
const LARGE_TXN_KEY_COUNTS: &[u64] = &[64, 256, 1024];

#[derive(Clone, Copy)]
pub(super) struct TransactionCase {
    pub(super) mode: TxnMode,
    pub(super) placement: Placement,
    pub(super) key_matrix: KeyMatrix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Placement {
    Local,
    Distributed,
    Matrix,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KeyMatrix {
    Regular,
    Large,
}

impl TransactionCase {
    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let groups = match self.placement {
            Placement::Local => LOCAL_GROUP_COUNTS,
            Placement::Distributed => DISTRIBUTED_GROUP_COUNTS,
            Placement::Matrix => LARGE_TXN_GROUP_COUNTS,
        };
        anyhow::ensure!(
            !groups.is_empty(),
            "distributed transaction matrix needs a group count above one"
        );
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        let fixture_spec = fixture_spec(
            lab,
            FixtureSpec { keys: 2048, value_size: 1024 },
            FixtureSpec { keys: 16_384, value_size: 4096 },
            FixtureSpec { keys: 32_768, value_size: 8192 },
            FixtureSpec { keys: 131_072, value_size: 8192 },
        );
        let concurrency = match lab.config.profile {
            RunProfile::Smoke => 2,
            RunProfile::Regression => 16,
            RunProfile::Characterization => 32,
            RunProfile::Stress => 64,
        };
        let mut isolation = MatrixIsolation::default();
        for &groups in groups {
            let key_counts = if self.key_matrix == KeyMatrix::Large {
                LARGE_TXN_KEY_COUNTS
            } else {
                TXN_KEY_COUNTS
            };
            for &keys in key_counts {
                if keys < groups || keys > (fixture_spec.keys / groups) * groups {
                    continue;
                }
                isolation.start_item(lab).await?;
                let name = format!("groups_{groups}_keys_{keys}");
                let (db, targets) = layout(lab, &name, groups as usize, fixture_spec).await?;
                let per_table = keys.div_ceil(groups);
                let concurrency = concurrency.min((targets[0].keys / per_table) as usize).max(1);
                let mut spec = workload(
                    lab,
                    &name,
                    targets[0].clone(),
                    Operation::Transaction { mode: self.mode, keys: keys as usize },
                    fixture_spec.value_size,
                    closed_loop(concurrency),
                );
                // Keep read/write cost measurements free of accidental SI conflicts.
                spec.disjoint_keys = true;
                spec.targets = targets;
                reports.push(measure(lab, &db, spec).await?);
                derived.insert(format!("{name}.participants"), groups as f64);
                derived.insert(format!("{name}.keys"), keys as f64);
                derived.insert(
                    format!("{name}.write_payload_bytes"),
                    (keys * fixture_spec.value_size as u64) as f64,
                );
            }
        }
        anyhow::ensure!(
            !reports.is_empty(),
            "no transaction matrix entries: key counts must cover participants"
        );
        let checks = workload_checks(&reports, true, true);
        Ok(case_report(lab, name, reports, derived, checks))
    }
}
