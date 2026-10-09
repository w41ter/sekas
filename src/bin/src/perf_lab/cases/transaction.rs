// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::Result;

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, TxnMode};

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
            Placement::Local => vec![1],
            Placement::Distributed => lab
                .config
                .workload
                .group_counts
                .iter()
                .copied()
                .filter(|count| *count > 1)
                .collect(),
            Placement::Matrix => lab.config.workload.group_counts.clone(),
        };
        anyhow::ensure!(
            !groups.is_empty(),
            "distributed transaction matrix needs a group count above one"
        );
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        for groups in groups {
            let key_counts = if self.key_matrix == KeyMatrix::Large {
                lab.config.workload.large_txn_key_counts.clone()
            } else {
                lab.config.workload.txn_key_counts.clone()
            };
            for keys in key_counts {
                if keys < groups || keys > (lab.config.workload.key_space / groups) * groups {
                    continue;
                }
                let name = format!("groups_{groups}_keys_{keys}");
                let (db, targets) = layout(lab, &name, groups as usize).await?;
                let mut spec = workload(
                    lab,
                    &name,
                    targets[0].clone(),
                    Operation::Transaction { mode: self.mode, keys: keys as usize },
                );
                // Keep read/write cost measurements free of accidental SI conflicts.
                spec.disjoint_keys = true;
                let per_table = keys.div_ceil(groups);
                spec.concurrency =
                    spec.concurrency.min((targets[0].keys / per_table) as usize).max(1);
                spec.targets = targets;
                reports.push(measure(lab, &db, spec).await?);
                derived.insert(format!("{name}.participants"), groups as f64);
                derived.insert(format!("{name}.keys"), keys as f64);
                derived.insert(
                    format!("{name}.write_payload_bytes"),
                    (keys * lab.config.workload.value_size as u64) as f64,
                );
            }
        }
        anyhow::ensure!(
            !reports.is_empty(),
            "no transaction matrix entries: key counts must cover participants"
        );
        Ok(case_report(lab, name, reports, derived))
    }
}
