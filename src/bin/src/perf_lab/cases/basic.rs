// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Result, ensure};

use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{WorkloadKind, spawn_workload};
use crate::perf_lab::{LabContext, PerfCase};

pub(crate) struct SingleKeyUpdate;

impl PerfCase for SingleKeyUpdate {
    fn name(&self) -> &'static str {
        "single-key-update"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"single-key".to_vec();
        lab.mark("start");
        let workload = spawn_workload(
            db,
            "single_key_update",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end");
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

pub(crate) struct BatchTxnCommit;

impl PerfCase for BatchTxnCommit {
    fn name(&self) -> &'static str {
        "batch-txn-commit"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let left = lab.table(&db, &lab.config.workload.table).await?;
        let right = lab.table(&db, &lab.config.workload.second_table).await?;
        // A probe represents the whole workload only while each table has one shard.
        for table_id in [left.id, right.id] {
            let (_, shard) = lab.group_for_key(table_id, b"").await?;
            ensure!(
                shard
                    .range
                    .as_ref()
                    .is_some_and(|range| range.start.is_empty() && range.end.is_empty()),
                "batch-txn-commit requires a single shard per table; use a fresh cluster"
            );
        }
        let (left_group, right_group) = lab
            .ensure_different_group(
                (left.id, b"left-00000000000000000000"),
                (right.id, b"right-00000000000000000000"),
            )
            .await?;
        lab.mark("start");
        let workload = spawn_workload(
            db,
            "batch_txn_commit",
            WorkloadKind::BatchTxnCommit { left_table: left.id, right_table: right.id },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end");
        let report = workload.stop().await;
        let derived = BTreeMap::from([
            ("left_group_id".to_owned(), left_group as f64),
            ("right_group_id".to_owned(), right_group as f64),
        ]);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}
