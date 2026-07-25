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

use anyhow::Result;

use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{WorkloadKind, spawn_workload};
use crate::perf_lab::{LabContext, PerfCase};

pub(crate) struct MvccVersionAccumulation;
pub(crate) struct MvccGcImpact;
pub(crate) struct AutoShardBalance;
pub(crate) struct AutoSplitMerge;
pub(crate) struct SchemaChurn;

impl PerfCase for MvccVersionAccumulation {
    fn name(&self) -> &'static str {
        "mvcc-version-accumulation"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"mvcc-hot-key".to_vec();
        for i in 0..1000_u64 {
            db.put(table.id, key.clone(), i.to_be_bytes().to_vec()).await?;
        }
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "mvcc_version_get",
            WorkloadKind::RandomGet { table: table.id, prefix: "mvcc-hot-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            1,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert("mvcc_versions_per_key".to_owned(), 1000.0);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for MvccGcImpact {
    fn name(&self) -> &'static str {
        "mvcc-gc-impact"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"mvcc-gc-key".to_vec();
        for i in 0..500_u64 {
            db.put(table.id, key.clone(), i.to_be_bytes().to_vec()).await?;
        }
        lab.mark("baseline_start").await?;
        let workload = spawn_workload(
            db,
            "write_during_mvcc_gc",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("gc_window_start").await?;
        workload.phase("gc_window").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs.max(1))).await;
        lab.mark("gc_window_end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for AutoShardBalance {
    fn name(&self) -> &'static str {
        "auto-shard-balance"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let workload = spawn_workload(
            db,
            "write_during_auto_shard_balance",
            WorkloadKind::RandomPut { table: table.id, prefix: "auto-balance-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_balance_window").await?;
        workload.phase("balance_window").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs.max(1))).await;
        lab.mark("after_balance_window").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for AutoSplitMerge {
    fn name(&self) -> &'static str {
        "auto-split-merge"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"split-point".to_vec();
        let workload = spawn_workload(
            db,
            "write_during_split_merge",
            WorkloadKind::RandomPut { table: table.id, prefix: "split-merge-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_split").await?;
        workload.phase("split").await;
        let split = lab.split_shard_for_key(table.id, &key).await?;
        lab.mark("after_split").await?;
        workload.phase("merge").await;
        let merge =
            lab.merge_shards(split.group_id, split.left_shard_id, split.right_shard_id).await?;
        lab.mark("after_merge").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived
            .insert("split_rpc_duration_ms".to_owned(), split.rpc_duration.as_secs_f64() * 1000.0);
        derived.insert(
            "split_route_convergence_ms".to_owned(),
            split.route_convergence.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "split_route_converged".to_owned(),
            if split.route_converged { 1.0 } else { 0.0 },
        );
        derived
            .insert("merge_rpc_duration_ms".to_owned(), merge.rpc_duration.as_secs_f64() * 1000.0);
        derived.insert(
            "merge_route_convergence_ms".to_owned(),
            merge.route_convergence.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "merge_route_converged".to_owned(),
            if merge.route_converged { 1.0 } else { 0.0 },
        );
        derived.insert("merge_attempts".to_owned(), merge.attempts as f64);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for SchemaChurn {
    fn name(&self) -> &'static str {
        "schema-churn"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let workload = spawn_workload(
            db.clone(),
            "write_during_schema_churn",
            WorkloadKind::RandomPut { table: table.id, prefix: "schema-churn-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("schema_churn_start").await?;
        workload.phase("schema_churn").await;
        let started = std::time::Instant::now();
        for i in 0..16_u64 {
            let name = format!("schema_churn_{i}");
            let _ = lab.table(&db, &name).await?;
        }
        lab.mark("schema_churn_end").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert(
            "schema_churn_duration_ms".to_owned(),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}
