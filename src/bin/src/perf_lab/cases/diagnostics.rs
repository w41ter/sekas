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

use crate::perf_lab::report::{CaseReport, HistogramSummary, case_report};
use crate::perf_lab::workload::{WorkloadKind, WorkloadReport, spawn_workload};
use crate::perf_lab::{LabContext, PerfCase};

pub(crate) struct HotspotUpdateDiagnostics;
pub(crate) struct HotspotDirectWriteDiagnostics;
pub(crate) struct MultiKeyTxnMatrix;
pub(crate) struct RootFailoverMatrix;
pub(crate) struct SchemaChurnScale;

impl PerfCase for HotspotUpdateDiagnostics {
    fn name(&self) -> &'static str {
        "hotspot-update-diagnostics"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        for (name, key_space) in
            [("single_key", 1_u64), ("small_hotset", 16), ("wide_hotset", 1024)]
        {
            lab.mark(format!("{name}_start")).await?;
            let workload = spawn_workload(
                db.clone(),
                &format!("hotspot_{name}"),
                WorkloadKind::RandomPut { table: table.id, prefix: format!("hotspot-{name}-") },
                lab.config.workload.concurrency,
                lab.config.workload.value_size,
                key_space,
            );
            tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
            lab.mark(format!("{name}_end")).await?;
            let report = workload.stop().await;
            collect_error_derived(&mut derived, &report);
            reports.push(report);
        }
        Ok(case_report(lab, self.name(), reports, derived))
    }
}

impl PerfCase for HotspotDirectWriteDiagnostics {
    fn name(&self) -> &'static str {
        "hotspot-direct-write-diagnostics"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let mut reports = Vec::new();
        for (name, key_space) in
            [("single_key", 1_u64), ("small_hotset", 16), ("wide_hotset", 1024)]
        {
            lab.mark(format!("{name}_start")).await?;
            let started = std::time::Instant::now();
            let mut latencies = Vec::new();
            let mut successes = 0_u64;
            let mut failures = 0_u64;
            while started.elapsed() < Duration::from_secs(lab.config.workload.duration_secs) {
                let key =
                    format!("direct-hotspot-{name}-{:020}", successes % key_space).into_bytes();
                let value = successes.to_be_bytes().to_vec();
                let op_started = std::time::Instant::now();
                match lab.direct_put(table.id, key, value).await {
                    Ok(()) => successes += 1,
                    Err(_) => failures += 1,
                }
                latencies.push(op_started.elapsed().as_micros() as u64);
            }
            lab.mark(format!("{name}_end")).await?;
            let duration = started.elapsed();
            let operations = successes + failures;
            reports.push(WorkloadReport {
                name: format!("direct_hotspot_{name}"),
                operations,
                successes,
                failures,
                duration_ms: duration.as_millis(),
                qps: operations as f64 / duration.as_secs_f64().max(0.001),
                latency: HistogramSummary::from_latencies(&latencies),
                errors: BTreeMap::new(),
                phase_summaries: Vec::new(),
            });
        }
        Ok(case_report(lab, self.name(), reports, BTreeMap::new()))
    }
}

impl PerfCase for MultiKeyTxnMatrix {
    fn name(&self) -> &'static str {
        "multi-key-txn-matrix"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        for keys_per_txn in [1_usize, 2, 4, 8, 16] {
            lab.mark(format!("txn_keys_{keys_per_txn}_start")).await?;
            let workload = spawn_workload(
                db.clone(),
                &format!("multi_key_txn_{keys_per_txn}"),
                WorkloadKind::MultiKeyTxn {
                    table: table.id,
                    prefix: format!("multi-key-matrix-{keys_per_txn}-"),
                    keys_per_txn,
                },
                lab.config.workload.concurrency,
                lab.config.workload.value_size,
                lab.config.workload.key_space,
            );
            tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
            lab.mark(format!("txn_keys_{keys_per_txn}_end")).await?;
            let report = workload.stop().await;
            derived
                .insert(format!("multi_key_txn_{keys_per_txn}.keys_per_txn"), keys_per_txn as f64);
            reports.push(report);
        }
        Ok(case_report(lab, self.name(), reports, derived))
    }
}

impl PerfCase for RootFailoverMatrix {
    fn name(&self) -> &'static str {
        "root-failover-matrix"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let root_leader_node = lab.group_leader_node(0).await?;
        let workload = spawn_workload(
            db,
            "write_during_root_failover_matrix",
            WorkloadKind::RandomPut { table: table.id, prefix: "root-failover-matrix-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("failover_start").await?;
        workload.phase("failover").await;
        let started = std::time::Instant::now();
        lab.stop_server(root_leader_node).await?;
        let _ = lab.group_leader(0).await?;
        let recovery = started.elapsed();
        lab.mark("failover_end").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert("root_failover_recovery_ms".to_owned(), recovery.as_secs_f64() * 1000.0);
        derived.insert(
            "root_failover_tick_interval_ms".to_owned(),
            lab.config.cluster.raft.tick_interval_ms as f64,
        );
        derived.insert(
            "root_failover_election_tick".to_owned(),
            lab.config.cluster.raft.election_tick as f64,
        );
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for SchemaChurnScale {
    fn name(&self) -> &'static str {
        "schema-churn-scale"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let workload = spawn_workload(
            db.clone(),
            "write_during_schema_churn_scale",
            WorkloadKind::RandomPut { table: table.id, prefix: "schema-churn-scale-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        let mut derived = BTreeMap::new();
        for count in [1_u64, 10, 100] {
            lab.mark(format!("schema_churn_{count}_start")).await?;
            workload.phase(&format!("schema_churn_{count}")).await;
            let started = std::time::Instant::now();
            for i in 0..count {
                let _ = lab.table(&db, &format!("schema_churn_scale_{count}_{i}")).await?;
            }
            lab.mark(format!("schema_churn_{count}_end")).await?;
            derived.insert(
                format!("schema_churn_{count}.duration_ms"),
                started.elapsed().as_secs_f64() * 1000.0,
            );
            derived.insert(format!("schema_churn_{count}.tables"), count as f64);
        }
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

fn collect_error_derived(derived: &mut BTreeMap<String, f64>, report: &WorkloadReport) {
    for (kind, count) in &report.errors {
        derived.insert(format!("{}.errors.{kind}", report.name), *count as f64);
    }
}
