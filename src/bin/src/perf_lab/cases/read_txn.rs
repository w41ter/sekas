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

const PREFIX: &str = "perf-key-";

pub(crate) struct PointRead;
pub(crate) struct MixedReadWrite;
pub(crate) struct PrefixScan;
pub(crate) struct TxnConflict;
pub(crate) struct MultiKeyTxn;
pub(crate) struct ValueSizeMatrix;

impl PerfCase for PointRead {
    fn name(&self) -> &'static str {
        "point-read"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        seed_table(&db, table.id, lab.config.workload.key_space, lab.config.workload.value_size)
            .await?;
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "point_read",
            WorkloadKind::RandomGet { table: table.id, prefix: PREFIX.to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for MixedReadWrite {
    fn name(&self) -> &'static str {
        "mixed-read-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        seed_table(&db, table.id, lab.config.workload.key_space, lab.config.workload.value_size)
            .await?;
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "mixed_read_write",
            WorkloadKind::MixedReadWrite {
                table: table.id,
                prefix: PREFIX.to_owned(),
                write_ratio: 0.1,
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for PrefixScan {
    fn name(&self) -> &'static str {
        "prefix-scan"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        seed_table(&db, table.id, lab.config.workload.key_space, lab.config.workload.value_size)
            .await?;
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "prefix_scan",
            WorkloadKind::PrefixScan {
                table: table.id,
                prefix: PREFIX.as_bytes().to_vec(),
                limit: 128,
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for TxnConflict {
    fn name(&self) -> &'static str {
        "txn-conflict"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        db.put(table.id, b"txn-conflict-guard".to_vec(), b"seed".to_vec()).await?;
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "txn_conflict",
            WorkloadKind::TxnConflict {
                table: table.id,
                guard_key: b"txn-conflict-guard".to_vec(),
                prefix: "txn-conflict-key-".to_owned(),
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for MultiKeyTxn {
    fn name(&self) -> &'static str {
        "multi-key-txn"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        lab.mark("start").await?;
        let workload = spawn_workload(
            db,
            "multi_key_txn",
            WorkloadKind::MultiKeyTxn {
                table: table.id,
                prefix: "multi-key-txn-".to_owned(),
                keys_per_txn: 8,
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

impl PerfCase for ValueSizeMatrix {
    fn name(&self) -> &'static str {
        "value-size-matrix"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        for size in [16usize, 128, 1024, 16 * 1024] {
            lab.mark(format!("value_size_{size}_start")).await?;
            let workload = spawn_workload(
                db.clone(),
                &format!("value_size_{size}"),
                WorkloadKind::RandomPut { table: table.id, prefix: format!("value-size-{size}-") },
                lab.config.workload.concurrency,
                size,
                lab.config.workload.key_space,
            );
            tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
            lab.mark(format!("value_size_{size}_end")).await?;
            let report = workload.stop().await;
            let payload_bytes = report.successes as f64 * size as f64;
            let duration_secs = (report.duration_ms as f64 / 1000.0).max(0.001);
            let kib = (size as f64 / 1024.0).max(1.0 / 1024.0);
            derived.insert(format!("value_size_{size}.qps"), report.qps);
            derived.insert(format!("value_size_{size}.p99_us"), report.latency.p99_us as f64);
            derived
                .insert(format!("value_size_{size}.bytes_per_sec"), payload_bytes / duration_secs);
            derived.insert(
                format!("value_size_{size}.p99_us_per_kib"),
                report.latency.p99_us as f64 / kib,
            );
            derived.insert(format!("value_size_{size}.qps_per_kib"), report.qps / kib);
            reports.push(report);
        }
        Ok(case_report(lab, self.name(), reports, derived))
    }
}

async fn seed_table(
    db: &sekas_client::Database,
    table: u64,
    keys: u64,
    value_size: usize,
) -> Result<()> {
    let value = vec![b'x'; value_size];
    for i in 0..keys {
        db.put(table, format!("{PREFIX}{i:020}").into_bytes(), value.clone()).await?;
    }
    Ok(())
}
