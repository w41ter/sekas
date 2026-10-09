// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::config::LabConfig;
use crate::perf_lab::observer::metric_counter;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, spawn_workload};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StorageCase {
    GcBacklog,
    GcSteady,
    RetentionBoundary,
    InsertCompaction,
    ChurnCompaction,
}

impl StorageCase {
    pub(super) fn configure(self, cfg: &mut LabConfig) {
        if matches!(
            self,
            Self::GcBacklog | Self::GcSteady | Self::RetentionBoundary | Self::ChurnCompaction
        ) {
            cfg.cluster.root.mvcc_gc_retention_ms = cfg.workload.gc_retention_ms;
            cfg.cluster.node.mvcc_gc_retention_ms = cfg.workload.gc_retention_ms;
            cfg.cluster.node.mvcc_gc_interval_ms = cfg.workload.gc_interval_ms;
        }
        if matches!(self, Self::InsertCompaction | Self::ChurnCompaction) {
            cfg.cluster.db.write_buffer_size = 256 * 1024;
            cfg.cluster.db.target_file_size_base = 256 * 1024;
            cfg.cluster.db.max_bytes_for_level_base = 1024 * 1024;
            cfg.cluster.db.level0_file_num_compaction_trigger = 2;
            cfg.cluster.db.mvcc_gc_retention_ms = cfg.workload.gc_retention_ms;
        }
    }

    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let (db, mut target) = fixture(lab, name, true).await?;
        let mut derived = BTreeMap::new();
        let mut reports = Vec::new();
        if matches!(self, Self::InsertCompaction | Self::ChurnCompaction) {
            let before = lab.storage_stats()?;
            let spec = workload(
                lab,
                "storage_pressure",
                target.clone(),
                if self == Self::InsertCompaction { Operation::Insert } else { Operation::Churn },
            );
            reports.push(
                measure_for(
                    lab,
                    &db,
                    spec,
                    Duration::from_secs(lab.config.workload.storage_duration_secs),
                )
                .await?,
            );
            let after = lab.storage_stats()?;
            let flushes = after.flushes.saturating_sub(before.flushes);
            let compactions = after.compactions.saturating_sub(before.compactions);
            ensure!(
                flushes >= 2 && compactions > 0,
                "storage window did not observe multiple flushes and compaction; increase storage_duration_secs/value_size"
            );
            derived.insert("storage.flushes".into(), flushes as f64);
            derived.insert("storage.compactions".into(), compactions as f64);
            derived.insert("storage.sst_bytes_before".into(), before.sst_bytes as f64);
            derived.insert("storage.sst_bytes_after".into(), after.sst_bytes as f64);
            derived.insert(
                "storage.write_stalls".into(),
                after.stalls.saturating_sub(before.stalls) as f64,
            );
        } else if self == Self::RetentionBoundary {
            target.keys = 1;
            let old = vec![b'o'; lab.config.workload.value_size];
            seed_target(&db, &target, &old).await?;
            let snapshot = db.begin_txn().start_version().await?;
            seed_target(&db, &target, &vec![b'n'; lab.config.workload.value_size]).await?;
            seed_target(&db, &target, &vec![b'p'; lab.config.workload.value_size]).await?;
            let started = Instant::now();
            let spec = workload(
                lab,
                "retained_snapshot",
                target.clone(),
                Operation::Read { present: true, version: Some(snapshot), expected: Some(old) },
            );
            // Check within retention without a warmup that could consume the retention
            // window.
            let handle = spawn_workload(db.clone(), lab.client.clone(), spec);
            let within = Duration::from_millis((lab.config.workload.gc_retention_ms / 4).max(1));
            lab.mark("retained_start");
            tokio::time::sleep(within).await;
            let report = handle.stop().await;
            validate(lab, &report)?;
            reports.push(report);
            lab.mark("retained_end");
            ensure!(
                started.elapsed().as_millis() < u128::from(lab.config.workload.gc_retention_ms),
                "retention window expired during verification; increase gc_retention_ms"
            );
            let deadline =
                Instant::now() + Duration::from_secs(lab.config.workload.event_timeout_secs);
            loop {
                if !raw_versions(lab, &target).await?.iter().any(|v| v.version <= snapshot) {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "retention did not reclaim the old user version"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let spec = workload(
                lab,
                "expired_snapshot",
                target.clone(),
                Operation::Read { present: false, version: Some(snapshot), expected: None },
            );
            let handle = spawn_workload(db.clone(), lab.client.clone(), spec);
            tokio::time::sleep(Duration::from_millis(100)).await;
            let report = handle.stop().await;
            validate(lab, &report)?;
            derived.insert("retention.old_version_reclaimed".into(), 1.0);
            reports.push(report);
        } else {
            target.keys = target.keys.min(64);
            let baseline_count = metric_counter("node_mvcc_gc_delete_versions_total");
            let spec = workload(lab, "foreground_during_gc", target.clone(), Operation::Put);
            lab.mark("gc_start");
            let handle = spawn_workload(db.clone(), lab.client.clone(), spec);
            handle.phase("accumulation");
            let (_, mut probe) = fixture(lab, "gc_immutable_probe", false).await?;
            probe.keys = 1;
            for _ in 0..16 {
                seed_target(&db, &probe, &vec![b'v'; lab.config.workload.value_size]).await?;
            }
            let before = raw_versions(lab, &probe).await?.len();
            ensure!(before > 1, "GC probe did not accumulate versions; increase gc_retention_ms");
            derived.insert("gc.probe_versions_before".into(), before as f64);
            handle.phase("gc_window");
            let deadline =
                Instant::now() + Duration::from_secs(lab.config.workload.event_timeout_secs);
            loop {
                let after = raw_versions(lab, &probe).await?.len();
                if after < before {
                    derived.insert("gc.probe_versions_after".into(), after as f64);
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "GC did not reclaim the user probe versions: before={before}, after={after}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            if self == Self::GcSteady {
                tokio::time::sleep(Duration::from_secs(lab.config.workload.storage_duration_secs))
                    .await;
            }
            handle.phase("recovery");
            tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
            let report = handle.stop().await;
            validate(lab, &report)?;
            reports.push(report);
            lab.mark("gc_end");
            let deleted = metric_counter("node_mvcc_gc_delete_versions_total") - baseline_count;
            ensure!(deleted > 0.0, "GC activity missing");
            derived.insert("gc.deleted_versions".into(), deleted);
        }
        let mut report = case_report(lab, name, reports, derived);
        report.derived.extend(observations(&report, &["node_mvcc_gc_delete_versions_total"]));
        Ok(report)
    }
}
