// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;

use anyhow::{Result, ensure};

use super::support::*;
use crate::perf_lab::report::{CaseReport, case_report, workload_checks};
use crate::perf_lab::workload::{LoadModel, Operation};
use crate::perf_lab::{LabContext, RunProfile};

// Version-depth matrix shared by the MVCC cases.
const VERSION_COUNTS: &[u64] = &[1, 100, 1000];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MvccCase {
    LatestVersions,
    SnapshotPoint,
    SnapshotScan,
    TombstoneRead,
}

impl MvccCase {
    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        let fixture_spec = fixture_spec(
            lab,
            FixtureSpec { keys: 64, value_size: 1024 },
            FixtureSpec { keys: 64, value_size: 4096 },
            FixtureSpec { keys: 64, value_size: 8192 },
            FixtureSpec { keys: 64, value_size: 8192 },
        );
        let concurrency = match lab.config.profile {
            RunProfile::Smoke => 2,
            RunProfile::Regression => 16,
            RunProfile::Characterization => 32,
            RunProfile::Stress => 64,
        };
        let mut isolation = MatrixIsolation::default();
        for &versions in VERSION_COUNTS {
            isolation.start_item(lab).await?;
            let (db, mut target) =
                fixture(lab, &format!("{}_v{versions}", name), false, fixture_spec).await?;
            // Keep this matrix bounded independently of the read working-set profile.
            target.keys = target.keys.min(64);
            let old = vec![b'o'; fixture_spec.value_size];
            seed_target(lab, &db, &target, &old).await?;
            let snapshot = db.begin_txn().start_version().await?;
            for _ in 1..versions {
                seed_target(lab, &db, &target, &vec![b'n'; fixture_spec.value_size]).await?;
            }
            let latest =
                if versions == 1 { old.clone() } else { vec![b'n'; fixture_spec.value_size] };
            if self == Self::TombstoneRead || self == Self::SnapshotScan {
                for i in (0..target.keys).step_by(2) {
                    db.delete(target.table, target.key(i)).await?;
                }
                if self == Self::SnapshotScan {
                    // Insert after the snapshot: historical scan must exclude these keys.
                    for i in target.keys..target.keys + 8 {
                        db.put(target.table, target.key(i), latest.clone()).await?;
                    }
                }
            }
            derived.insert(format!("versions_{versions}.seed_versions_per_key"), versions as f64);
            derived.insert(format!("versions_{versions}.snapshot_version"), snapshot as f64);
            match self {
                Self::LatestVersions | Self::SnapshotPoint => {
                    let historical = self == Self::SnapshotPoint;
                    let spec = workload(
                        lab,
                        &format!("versions_{versions}"),
                        target.clone(),
                        Operation::Read {
                            present: true,
                            version: historical.then_some(snapshot),
                            expected: Some(if historical { old.clone() } else { latest.clone() }),
                        },
                        fixture_spec.value_size,
                        closed_loop(concurrency),
                    );
                    reports.push(measure(lab, &db, spec).await?);
                }
                Self::SnapshotScan => {
                    let mut scan_target = target.clone();
                    scan_target.keys += 8;
                    let spec = workload(
                        lab,
                        &format!("snapshot_scan_{versions}"),
                        scan_target,
                        Operation::Scan {
                            limit: target.keys + 8,
                            version: Some(snapshot),
                            expected_rows: Some(target.keys),
                            expected: Some(old.clone()),
                        },
                        fixture_spec.value_size,
                        LoadModel::Once,
                    );
                    reports.push(measure(lab, &db, spec).await?);
                }
                Self::TombstoneRead => {
                    // Restrict to a known deleted key for latest, and the same key at the old
                    // snapshot.
                    let mut deleted = target.clone();
                    deleted.keys = 1;
                    ensure!(
                        db.get(deleted.table, deleted.key(0)).await?.is_none(),
                        "deleted key still visible"
                    );
                    for historical in [false, true] {
                        let spec = workload(
                            lab,
                            &format!("tombstone_{versions}_historical_{historical}"),
                            deleted.clone(),
                            Operation::Read {
                                present: historical,
                                version: historical.then_some(snapshot),
                                expected: historical.then_some(old.clone()),
                            },
                            fixture_spec.value_size,
                            closed_loop(concurrency),
                        );
                        reports.push(measure(lab, &db, spec).await?);
                    }
                }
            }
        }
        let checks = workload_checks(&reports, true, true);
        Ok(case_report(lab, name, reports, derived, checks))
    }
}
