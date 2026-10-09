// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::config::LabConfig;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, TxnMode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CapacityCase {
    Concurrency,
    OfferedLoad,
    GroupScale,
    NodeScale,
    MetadataScale,
}

impl CapacityCase {
    pub(super) fn configure(self, cfg: &mut LabConfig) {
        if self == Self::NodeScale {
            cfg.cluster.node.replica.testing_knobs.disable_scheduler_durable_task = true;
        }
    }

    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let mut reports = Vec::new();
        let mut derived = BTreeMap::new();
        match self {
            Self::Concurrency | Self::OfferedLoad => {
                let (db, targets) = layout(lab, "capacity", 2).await?;
                for (name, operation) in [
                    ("read", Operation::Read { present: true, version: None, expected: None }),
                    ("update", Operation::Put),
                    ("distributed_txn", Operation::Transaction { mode: TxnMode::Blind, keys: 2 }),
                ] {
                    let mut template = workload(lab, name, targets[0].clone(), operation);
                    if name == "distributed_txn" {
                        template.targets = targets.clone();
                    }
                    if self == Self::Concurrency {
                        for concurrency in lab.config.workload.concurrency_levels.clone() {
                            let mut spec = template.clone();
                            spec.name = format!("{name}_concurrency_{concurrency}");
                            spec.concurrency = concurrency;
                            reports.push(measure(lab, &db, spec).await?);
                        }
                    } else {
                        // Overload/deadline errors are expected in this capacity probe, but
                        // validation errors are not.

                        for qps in lab.config.workload.offered_qps.clone() {
                            let mut spec = template.clone();
                            spec.name = format!("{name}_offered_{qps}");
                            spec.offered_qps = Some(qps);
                            spec.allow_overload = true;
                            let report = measure(lab, &db, spec).await?;
                            ensure!(
                                !report.errors.contains_key("validation")
                                    && !report.errors.contains_key("internal"),
                                "capacity probe encountered invalid data"
                            );
                            derived.insert(format!("{}.offered_qps", report.name), qps);
                            reports.push(report);
                        }
                    }
                }
            }
            Self::GroupScale => {
                for groups in lab.config.workload.group_counts.clone() {
                    let (db, targets) =
                        layout(lab, &format!("scale_{groups}"), groups as usize).await?;
                    let mut spec = workload(
                        lab,
                        &format!("groups_{groups}"),
                        targets[0].clone(),
                        Operation::Put,
                    );
                    spec.targets = targets;
                    spec.concurrency =
                        *lab.config.workload.concurrency_levels.iter().max().unwrap();
                    reports.push(measure(lab, &db, spec).await?);
                }
            }
            Self::NodeScale => {
                let participants =
                    lab.config.workload.group_counts.iter().copied().max().unwrap().max(2);
                let (db, targets) = layout(lab, "node_scale", participants as usize).await?;
                let mut spec =
                    workload(lab, "before_scale_out", targets[0].clone(), Operation::Put);
                spec.targets = targets.clone();
                spec.concurrency = *lab.config.workload.concurrency_levels.iter().max().unwrap();
                let before = measure(lab, &db, spec.clone()).await?;
                let started = Instant::now();
                let node = lab.add_server().await?;
                // Explicit redistribution makes scale-out reproducible without a heuristic
                // scheduler.
                for target in targets.iter().step_by(2) {
                    let group = lab.group_for_key(target.table, &target.key(0)).await?.0;
                    let old_leader_node = lab.group_leader_node(group).await?;
                    lab.add_group_replica(group, node).await?;
                    let replica = lab
                        .router
                        .find_group(group)?
                        .replicas
                        .values()
                        .find(|r| r.node_id == node)
                        .unwrap()
                        .id;
                    lab.group(group).transfer_leader(replica).await?;
                    lab.wait_group_leader(group, replica).await?;
                    lab.remove_extra_replica(
                        group,
                        old_leader_node,
                        Duration::from_secs(lab.config.workload.event_timeout_secs),
                    )
                    .await?;
                }
                derived.insert(
                    "scale_out.event_duration_ms".into(),
                    started.elapsed().as_secs_f64() * 1000.0,
                );
                spec.name = "after_scale_out".into();
                let after = measure(lab, &db, spec).await?;
                derived.insert("scale_out.throughput_ratio".into(), after.qps / before.qps);
                reports.extend([before, after]);
            }
            Self::MetadataScale => {
                for count in lab.config.workload.metadata_counts.clone() {
                    let db = lab.client.create_database(format!("metadata_scale_{count}")).await?;
                    for i in 0..count {
                        lab.table(&db, &format!("table_{i}")).await?;
                    }
                    ensure!(
                        db.list_table().await?.len() == count as usize,
                        "metadata fixture count mismatch"
                    );
                    let table = db.open_table("table_0".into()).await?;
                    let target = crate::perf_lab::workload::Target {
                        table: table.id,
                        prefix: "key-".into(),
                        keys: lab.config.workload.key_space,
                    };
                    seed_target(&db, &target, &vec![b's'; lab.config.workload.value_size]).await?;
                    let spec = workload(
                        lab,
                        &format!("metadata_{count}"),
                        target.clone(),
                        Operation::Metadata,
                    );
                    reports.push(measure(lab, &db, spec).await?);
                    let spec = workload(
                        lab,
                        &format!("data_read_{count}"),
                        target,
                        Operation::Read { present: true, version: None, expected: None },
                    );
                    reports.push(measure(lab, &db, spec).await?);
                    let deadline = Instant::now()
                        + Duration::from_secs(lab.config.workload.event_timeout_secs);
                    let started = Instant::now();
                    lab.table(&db, "new_table").await?;
                    if Instant::now() > deadline {
                        bail!("create table exceeded event timeout");
                    }
                    derived.insert(
                        format!("metadata_{count}.create_duration_ms"),
                        started.elapsed().as_secs_f64() * 1000.0,
                    );
                    for table in db.list_table().await? {
                        db.delete_table(table.name).await?;
                    }
                }
            }
        }
        Ok(case_report(lab, name, reports, derived))
    }
}
