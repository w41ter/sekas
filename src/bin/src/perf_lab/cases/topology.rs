// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{Result, bail, ensure};
use sekas_api::server::v1::ReplicaRole;
use sekas_client::Database;

use super::support::*;
use crate::perf_lab::LabContext;
use crate::perf_lab::config::LabConfig;
use crate::perf_lab::observer::metric_counter;
use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{Operation, Target, TxnMode, WorkloadReport, spawn_workload};

type Metrics = BTreeMap<String, f64>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TopologyCase {
    LeaderTransfer,
    DataLeaderFailover,
    FollowerRecovery,
    ReplicaAdd,
    ReplicaRemove,
    SnapshotCatchup,
    ShardMigration,
    SplitMerge,
    RootFailover,
}

impl TopologyCase {
    pub(super) fn configure(self, cfg: &mut LabConfig) {
        if matches!(self, Self::ReplicaAdd | Self::ReplicaRemove | Self::SnapshotCatchup) {
            cfg.cluster.node.replica.testing_knobs.disable_scheduler_durable_task = true;
        }
        if self == Self::SnapshotCatchup {
            cfg.cluster.raft.testing_knobs.force_new_peer_receiving_snapshot = true;
        }
    }

    pub(super) async fn run(self, name: &str, lab: &mut LabContext) -> Result<CaseReport> {
        let mut reports = Vec::new();
        let mut derived = Metrics::new();
        for (stage, operation) in [
            ("point_write", Operation::Put),
            ("distributed_txn", Operation::Transaction { mode: TxnMode::ReadWrite, keys: 2 }),
        ] {
            let (db, targets) = layout(lab, stage, 2).await?;
            let target = &targets[0];
            let (group, _) = lab.group_for_key(target.table, &target.key(0)).await?;
            let leader = lab.group_leader(group).await?;
            let mut spec = workload(lab, stage, target.clone(), operation);
            if matches!(spec.operation, Operation::Transaction { .. }) {
                spec.targets = targets.clone();
                spec.concurrency = 1;
            }
            let extra_node = prepare_replica_change(self, lab, group).await?;

            lab.mark(format!("{stage}_baseline"));
            let handle = spawn_workload(db.clone(), lab.client.clone(), spec);
            handle.phase("baseline");
            tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs.max(1))).await;
            handle.phase("disturbance");
            lab.mark(format!("{stage}_event_start"));
            let started = Instant::now();
            let metrics = match self {
                Self::LeaderTransfer => transfer_leader(lab, group).await?,
                Self::DataLeaderFailover => {
                    fail_data_leader(lab, &db, target, group, leader).await?
                }
                Self::FollowerRecovery => recover_follower(lab, &db, target, group, leader).await?,
                Self::ReplicaAdd | Self::SnapshotCatchup => {
                    add_replica(lab, group, extra_node.unwrap(), self == Self::SnapshotCatchup)
                        .await?
                }
                Self::ReplicaRemove => remove_replica(lab, group, extra_node.unwrap()).await?,
                Self::ShardMigration => migrate_shard(lab, target).await?,
                Self::SplitMerge => split_merge(lab, &db, target).await?,
                Self::RootFailover => {
                    let (metrics, metadata) =
                        fail_root(lab, &db, target, group, leader, stage).await?;
                    reports.push(metadata);
                    metrics
                }
            };
            derived
                .extend(metrics.into_iter().map(|(key, value)| (format!("{stage}.{key}"), value)));
            derived.insert(format!("{stage}.event_duration_ms"), millis(started.elapsed()));
            lab.mark(format!("{stage}_event_end"));

            handle.phase("recovery");
            tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs.max(1))).await;
            let report = handle.stop().await;
            validate(lab, &report)?;
            reports.push(report);
            for target in &targets {
                ensure!(
                    db.get(target.table, target.key(0)).await?.is_some(),
                    "topology action lost fixture data"
                );
            }
            lab.mark(format!("{stage}_end"));
            if matches!(self, Self::ReplicaAdd | Self::SnapshotCatchup) {
                remove_replica(lab, group, extra_node.unwrap()).await?;
            }
        }
        let mut report = case_report(lab, name, reports, derived);
        report.derived.extend(observations(
            &report,
            &[
                "raftgroup_send_snapshot_bytes_total",
                "raftgroup_download_snapshot_bytes_total",
                "raftgroup_apply_snapshot_total",
            ],
        ));
        Ok(report)
    }
}

async fn prepare_replica_change(
    case: TopologyCase,
    lab: &mut LabContext,
    group: u64,
) -> Result<Option<u64>> {
    if !matches!(
        case,
        TopologyCase::ReplicaAdd | TopologyCase::ReplicaRemove | TopologyCase::SnapshotCatchup
    ) {
        return Ok(None);
    }
    let node = lab.add_server().await?;
    if case == TopologyCase::ReplicaRemove {
        lab.add_group_replica(group, node).await?;
    }
    Ok(Some(node))
}

async fn transfer_leader(lab: &LabContext, group: u64) -> Result<Metrics> {
    let result = lab.transfer_group_leader(group).await?;
    Ok(Metrics::from([
        ("transfer_rpc_duration_ms".into(), millis(result.rpc_duration)),
        ("route_duration_ms".into(), millis(result.route_convergence)),
        ("target_replica".into(), result.target_replica as f64),
    ]))
}

async fn fail_data_leader(
    lab: &mut LabContext,
    db: &Database,
    target: &Target,
    group: u64,
    leader: u64,
) -> Result<Metrics> {
    let started = Instant::now();
    let node = lab.group_leader_node(group).await?;
    lab.stop_server(node).await?;
    wait_new_leader(lab, group, leader).await?;
    ensure!(
        db.get(target.table, target.key(0)).await?.is_some(),
        "read failed after data leader failover"
    );
    let recovery = started.elapsed();
    lab.restart_server(node).await?;
    lab.ensure_group_voters(group, 3).await?;
    Ok(Metrics::from([("recovery_duration_ms".into(), millis(recovery))]))
}

async fn recover_follower(
    lab: &mut LabContext,
    db: &Database,
    target: &Target,
    group_id: u64,
    leader: u64,
) -> Result<Metrics> {
    let group = lab.router.find_group(group_id)?;
    let follower = group
        .replicas
        .values()
        .filter(|r| r.id != leader && r.role == ReplicaRole::Voter as i32)
        .min_by_key(|r| r.node_id)
        .ok_or_else(|| anyhow::anyhow!("group {group_id} has no follower voter"))?
        .clone();
    lab.stop_server(follower.node_id).await?;
    tokio::time::sleep(Duration::from_secs(lab.config.workload.duration_secs)).await;
    let marker = b"recovery-proof".to_vec();
    let value = b"written-while-offline".to_vec();
    db.put(target.table, marker.clone(), value.clone()).await?;
    lab.restart_server(follower.node_id).await?;
    lab.group(group_id).transfer_leader(follower.id).await?;
    lab.wait_group_leader(group_id, follower.id).await?;
    ensure!(
        db.get(target.table, marker).await? == Some(value),
        "recovered replica lost acknowledged write"
    );
    Ok(Metrics::new())
}

async fn add_replica(lab: &LabContext, group: u64, node: u64, snapshot: bool) -> Result<Metrics> {
    let before = metric_counter("raftgroup_apply_snapshot_total");
    lab.add_group_replica(group, node).await?;
    if !snapshot {
        return Ok(Metrics::new());
    }
    wait_snapshot(lab, before).await?;
    Ok(Metrics::from([(
        "snapshot_applied".into(),
        metric_counter("raftgroup_apply_snapshot_total") - before,
    )]))
}

async fn remove_replica(lab: &LabContext, group: u64, node: u64) -> Result<Metrics> {
    let duration = lab
        .remove_extra_replica(
            group,
            node,
            Duration::from_secs(lab.config.workload.event_timeout_secs),
        )
        .await?;
    Ok(Metrics::from([("remove_duration_ms".into(), millis(duration))]))
}

async fn migrate_shard(lab: &LabContext, target: &Target) -> Result<Metrics> {
    let result = lab.migrate_shard_to_new_group(target.table, &target.key(0)).await?;
    ensure!(result.src_group != result.dest_group, "migration did not change owner");
    Ok(Metrics::from([
        ("migration_duration_ms".into(), millis(result.duration)),
        ("migration_route_duration_ms".into(), millis(result.route_convergence)),
        ("shard_id".into(), result.shard_id as f64),
    ]))
}

async fn split_merge(lab: &LabContext, db: &Database, target: &Target) -> Result<Metrics> {
    ensure!(target.keys >= 2, "split/merge needs two keys");
    let split = lab.split_shard_for_key(target.table, &target.key(target.keys / 2)).await?;
    for index in [0, target.keys - 1] {
        ensure!(db.get(target.table, target.key(index)).await?.is_some(), "split lost data");
    }
    let merge = lab.merge_shards(split.group_id, split.left_shard_id, split.right_shard_id).await?;
    Ok(Metrics::from([
        ("split_rpc_duration_ms".into(), millis(split.rpc_duration)),
        ("split_route_duration_ms".into(), millis(split.route_convergence)),
        ("merge_rpc_duration_ms".into(), millis(merge.rpc_duration)),
        ("merge_route_duration_ms".into(), millis(merge.route_convergence)),
        ("merge_attempts".into(), merge.attempts as f64),
    ]))
}

async fn fail_root(
    lab: &mut LabContext,
    db: &Database,
    target: &Target,
    group: u64,
    leader: u64,
    stage: &str,
) -> Result<(Metrics, WorkloadReport)> {
    let started = Instant::now();
    let old = lab.group_leader(0).await?;
    let node = lab.group_leader_node(0).await?;
    // Metadata RPCs and ordinary SI traffic both depend on root availability.
    let metadata = spawn_workload(
        db.clone(),
        lab.client.clone(),
        workload(lab, &format!("metadata_{stage}"), target.clone(), Operation::Metadata),
    );
    lab.stop_server(node).await?;
    wait_new_leader(lab, 0, old).await?;
    db.list_table().await?;
    let recovery = started.elapsed();
    lab.restart_server(node).await?;
    let report = metadata.stop().await;
    validate(lab, &report)?;
    let colocated =
        lab.router.find_group(group)?.replicas.get(&leader).is_some_and(|r| r.node_id == node);
    Ok((
        Metrics::from([
            ("root_recovery_duration_ms".into(), millis(recovery)),
            ("data_leader_colocated".into(), f64::from(colocated)),
        ]),
        report,
    ))
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

async fn wait_new_leader(lab: &LabContext, group: u64, old: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(lab.config.workload.event_timeout_secs);
    while Instant::now() < deadline {
        if lab
            .router
            .find_group(group)
            .is_ok_and(|g| g.leader_state.is_some_and(|(leader, _)| leader != old))
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    bail!("group {group} retained stale leader {old}")
}

async fn wait_snapshot(lab: &LabContext, before: f64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(lab.config.workload.event_timeout_secs);
    while Instant::now() < deadline {
        if metric_counter("raftgroup_apply_snapshot_total") > before {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("new replica did not apply a snapshot")
}
