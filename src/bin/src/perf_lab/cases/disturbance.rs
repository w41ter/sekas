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

use anyhow::{Result, anyhow, bail};
use sekas_api::server::v1::ReplicaRole;

use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{WorkloadKind, spawn_workload};
use crate::perf_lab::{LabContext, PerfCase};

pub(crate) struct TransferLeaderUnderWrite;
pub(crate) struct ShardMetaChurnUnderRw;

impl PerfCase for TransferLeaderUnderWrite {
    fn name(&self) -> &'static str {
        "transfer-leader-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"leader-transfer-key".to_vec();
        let (group_id, _) = lab.group_for_key(table.id, &key).await?;
        let workload = spawn_workload(
            db,
            "write_during_transfer_leader",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_transfer").await?;
        workload.phase("disturbance").await;
        let transfer = lab.transfer_group_leader(group_id).await?;
        let _ = lab.group_leader(group_id).await?;
        lab.mark("after_transfer").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert(
            "transfer_rpc_duration_ms".to_owned(),
            transfer.rpc_duration.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "transfer_route_convergence_ms".to_owned(),
            transfer.route_convergence.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "transfer_route_converged".to_owned(),
            if transfer.route_converged { 1.0 } else { 0.0 },
        );
        derived.insert("transfer_target_replica".to_owned(), transfer.target_replica as f64);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

pub(crate) struct NodeOfflineUnderWrite;

impl PerfCase for NodeOfflineUnderWrite {
    fn name(&self) -> &'static str {
        "node-offline-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"node-offline-key".to_vec();
        let (group_id, _) = lab.group_for_key(table.id, &key).await?;
        lab.ensure_group_voters(group_id, 3).await?;
        let leader = lab.group_leader(group_id).await?;
        let group = lab.router.find_group(group_id)?;
        let offline_node = group
            .replicas
            .values()
            .find(|replica| replica.id != leader && replica.role == ReplicaRole::Voter as i32)
            .map(|replica| replica.node_id)
            .or_else(|| lab.nodes.keys().copied().find(|id| *id != 0))
            .ok_or_else(|| anyhow!("no node available for offline scenario"))?;
        let workload = spawn_workload(
            db,
            "write_during_node_offline",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_offline").await?;
        workload.phase("disturbance").await;
        lab.stop_server(offline_node).await?;
        tokio::time::sleep(Duration::from_millis(lab.config.cluster.raft.tick_interval_ms * 6))
            .await;
        lab.mark("after_offline").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        Ok(case_report(lab, self.name(), vec![report], BTreeMap::new()))
    }
}

pub(crate) struct ShardMigrationUnderWrite;

impl PerfCase for ShardMigrationUnderWrite {
    fn name(&self) -> &'static str {
        "shard-migration-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"migration-key".to_vec();
        let workload = spawn_workload(
            db,
            "write_during_shard_migration",
            WorkloadKind::RandomPut { table: table.id, prefix: "migration-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_migration").await?;
        workload.phase("disturbance").await;
        let migration = lab.migrate_shard_to_new_group(table.id, &key).await?;
        lab.mark("after_migration").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert(
            "shard_migration_duration_ms".to_owned(),
            migration.duration.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "shard_migration_route_convergence_ms".to_owned(),
            migration.route_convergence.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "shard_migration_route_converged".to_owned(),
            if migration.route_converged { 1.0 } else { 0.0 },
        );
        derived.insert("shard_migration_src_group".to_owned(), migration.src_group as f64);
        derived.insert("shard_migration_dest_group".to_owned(), migration.dest_group as f64);
        derived.insert("shard_migration_shard_id".to_owned(), migration.shard_id as f64);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for ShardMetaChurnUnderRw {
    fn name(&self) -> &'static str {
        "shard-meta-churn-under-rw"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let foreground_table = lab.table(&db, &lab.config.workload.table).await?;
        let meta_table = lab.table(&db, &lab.config.workload.second_table).await?;
        let foreground_probe_key = b"meta-churn-write-00000000000000000000".to_vec();
        let meta_split_key = b"meta-churn-split-point".to_vec();
        let (foreground_group, _) =
            lab.group_for_key(foreground_table.id, &foreground_probe_key).await?;
        let mut setup_migration_duration_ms = 0.0;
        let (mut meta_group, _) = lab.group_for_key(meta_table.id, &meta_split_key).await?;
        if foreground_group != meta_group {
            let migration = lab
                .migrate_shard_to_group(meta_table.id, &meta_split_key, foreground_group)
                .await?;
            setup_migration_duration_ms = migration.duration.as_secs_f64() * 1000.0;
            (meta_group, _) = lab.group_for_key(meta_table.id, &meta_split_key).await?;
        }
        if foreground_group != meta_group {
            bail!(
                "foreground table {} and meta table {} are not in the same group after setup: {} vs {}",
                foreground_table.id,
                meta_table.id,
                foreground_group,
                meta_group
            );
        }

        seed_random_read_keys(
            &db,
            foreground_table.id,
            "meta-churn-read-",
            lab.config.workload.key_space,
            lab.config.workload.value_size,
        )
        .await?;

        let write_workload = spawn_workload(
            db.clone(),
            "write_during_shard_meta_churn",
            WorkloadKind::RandomPut {
                table: foreground_table.id,
                prefix: "meta-churn-write-".to_owned(),
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        let read_workload = spawn_workload(
            db,
            "read_during_shard_meta_churn",
            WorkloadKind::RandomGet {
                table: foreground_table.id,
                prefix: "meta-churn-read-".to_owned(),
            },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );

        lab.mark("baseline_start").await?;
        write_workload.phase("baseline").await;
        read_workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("meta_churn_start").await?;
        write_workload.phase("meta_churn").await;
        read_workload.phase("meta_churn").await;
        let meta_ops = run_meta_churn_window(
            lab,
            meta_table.id,
            &meta_split_key,
            Duration::from_secs(lab.config.workload.duration_secs),
            Duration::from_secs(lab.config.workload.meta_interval_secs.max(1)),
        )
        .await?;
        lab.mark("meta_churn_end").await?;
        write_workload.phase("recovery").await;
        read_workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;

        let write_report = write_workload.stop().await;
        let read_report = read_workload.stop().await;
        let mut derived = BTreeMap::new();
        meta_ops.insert_into(&mut derived);
        derived.insert("foreground_group_id".to_owned(), foreground_group as f64);
        derived.insert("meta_table_id".to_owned(), meta_table.id as f64);
        derived.insert(
            "setup_meta_table_migration_duration_ms".to_owned(),
            setup_migration_duration_ms,
        );
        Ok(case_report(lab, self.name(), vec![write_report, read_report], derived))
    }
}

#[derive(Default)]
struct MetaChurnStats {
    rounds: u64,
    split_rpc_ms: f64,
    split_route_ms: f64,
    split_converged: u64,
    merge_rpc_ms: f64,
    merge_route_ms: f64,
    merge_converged: u64,
    merge_attempts: u64,
}

impl MetaChurnStats {
    fn observe_split(&mut self, split: &crate::perf_lab::SplitShardResult) {
        self.rounds += 1;
        self.split_rpc_ms += split.rpc_duration.as_secs_f64() * 1000.0;
        self.split_route_ms += split.route_convergence.as_secs_f64() * 1000.0;
        self.split_converged += u64::from(split.route_converged);
    }

    fn observe_merge(&mut self, merge: &crate::perf_lab::MergeShardResult) {
        self.merge_rpc_ms += merge.rpc_duration.as_secs_f64() * 1000.0;
        self.merge_route_ms += merge.route_convergence.as_secs_f64() * 1000.0;
        self.merge_converged += u64::from(merge.route_converged);
        self.merge_attempts += merge.attempts;
    }

    fn insert_into(&self, derived: &mut BTreeMap<String, f64>) {
        let rounds = self.rounds.max(1) as f64;
        derived.insert("meta_churn_rounds".to_owned(), self.rounds as f64);
        derived.insert("meta_churn_split_rpc_avg_ms".to_owned(), self.split_rpc_ms / rounds);
        derived.insert("meta_churn_split_route_avg_ms".to_owned(), self.split_route_ms / rounds);
        derived.insert("meta_churn_split_converged".to_owned(), self.split_converged as f64);
        derived.insert("meta_churn_merge_rpc_avg_ms".to_owned(), self.merge_rpc_ms / rounds);
        derived.insert("meta_churn_merge_route_avg_ms".to_owned(), self.merge_route_ms / rounds);
        derived.insert("meta_churn_merge_converged".to_owned(), self.merge_converged as f64);
        derived.insert("meta_churn_merge_attempts".to_owned(), self.merge_attempts as f64);
    }
}

async fn run_meta_churn_window(
    lab: &mut LabContext,
    table_id: u64,
    split_key: &[u8],
    duration: Duration,
    interval: Duration,
) -> Result<MetaChurnStats> {
    let deadline = std::time::Instant::now() + duration;
    let mut stats = MetaChurnStats::default();
    while std::time::Instant::now() < deadline {
        let split = lab.split_shard_for_key(table_id, split_key).await?;
        stats.observe_split(&split);
        let merge =
            lab.merge_shards(split.group_id, split.left_shard_id, split.right_shard_id).await?;
        stats.observe_merge(&merge);
        tokio::time::sleep(interval).await;
    }
    Ok(stats)
}

async fn seed_random_read_keys(
    db: &sekas_client::Database,
    table: u64,
    prefix: &str,
    keys: u64,
    value_size: usize,
) -> Result<()> {
    let value = vec![b'x'; value_size];
    for i in 0..keys {
        db.put(table, format!("{prefix}{i:020}").into_bytes(), value.clone()).await?;
    }
    Ok(())
}
