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

use anyhow::{Result, anyhow};
use sekas_api::server::v1::ReplicaRole;

use crate::perf_lab::report::{CaseReport, case_report};
use crate::perf_lab::workload::{WorkloadKind, spawn_workload};
use crate::perf_lab::{LabContext, PerfCase};

pub(crate) struct TransferLeaderUnderWrite;

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
