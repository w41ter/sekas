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

pub(crate) struct ReplicaChangeUnderWrite;
pub(crate) struct ReplicaRemoveUnderWrite;
pub(crate) struct NodeJoinScaleOut;
pub(crate) struct RootLeaderFailover;
pub(crate) struct SnapshotUnderWrite;

impl PerfCase for ReplicaChangeUnderWrite {
    fn name(&self) -> &'static str {
        "replica-change-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"replica-change-key".to_vec();
        let (group_id, _) = lab.group_for_key(table.id, &key).await?;
        let workload = spawn_workload(
            db,
            "write_during_replica_change",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        let new_node = lab.add_server().await?;
        lab.mark("before_replica_add").await?;
        workload.phase("replica_add").await;
        let add_duration = lab.add_group_replica(group_id, new_node).await?;
        lab.mark("after_replica_add").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert("replica_add_duration_ms".to_owned(), add_duration.as_secs_f64() * 1000.0);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for ReplicaRemoveUnderWrite {
    fn name(&self) -> &'static str {
        "replica-remove-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let key = b"replica-remove-key".to_vec();
        let (group_id, _) = lab.group_for_key(table.id, &key).await?;
        let new_node = lab.add_server().await?;
        let add_duration = lab.add_group_replica(group_id, new_node).await?;
        let workload = spawn_workload(
            db,
            "write_during_replica_remove",
            WorkloadKind::FixedKeyPut { table: table.id, key },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_replica_remove").await?;
        workload.phase("replica_remove").await;
        let remove = lab
            .remove_group_replica_on_node(
                group_id,
                new_node,
                Duration::from_secs(lab.config.workload.cooldown_secs.max(1)),
            )
            .await?;
        lab.mark("after_replica_remove").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert("replica_add_duration_ms".to_owned(), add_duration.as_secs_f64() * 1000.0);
        derived.insert(
            "replica_remove_duration_ms".to_owned(),
            remove.duration.as_secs_f64() * 1000.0,
        );
        derived.insert(
            "replica_remove_converged".to_owned(),
            if remove.converged { 1.0 } else { 0.0 },
        );
        derived.insert("replica_remove_final_voters".to_owned(), remove.final_voters as f64);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for NodeJoinScaleOut {
    fn name(&self) -> &'static str {
        "node-join-scale-out"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let workload = spawn_workload(
            db,
            "write_during_node_join",
            WorkloadKind::RandomPut { table: table.id, prefix: "node-join-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_node_join").await?;
        workload.phase("node_join").await;
        let started = std::time::Instant::now();
        let _ = lab.add_server().await?;
        lab.mark("after_node_join").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived
            .insert("node_join_duration_ms".to_owned(), started.elapsed().as_secs_f64() * 1000.0);
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for RootLeaderFailover {
    fn name(&self) -> &'static str {
        "root-leader-failover"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let workload = spawn_workload(
            db,
            "write_during_root_leader_failover",
            WorkloadKind::RandomPut { table: table.id, prefix: "root-failover-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        let root_leader_node = lab.group_leader_node(0).await?;
        lab.mark("before_root_leader_stop").await?;
        workload.phase("failover").await;
        lab.stop_server(root_leader_node).await?;
        let started = std::time::Instant::now();
        let _ = lab.group_leader(0).await?;
        lab.mark("after_root_leader_stop").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut derived = BTreeMap::new();
        derived.insert(
            "root_failover_recovery_ms".to_owned(),
            started.elapsed().as_secs_f64() * 1000.0,
        );
        Ok(case_report(lab, self.name(), vec![report], derived))
    }
}

impl PerfCase for SnapshotUnderWrite {
    fn name(&self) -> &'static str {
        "snapshot-under-write"
    }

    async fn run(&self, lab: &mut LabContext) -> Result<CaseReport> {
        let db = lab.database().await?;
        let table = lab.table(&db, &lab.config.workload.table).await?;
        let seed_value_size = lab.config.workload.value_size.max(1024);
        let seed_keys = lab.config.workload.key_space.max(512);
        for i in 0..seed_keys {
            db.put(
                table.id,
                format!("snapshot-seed-{i:020}").into_bytes(),
                vec![b's'; seed_value_size],
            )
            .await?;
        }
        let key = b"snapshot-hot-key".to_vec();
        let (group_id, _) = lab.group_for_key(table.id, &key).await?;
        let new_node = lab.add_server().await?;
        let workload = spawn_workload(
            db,
            "write_during_snapshot",
            WorkloadKind::RandomPut { table: table.id, prefix: "snapshot-write-".to_owned() },
            lab.config.workload.concurrency,
            lab.config.workload.value_size,
            lab.config.workload.key_space,
        );
        lab.mark("baseline_start").await?;
        workload.phase("baseline").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.warmup_secs)).await;
        lab.mark("before_snapshot_pressure").await?;
        workload.phase("snapshot_pressure").await;
        let duration = lab.add_group_replica(group_id, new_node).await?;
        lab.mark("after_snapshot_pressure").await?;
        workload.phase("recovery").await;
        tokio::time::sleep(Duration::from_secs(lab.config.workload.cooldown_secs)).await;
        lab.mark("end").await?;
        let report = workload.stop().await;
        let mut case = case_report(lab, self.name(), vec![report], BTreeMap::new());
        let send_total = case.counter_delta_contains("raftgroup_send_snapshot_total");
        let send_bytes = case.counter_delta_contains("raftgroup_send_snapshot_bytes_total");
        let download_total = case.counter_delta_contains("raftgroup_download_snapshot_total");
        let download_bytes = case.counter_delta_contains("raftgroup_download_snapshot_bytes_total");
        case.derived.insert("snapshot_seed_keys".to_owned(), seed_keys as f64);
        case.derived.insert(
            "snapshot_seed_bytes".to_owned(),
            (seed_keys as usize * seed_value_size) as f64,
        );
        case.derived.insert("snapshot_send_total".to_owned(), send_total);
        case.derived.insert("snapshot_send_bytes_total".to_owned(), send_bytes);
        case.derived.insert("snapshot_download_total".to_owned(), download_total);
        case.derived.insert("snapshot_download_bytes_total".to_owned(), download_bytes);
        case.derived.insert(
            "snapshot_activity_observed".to_owned(),
            if send_total + send_bytes + download_total + download_bytes > 0.0 { 1.0 } else { 0.0 },
        );
        case.derived
            .insert("snapshot_replica_add_duration_ms".to_owned(), duration.as_secs_f64() * 1000.0);
        Ok(case)
    }
}
