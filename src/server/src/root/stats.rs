// Copyright 2024-present The Sekas Authors.
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

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::{Arc, Mutex};

use rand::RngExt;
use sekas_api::server::v1::*;
use tokio::time::Instant;

use super::liveness::Liveness;
use super::schedule::TaskReason;

mod show;

pub(super) use show::{ShowMetrics, ShowSnapshot};

struct GroupDelta {
    epoch: u64,
    updated_at: Instant,
    started_at: Instant,
    incoming: Vec<ReplicaDesc>,
    outgoing: Vec<ReplicaDesc>,
    repair: Option<ReplicaRepair>,
    reason: Option<TaskReason>,
}

impl GroupDelta {
    fn pending(&self) -> bool {
        !self.incoming.is_empty() || !self.outgoing.is_empty()
    }

    fn has_activity(&self) -> bool {
        self.pending() || self.repair.is_some()
    }

    fn replica_move(&self) -> Option<(u64, u64)> {
        match (&self.incoming[..], &self.outgoing[..], &self.repair) {
            ([incoming], [outgoing], None) => Some((incoming.id, outgoing.id)),
            _ => None,
        }
    }
}

pub(super) struct ReplicaChangeStatus {
    pub group: u64,
    pub change: ReplicaRepair,
    pub repair: bool,
    pub age_s: u64,
    pub reason: Option<TaskReason>,
}

pub(super) struct ShardMoveStatus {
    pub shard: u64,
    pub source: u64,
    pub destination: u64,
    pub submitted_at: Option<Instant>,
    pub reason: Option<TaskReason>,
}

pub(super) struct ReplicaIssue {
    pub group: u64,
    pub replica: u64,
    pub node: u64,
    pub reason: &'static str,
}

struct NodeReplicaReport {
    received_at: Instant,
    terms: HashMap<(u64, u64), u64>,
}

#[derive(Clone, Default)]
pub struct NodeDelta {
    pub replica_count: i64,
    // TODO: qps
}

#[derive(Default, Clone)]
pub struct ClusterStats {
    sched_stats: Arc<Mutex<SchedStats>>,
    job_stats: Arc<Mutex<JobStats>>,
    table_set_stats: Arc<Mutex<TableSetStats>>,
    show_stats: Arc<Mutex<show::ShowStats>>,
    node_resources: Arc<Mutex<HashMap<u64, NodeResourceSample>>>,
    replica_progress: Arc<Mutex<HashMap<(u64, u64, u64), ReplicaProgress>>>,
    replica_reports: Arc<Mutex<HashMap<u64, NodeReplicaReport>>>,
}

#[derive(Default)]
struct NodeResourceSample {
    current: Option<(Instant, NodeResourceUsage)>,
    displayed: NodeResourceUsage,
}

impl NodeResourceSample {
    fn update(&mut self, resources: Option<NodeResourceUsage>) {
        self.current = resources.map(|usage| (Instant::now(), usage));
        if let Some(usage) = resources {
            let displayed = &mut self.displayed;
            displayed.cpu_used = usage.cpu_used.or(displayed.cpu_used);
            if usage.cpu_capacity > 0. {
                displayed.cpu_capacity = usage.cpu_capacity;
            }
            if usage.memory_capacity > 0 {
                displayed.memory_used = usage.memory_used;
                displayed.memory_capacity = usage.memory_capacity;
            }
            if usage.disk_capacity > 0 {
                displayed.disk_used = usage.disk_used;
                displayed.disk_capacity = usage.disk_capacity;
            }
        }
    }
}

#[cfg(test)]
#[test]
fn show_uses_cached_node_values_without_refreshing_placement_inputs() {
    let stats = ClusterStats::default();
    let latest = NodeResourceUsage {
        cpu_used: Some(2.),
        cpu_capacity: 8.,
        memory_used: 14,
        memory_capacity: 16,
        disk_used: 10,
        disk_capacity: 100,
        ..Default::default()
    };
    stats.handle_node_resources(1, Some(latest));
    stats.handle_node_resources(
        1,
        Some(NodeResourceUsage { disk_used: 0, disk_capacity: 100, ..Default::default() }),
    );
    let displayed = stats.display_node_resources()[&1];
    assert_eq!(displayed.cpu_used, Some(2.));
    assert_eq!(displayed.memory_used, 14);
    assert_eq!(displayed.disk_used, 0);
    let fresh = stats.node_resources(std::time::Duration::from_secs(5))[&1];
    assert_eq!(fresh.cpu_used, None);
    assert_eq!(fresh.memory_capacity, 0);
    stats.handle_node_resources(1, None);
    assert!(!stats.node_resources(std::time::Duration::from_secs(5)).contains_key(&1));
    assert_eq!(stats.display_node_resources()[&1], displayed);
}

#[cfg(test)]
#[test]
fn replica_change_waits_for_a_heartbeat_requested_after_submission() {
    let stats = ClusterStats::default();
    let started = Instant::now();
    let moving = ScheduleState {
        group_id: 1,
        epoch: 42,
        incoming_replicas: vec![ReplicaDesc { id: 1, node_id: 2, ..Default::default() }],
        ..Default::default()
    };
    let completed = ScheduleState { group_id: 1, epoch: 42, ..Default::default() };
    stats.handle_schedule_update(&[moving], None);
    stats.handle_heartbeat_schedule_update(std::slice::from_ref(&completed), started);
    assert!(stats.has_pending_replica_changes());
    stats.handle_heartbeat_schedule_update(
        &[completed],
        started + std::time::Duration::from_secs(1),
    );
    assert!(!stats.has_pending_replica_changes());
}

#[derive(Default)]
struct SchedStats {
    raw_group_delta: HashMap<u64 /* group */, GroupDelta>,
    node_view: HashMap<u64 /* node */, NodeDelta>,
    split_shards: HashSet<u64>,
    moving_shards: HashMap<u64, ShardMove>,
    merge_shards: HashSet<(u64, u64)>,
    split_retry_at: HashMap<u64, Instant>,
}

struct ShardMove {
    source: u64,
    destination: u64,
    submitted_at: Option<Instant>,
    reason: Option<TaskReason>,
}

#[derive(Default)]
struct JobStats {
    node_delta: HashMap<u64 /* node_id */, NodeDelta>,
}

#[derive(Default)]
pub struct TableSetStats {
    tables: HashMap<u64, TableStats>,
}

#[derive(Default)]
pub struct TableStats {
    shards: HashMap<u64, ShardStats>,
    shard_indexes: HashMap<u64, /* group */ u64>,
}

impl ClusterStats {
    pub(super) fn handle_node_resources(&self, node: u64, resources: Option<NodeResourceUsage>) {
        self.node_resources.lock().unwrap().entry(node).or_default().update(resources);
    }

    pub(super) fn node_resources(
        &self,
        max_age: std::time::Duration,
    ) -> HashMap<u64, NodeResourceUsage> {
        let now = Instant::now();
        self.node_resources
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(&node, sample)| {
                let (sampled, resources) = sample.current?;
                (now.duration_since(sampled) <= max_age).then_some((node, resources))
            })
            .collect()
    }

    pub(super) fn display_node_resources(&self) -> HashMap<u64, NodeResourceUsage> {
        self.node_resources
            .lock()
            .unwrap()
            .iter()
            .map(|(&node, sample)| (node, sample.displayed))
            .collect()
    }

    pub(super) fn display_replica_progress(
        &self,
        group: u64,
        replica: &ReplicaDesc,
        current: Option<ReplicaProgress>,
    ) -> Option<ReplicaProgress> {
        let key = (group, replica.id, replica.node_id);
        let mut samples = self.replica_progress.lock().unwrap();
        if let Some(progress) = current {
            samples.insert(key, progress);
            Some(progress)
        } else {
            samples.get(&key).copied()
        }
    }

    pub(super) fn handle_replica_report(&self, node: u64, states: &[ReplicaState]) {
        self.replica_reports.lock().unwrap().insert(
            node,
            NodeReplicaReport {
                received_at: Instant::now(),
                terms: states
                    .iter()
                    .map(|state| ((state.group_id, state.replica_id), state.term))
                    .collect(),
            },
        );
    }

    pub(super) fn replica_issues(
        &self,
        groups: &[GroupDesc],
        nodes: &[NodeDesc],
        liveness: &Liveness,
        max_age: std::time::Duration,
    ) -> Vec<ReplicaIssue> {
        let node_issues: HashMap<_, _> = nodes
            .iter()
            .map(|node| {
                let reason = if node.status == NodeStatus::Decommissioned as i32 {
                    Some("node decommissioned")
                } else if liveness.get(&node.id).is_dead() {
                    Some("node unreachable")
                } else {
                    None
                };
                (node.id, reason)
            })
            .collect();
        let reports = self.replica_reports.lock().unwrap();
        let now = Instant::now();
        let mut issues = Vec::new();
        for group in groups {
            for replica in &group.replicas {
                let reason = match node_issues.get(&replica.node_id) {
                    None => Some("node missing"),
                    Some(Some(reason)) => Some(*reason),
                    Some(None) => reports
                        .get(&replica.node_id)
                        .filter(|report| now.duration_since(report.received_at) <= max_age)
                        .and_then(|report| match report.terms.get(&(group.id, replica.id)) {
                            None => Some("missing from latest heartbeat"),
                            Some(0) => Some("Raft uninitialized"),
                            Some(_) => None,
                        }),
                }
                .or(match ReplicaRole::try_from(replica.role) {
                    Ok(ReplicaRole::Voter) => None,
                    Ok(ReplicaRole::Learner) => Some("learner"),
                    Ok(ReplicaRole::IncomingVoter) => Some("incoming voter"),
                    Ok(ReplicaRole::DemotingVoter) => Some("demoting voter"),
                    Err(_) => Some("unknown replica role"),
                });
                if let Some(reason) = reason {
                    issues.push(ReplicaIssue {
                        group: group.id,
                        replica: replica.id,
                        node: replica.node_id,
                        reason,
                    });
                }
            }
        }
        issues
    }

    pub(super) fn retain_display_samples(&self, nodes: &[NodeDesc], groups: &[GroupDesc]) {
        self.show_stats.lock().unwrap().retain(groups);
        let node_ids: HashSet<_> = nodes.iter().map(|node| node.id).collect();
        self.node_resources.lock().unwrap().retain(|node, _| node_ids.contains(node));
        self.replica_reports.lock().unwrap().retain(|node, _| node_ids.contains(node));
        let replicas: HashSet<_> = groups
            .iter()
            .flat_map(|group| {
                group.replicas.iter().map(|replica| (group.id, replica.id, replica.node_id))
            })
            .collect();
        self.replica_progress.lock().unwrap().retain(|replica, _| replicas.contains(replica));
    }

    pub fn handle_group_stats(&self, group_stats: &GroupStats) {
        {
            let mut sched_stats = self.sched_stats.lock().expect("poisoned");
            for shard in &group_stats.shard_stats {
                if sched_stats.moving_shards.get(&shard.shard_id).is_some_and(|migration| {
                    migration.destination == group_stats.group_id
                        && migration.submitted_at.is_none()
                }) {
                    sched_stats.moving_shards.remove(&shard.shard_id);
                }
            }
        }
        {
            let mut table_set = self.table_set_stats.lock().expect("poisoned");
            for shard in &group_stats.shard_stats {
                let shard_id = shard.shard_id;
                let table_stats = table_set.tables.entry(shard.table_id).or_default();
                table_stats.shards.insert(shard_id, shard.clone());
                table_stats.shard_indexes.insert(shard_id, group_stats.group_id);
            }
        }
    }

    pub(super) fn handle_show_group_stats(&self, group_stats: &GroupStats, node_id: u64) -> bool {
        self.show_stats.lock().unwrap().update(group_stats, node_id)
    }

    pub(super) fn confirm_shard_moves(&self, stats: &GroupStats, requested_at: Instant) {
        let migration_in_progress =
            stats.sample.and_then(|sample| sample.shard_migration_in_progress);
        if migration_in_progress == Some(true) {
            return;
        }
        self.sched_stats.lock().unwrap().moving_shards.retain(|shard, migration| {
            if migration.destination != stats.group_id
                || migration.submitted_at.is_none_or(|submitted| requested_at <= submitted)
            {
                return true;
            }
            // A report requested after submission can observe completion or
            // rollback. Older nodes can still confirm ownership at the destination.
            migration_in_progress.is_none()
                && !stats.shard_stats.iter().any(|s| s.shard_id == *shard)
        });
    }

    pub(super) fn show_snapshot(
        &self,
        groups: &[GroupDesc],
        max_age: std::time::Duration,
    ) -> ShowSnapshot {
        self.show_stats.lock().unwrap().snapshot(groups, max_age)
    }

    pub(super) fn display_snapshot(
        &self,
        groups: &[GroupDesc],
        max_age: std::time::Duration,
    ) -> ShowSnapshot {
        self.show_stats.lock().unwrap().display_snapshot(groups, max_age)
    }

    pub fn handle_schedule_update(
        &self,
        state_updates: &[ScheduleState],
        job_updates: Option<HashMap<u64 /* node */, NodeDelta>>,
    ) {
        if !state_updates.is_empty() {
            let mut inner = self.sched_stats.lock().unwrap();
            if inner.replace_state(state_updates, None) {
                inner.rebuild_view();
            }
        }
        if let Some(job_updates) = job_updates.as_ref() {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta.clone_from(job_updates);
        }
    }

    pub(super) fn handle_heartbeat_schedule_update(
        &self,
        updates: &[ScheduleState],
        requested_at: Instant,
    ) {
        let mut inner = self.sched_stats.lock().unwrap();
        if inner.replace_state(updates, Some(requested_at)) {
            inner.rebuild_view();
        }
    }

    pub(super) fn handle_replica_move_update(
        &self,
        state: ScheduleState,
        incoming: u64,
        outgoing: u64,
        reason: &TaskReason,
    ) {
        let group = state.group_id;
        let mut inner = self.sched_stats.lock().unwrap();
        if inner.replace_state(&[state], None) {
            inner.rebuild_view();
        }
        if let Some(delta) = inner.raw_group_delta.get_mut(&group)
            && delta.replica_move() == Some((incoming, outgoing))
        {
            delta.reason = Some(reason.clone());
        }
    }

    /// Add the spliting shard to sched stats.
    pub fn handle_split_shard(&self, shard_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.split_shards.insert(shard_id);
    }

    /// Finish the splited shard from sched stats.
    pub fn finish_split_shard(&self, shard_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.split_shards.remove(&shard_id);
    }

    pub(super) fn defer_shard_split(&self, shard_id: u64, delay: std::time::Duration) {
        let now = Instant::now();
        let mut stats = self.sched_stats.lock().unwrap();
        stats.split_retry_at.retain(|_, retry_at| *retry_at > now);
        stats.split_retry_at.insert(shard_id, now + delay);
    }

    pub(super) fn is_shard_split_cooling_down(&self, shard_id: u64) -> bool {
        self.sched_stats
            .lock()
            .unwrap()
            .split_retry_at
            .get(&shard_id)
            .is_some_and(|retry_at| *retry_at > Instant::now())
    }

    pub(super) fn handle_move_shard(
        &self,
        shard_id: u64,
        source_group_id: u64,
        dest_group_id: u64,
        reason: Option<TaskReason>,
    ) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.moving_shards.insert(
            shard_id,
            ShardMove {
                source: source_group_id,
                destination: dest_group_id,
                submitted_at: None,
                reason,
            },
        );
    }

    pub(super) fn mark_shard_move_submitted(&self, shard_id: u64) {
        if let Some(migration) = self.sched_stats.lock().unwrap().moving_shards.get_mut(&shard_id) {
            migration.submitted_at = Some(Instant::now());
        }
    }

    pub(super) fn has_pending_shard_moves(&self) -> bool {
        !self.sched_stats.lock().unwrap().moving_shards.is_empty()
    }

    pub(super) fn is_replica_change_pending(&self, group: u64) -> bool {
        self.sched_stats
            .lock()
            .unwrap()
            .raw_group_delta
            .get(&group)
            .is_some_and(GroupDelta::pending)
    }

    pub(super) fn has_pending_replica_changes(&self) -> bool {
        self.sched_stats.lock().unwrap().raw_group_delta.values().any(GroupDelta::pending)
    }

    pub(super) fn shard_moves(&self) -> Vec<ShardMoveStatus> {
        let mut moves: Vec<_> = self
            .sched_stats
            .lock()
            .unwrap()
            .moving_shards
            .iter()
            .map(|(&shard, state)| ShardMoveStatus {
                shard,
                source: state.source,
                destination: state.destination,
                submitted_at: state.submitted_at,
                reason: state.reason.clone(),
            })
            .collect();
        moves.sort_unstable_by_key(|state| state.shard);
        moves
    }

    pub(super) fn shard_migration_wait(&self, active_groups: &[u64]) -> Option<String> {
        let moves = self.shard_moves();
        if !moves.is_empty() {
            let details = moves
                .iter()
                .map(|migration| {
                    let target = format!(
                        "Shard {} g{} -> g{}",
                        migration.shard, migration.source, migration.destination
                    );
                    if let Some(submitted) = migration.submitted_at {
                        format!(
                            "{target} submitted {}s ago. Waiting for g{} to confirm completion.",
                            submitted.elapsed().as_secs(),
                            migration.destination
                        )
                    } else {
                        format!("{target} is queued.")
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Some(format!("{details}\nSee SHOW SCHEDULER_TASKS for the move."));
        }
        if active_groups.is_empty() {
            return None;
        }
        let groups = active_groups.iter().map(|id| format!("g{id}")).collect::<Vec<_>>().join(", ");
        Some(format!(
            "Active shard migration reported by {groups}. Waiting for the next idle report."
        ))
    }

    pub(super) fn replica_changes(&self) -> Vec<ReplicaChangeStatus> {
        let mut changes: Vec<_> = self
            .sched_stats
            .lock()
            .unwrap()
            .raw_group_delta
            .iter()
            .filter(|(_, delta)| delta.has_activity())
            .map(|(&group, delta)| ReplicaChangeStatus {
                group,
                change: delta.repair.clone().unwrap_or_else(|| ReplicaRepair {
                    incoming_replicas: delta.incoming.clone(),
                    outgoing_replicas: delta.outgoing.clone(),
                }),
                repair: delta.repair.is_some(),
                age_s: delta.started_at.elapsed().as_secs(),
                reason: delta.reason.clone(),
            })
            .collect();
        changes.sort_unstable_by_key(|state| state.group);
        changes
    }

    pub fn finish_move_shard(&self, shard_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.moving_shards.remove(&shard_id);
    }

    pub fn handle_merge_shard(&self, left_shard_id: u64, right_shard_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.merge_shards.insert((left_shard_id, right_shard_id));
    }

    pub fn finish_merge_shard(&self, left_shard_id: u64, right_shard_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.merge_shards.remove(&(left_shard_id, right_shard_id));
    }

    pub fn is_merging_shard(&self, left_shard_id: u64, right_shard_id: u64) -> bool {
        let sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.merge_shards.contains(&(left_shard_id, right_shard_id))
    }

    pub fn is_shard_scheduled(&self, shard_id: u64) -> bool {
        let sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats.split_shards.contains(&shard_id)
            || sched_stats.moving_shards.contains_key(&shard_id)
            || sched_stats
                .merge_shards
                .iter()
                .any(|(left, right)| *left == shard_id || *right == shard_id)
    }

    pub fn get_node_delta(&self, node: u64) -> NodeDelta {
        let mut rs = NodeDelta::default();
        if let Some(sched_node_delta) = {
            let inner = self.sched_stats.lock().unwrap();
            inner.node_view.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += sched_node_delta.replica_count;
        }
        if let Some(job_node_delta) = {
            let inner = self.job_stats.lock().unwrap();
            inner.node_delta.get(&node).map(ToOwned::to_owned)
        } {
            rs.replica_count += job_node_delta.replica_count;
        }
        rs
    }

    /// Get the large shards, return the group_id and shard_id.
    pub fn get_large_shards(&self, limit: usize, split_threshold: u64) -> Vec<(u64, u64)> {
        if limit == 0 {
            return Vec::new();
        }
        let scheduled_shards = {
            let sched_stats = self.sched_stats.lock().expect("poisoned");
            let mut excluded = sched_stats.scheduled_shards();
            let now = Instant::now();
            excluded.extend(
                sched_stats
                    .split_retry_at
                    .iter()
                    .filter_map(|(&shard, &retry_at)| (retry_at > now).then_some(shard)),
            );
            excluded
        };
        let table_set = self.table_set_stats.lock().expect("poisoned");
        let mut target_shards = Vec::with_capacity(limit);
        let mut seen = 0;
        let mut rng = rand::rng();
        for table_stats in table_set.tables.values() {
            for shard_stats in table_stats.shards.values() {
                if shard_stats.shard_size <= split_threshold
                    || scheduled_shards.contains(&shard_stats.shard_id)
                {
                    continue;
                }
                if let Some(&group_id) = table_stats.shard_indexes.get(&shard_stats.shard_id) {
                    if group_id == sekas_schema::ROOT_GROUP_ID {
                        // Don't split the root groups, such as txn shard.
                        continue;
                    }
                    seen += 1;
                    if target_shards.len() < limit {
                        target_shards.push((group_id, shard_stats.shard_id));
                    } else {
                        // Reservoir sampling gives every eligible shard a
                        // chance without storing the entire candidate set.
                        let index = rng.random_range(0..seen);
                        if index < limit {
                            target_shards[index] = (group_id, shard_stats.shard_id);
                        }
                    }
                }
            }
        }
        target_shards
    }

    /// Get the stats of a shard.
    pub fn get_shard_stats(&self, shard_id: u64) -> Option<ShardStats> {
        let table_set = self.table_set_stats.lock().expect("poisoned");
        table_set.tables.values().filter_map(|v| v.shards.get(&shard_id)).next().cloned()
    }

    pub fn reset(&self) {
        {
            let mut inner = self.sched_stats.lock().unwrap();
            inner.raw_group_delta.clear();
            inner.node_view.clear();
            inner.split_shards.clear();
            inner.moving_shards.clear();
            inner.merge_shards.clear();
            inner.split_retry_at.clear();
        }
        {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta.clear();
        }
        {
            let mut inner = self.table_set_stats.lock().expect("poisoned");
            inner.tables.clear();
        }
        self.show_stats.lock().unwrap().clear();
        self.node_resources.lock().unwrap().clear();
        self.replica_progress.lock().unwrap().clear();
        self.replica_reports.lock().unwrap().clear();
    }
}

impl SchedStats {
    fn scheduled_shards(&self) -> HashSet<u64> {
        let mut shards = self.split_shards.clone();
        shards.extend(self.moving_shards.keys().copied());
        for (left, right) in &self.merge_shards {
            shards.insert(*left);
            shards.insert(*right);
        }
        shards
    }

    fn replace_state(&mut self, updates: &[ScheduleState], requested_at: Option<Instant>) -> bool {
        let mut updated = false;
        let updated_at = requested_at.unwrap_or_else(Instant::now);
        for state in updates {
            let replacement = |started_at| GroupDelta {
                epoch: state.epoch,
                updated_at,
                started_at,
                incoming: state.incoming_replicas.to_owned(),
                outgoing: state.outgoing_replicas.to_owned(),
                repair: state.replica_repair.clone(),
                reason: None,
            };
            match self.raw_group_delta.entry(state.group_id) {
                hash_map::Entry::Occupied(mut ent) => {
                    let delta = ent.get_mut();
                    if requested_at.is_some_and(|requested| requested <= delta.updated_at) {
                        continue;
                    }
                    let active = !state.incoming_replicas.is_empty()
                        || !state.outgoing_replicas.is_empty()
                        || state.replica_repair.is_some();
                    let started = active && !delta.has_activity();
                    let settled = !active && delta.has_activity() && requested_at.is_some();
                    let learned_repair = delta.repair.is_none() && state.replica_repair.is_some();
                    if delta.epoch < state.epoch
                        || (delta.epoch == state.epoch && (started || settled || learned_repair))
                    {
                        let started_at = if active && delta.has_activity() {
                            delta.started_at
                        } else {
                            updated_at
                        };
                        let mut next = replacement(started_at);
                        if delta.reason.is_some() && delta.replica_move() == next.replica_move() {
                            next.reason = delta.reason.take();
                        }
                        *delta = next;
                        updated = true;
                    }
                }
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(replacement(updated_at));
                    updated = true;
                }
            }
        }
        updated
    }

    fn rebuild_view(&mut self) {
        let mut new_node_view: HashMap<u64, NodeDelta> = HashMap::new();
        for r in self.raw_group_delta.values() {
            for incoming in &r.incoming {
                match new_node_view.entry(incoming.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count += 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: 1 });
                    }
                }
            }
            for outgoing in &r.outgoing {
                match new_node_view.entry(outgoing.node_id) {
                    hash_map::Entry::Occupied(mut ent) => ent.get_mut().replica_count -= 1,
                    hash_map::Entry::Vacant(ent) => {
                        ent.insert(NodeDelta { replica_count: -1 });
                    }
                }
            }
        }
        self.node_view = new_node_view;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BalanceDimension;
    use crate::root::schedule::Policy;

    #[test]
    fn replica_move_keeps_its_reason_only_for_the_same_change() {
        let stats = ClusterStats::default();
        let mut moving = ScheduleState {
            group_id: 1,
            epoch: 42,
            incoming_replicas: vec![ReplicaDesc { id: 1, node_id: 2, ..Default::default() }],
            outgoing_replicas: vec![ReplicaDesc { id: 2, node_id: 3, ..Default::default() }],
            ..Default::default()
        };
        let reason = TaskReason {
            policy: Some(Policy::Placement(BalanceDimension::ReplicaCount)),
            detail: "Source node is above the upper threshold".into(),
        };
        stats.handle_schedule_update(std::slice::from_ref(&moving), None);
        assert!(stats.replica_changes()[0].reason.is_none());
        stats.handle_replica_move_update(moving.clone(), 1, 2, &reason);

        moving.epoch += 1;
        stats.handle_heartbeat_schedule_update(
            std::slice::from_ref(&moving),
            Instant::now() + std::time::Duration::from_secs(1),
        );
        let changes = stats.replica_changes();
        let displayed = changes[0].reason.as_ref().unwrap();
        assert_eq!(displayed.policy.unwrap().name(), "replica_count");
        assert_eq!(displayed.detail, reason.detail);

        moving.epoch += 1;
        moving.incoming_replicas[0].id = 3;
        stats.handle_schedule_update(&[moving], None);
        assert!(stats.replica_changes()[0].reason.is_none());
    }

    #[test]
    fn guarded_migration_waits_for_a_destination_sample_requested_after_submission() {
        let stats = ClusterStats::default();
        let reason = TaskReason {
            policy: Some(Policy::Placement(BalanceDimension::ShardSize)),
            detail: "Reduces the size imbalance between groups".into(),
        };
        stats.handle_move_shard(1, 1, 2, Some(reason.clone()));
        let queued = stats.shard_migration_wait(&[]).unwrap();
        assert!(queued.contains("Shard 1 g1 -> g2 is queued"), "{queued}");
        let submitted = Instant::now();
        stats.mark_shard_move_submitted(1);
        let waiting = stats.shard_migration_wait(&[]).unwrap();
        assert!(waiting.contains("Waiting for g2 to confirm completion"), "{waiting}");
        let moves = stats.shard_moves();
        assert_eq!(moves[0].source, 1);
        let displayed = moves[0].reason.as_ref().unwrap();
        assert_eq!(displayed.policy.unwrap().name(), "shard_size");
        assert_eq!(displayed.detail, reason.detail);
        let mut sample = GroupStats {
            group_id: 2,
            sample: Some(GroupStatsSample {
                shard_migration_in_progress: Some(false),
                ..Default::default()
            }),
            ..Default::default()
        };
        stats.confirm_shard_moves(&sample, submitted);
        assert!(stats.has_pending_shard_moves());

        let later = Instant::now() + std::time::Duration::from_secs(1);
        sample.sample.as_mut().unwrap().shard_migration_in_progress = Some(true);
        stats.confirm_shard_moves(&sample, later);
        assert!(stats.has_pending_shard_moves());

        sample.sample.as_mut().unwrap().shard_migration_in_progress = Some(false);
        // An accepted migration may roll back without ever reporting the shard at the
        // destination.
        stats.confirm_shard_moves(&sample, later);
        assert!(!stats.has_pending_shard_moves());
        assert!(stats.shard_migration_wait(&[]).is_none());
    }

    #[test]
    fn removal_only_replica_repair_is_visible_until_it_settles() {
        let stats = ClusterStats::default();
        let earlier_heartbeat = Instant::now();
        let outgoing = ReplicaDesc { id: 8, node_id: 3, ..Default::default() };
        let idle = ScheduleState { group_id: 2, epoch: 3, ..Default::default() };
        let repair = ReplicaRepair { incoming_replicas: vec![], outgoing_replicas: vec![outgoing] };
        stats.handle_schedule_update(
            &[ScheduleState { replica_repair: Some(repair.clone()), ..idle.clone() }],
            None,
        );
        stats.handle_heartbeat_schedule_update(std::slice::from_ref(&idle), earlier_heartbeat);
        let changes = stats.replica_changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].group, 2);
        assert!(changes[0].repair);
        assert_eq!(changes[0].change, repair);
        assert!(!stats.has_pending_replica_changes());
        stats.handle_heartbeat_schedule_update(
            &[idle],
            earlier_heartbeat + std::time::Duration::from_secs(1),
        );
        assert!(stats.replica_changes().is_empty());
    }

    #[test]
    fn replica_health_uses_current_node_reports_and_membership() {
        let stats = ClusterStats::default();
        let max_age = std::time::Duration::from_secs(60);
        let liveness = Liveness::new(max_age);
        let mut group = GroupDesc {
            id: 7,
            replicas: (1..=5)
                .map(|id| ReplicaDesc { id, node_id: id, role: ReplicaRole::Voter as i32 })
                .collect(),
            ..Default::default()
        };
        group.replicas[1].role = ReplicaRole::Learner as i32;
        let nodes: Vec<_> = (1..=4)
            .map(|id| NodeDesc { id, status: NodeStatus::Active as i32, ..Default::default() })
            .collect();
        stats.handle_replica_report(3, &[]);
        stats.handle_replica_report(
            4,
            &[ReplicaState { group_id: 7, replica_id: 4, term: 0, ..Default::default() }],
        );
        let issues = stats.replica_issues(&[group], &nodes, &liveness, max_age);
        assert_eq!(
            issues.iter().map(|issue| (issue.node, issue.reason)).collect::<Vec<_>>(),
            [
                (2, "learner"),
                (3, "missing from latest heartbeat"),
                (4, "Raft uninitialized"),
                (5, "node missing"),
            ]
        );
    }
}
