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

mod show;

pub(super) use show::{ShowMetrics, ShowSnapshot};

struct GroupDelta {
    epoch: u64,
    incoming: Vec<ReplicaDesc>,
    outgoing: Vec<ReplicaDesc>,
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
    destination: u64,
    submitted_at: Option<Instant>,
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
                table_stats.shards.insert(shard_id, *shard);
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

    pub fn handle_schedule_update(
        &self,
        state_updates: &[ScheduleState],
        job_updates: Option<HashMap<u64 /* node */, NodeDelta>>,
    ) {
        if !state_updates.is_empty() {
            let mut inner = self.sched_stats.lock().unwrap();
            if inner.replace_state(state_updates) {
                inner.rebuild_view();
            }
        }
        if let Some(job_updates) = job_updates.as_ref() {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta.clone_from(job_updates);
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

    pub fn handle_move_shard(&self, shard_id: u64, dest_group_id: u64) {
        let mut sched_stats = self.sched_stats.lock().expect("poisoned");
        sched_stats
            .moving_shards
            .insert(shard_id, ShardMove { destination: dest_group_id, submitted_at: None });
    }

    pub(super) fn mark_shard_move_submitted(&self, shard_id: u64) {
        if let Some(migration) = self.sched_stats.lock().unwrap().moving_shards.get_mut(&shard_id) {
            migration.submitted_at = Some(Instant::now());
        }
    }

    pub(super) fn has_pending_shard_moves(&self) -> bool {
        !self.sched_stats.lock().unwrap().moving_shards.is_empty()
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

    fn replace_state(&mut self, updates: &[ScheduleState]) -> bool {
        let mut updated = false;
        for state in updates {
            match self.raw_group_delta.entry(state.group_id) {
                hash_map::Entry::Occupied(mut ent) => {
                    let delta = ent.get_mut();
                    if delta.epoch < state.epoch {
                        *delta = GroupDelta {
                            epoch: state.epoch,
                            incoming: state.incoming_replicas.to_owned(),
                            outgoing: state.outgoing_replicas.to_owned(),
                        };
                        updated = true;
                    }
                }
                hash_map::Entry::Vacant(ent) => {
                    ent.insert(GroupDelta {
                        epoch: state.epoch,
                        incoming: state.incoming_replicas.to_owned(),
                        outgoing: state.outgoing_replicas.to_owned(),
                    });
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

    #[test]
    fn guarded_migration_waits_for_a_destination_sample_requested_after_submission() {
        let stats = ClusterStats::default();
        stats.handle_move_shard(1, 2);
        let submitted = Instant::now();
        stats.mark_shard_move_submitted(1);
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
    }
}
