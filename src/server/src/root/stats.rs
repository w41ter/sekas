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

use std::collections::{hash_map, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use sekas_api::server::v1::*;

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
    group_set_stats: Arc<Mutex<HashMap<u64, GroupStats>>>,
}

#[derive(Default)]
struct SchedStats {
    raw_group_delta: HashMap<u64 /* group */, GroupDelta>,
    node_view: HashMap<u64 /* node */, NodeDelta>,
    split_shards: HashSet<u64>,
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
    pub fn handle_group_stats(&self, group_stats: GroupStats) {
        {
            let mut table_set = self.table_set_stats.lock().expect("poisoned");
            for shard in &group_stats.shard_stats {
                let shard_id = shard.shard_id;
                let table_stats = table_set.tables.entry(shard.table_id).or_default();
                table_stats.shards.insert(shard_id, shard.clone());
                table_stats.shard_indexes.insert(shard_id, group_stats.group_id);
            }
        }
        {
            let mut group_set = self.group_set_stats.lock().expect("poisoned");
            group_set.insert(group_stats.group_id, group_stats);
        }
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
    pub fn get_large_shards(&self, limit: usize) -> Vec<(u64, u64)> {
        const SPLIT_THRESHOLD: u64 = 64 * 1024 * 1024;
        let in_spliting = { self.sched_stats.lock().expect("poisoned").split_shards.clone() };
        let table_set = self.table_set_stats.lock().expect("poisoned");
        let mut target_shards = Vec::with_capacity(limit);
        for table_stats in table_set.tables.values() {
            for shard_stats in table_stats.shards.values() {
                if shard_stats.shard_size < SPLIT_THRESHOLD
                    || in_spliting.contains(&shard_stats.shard_id)
                {
                    continue;
                }
                if let Some(&group_id) = table_stats.shard_indexes.get(&shard_stats.shard_id) {
                    target_shards.push((group_id, shard_stats.shard_id));
                    if target_shards.len() >= limit {
                        break;
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

    /// Get the stats of a group.
    pub fn get_group_stats(&self, group_id: u64) -> Option<GroupStats> {
        let group_set = self.group_set_stats.lock().expect("poisoned");
        group_set.get(&group_id).cloned()
    }

    pub fn reset(&self) {
        {
            let mut inner = self.sched_stats.lock().unwrap();
            inner.raw_group_delta.clear();
            inner.node_view.clear();
            inner.split_shards.clear();
        }
        {
            let mut inner = self.job_stats.lock().unwrap();
            inner.node_delta.clear();
        }
        {
            let mut inner = self.table_set_stats.lock().expect("poisoned");
            inner.tables.clear();
        }
    }
}

impl SchedStats {
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
