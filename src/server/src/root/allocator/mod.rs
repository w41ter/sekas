// Copyright 2022 The Engula Authors.
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

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use sekas_api::server::v1::{GroupDesc, NodeDesc, NodeStatus, ReplicaRole};

use super::liveness::Liveness;
use super::{ClusterStats, RootShared};
use crate::Result;

#[cfg(test)]
mod sim_test;

mod balance;
mod placement;

pub(super) use placement::{CapacityAction, PlacementAction, PlacementPriority, plan_round};

#[derive(Debug, Clone, Copy)]
pub struct TransferLeader {
    pub group: u64,
    pub src_node: u64,
    pub src_replica: u64,
    pub target_node: u64,
    pub target_replica: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReallocateReplica {
    pub group: u64,
    pub source_node: u64,
    pub source_replica: u64,
    pub target_node: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReallocateShard {
    pub shard: u64,
    pub source_group: u64,
    pub target_group: u64,
    /// Split first and replan placement once the new range is visible.
    pub split_bytes: Option<u64>,
}

#[derive(Clone)]
pub(super) enum BalanceDecision {
    Unknown,
    Disabled,
    Deferred(Cow<'static, str>),
    Blocked(String),
    NoAction(String),
    ActionNeeded(String),
    Error(String),
}

#[derive(Clone, Copy)]
pub(in crate::root) struct BalanceThresholds {
    pub avg: f64,
    pub low: f64,
    pub high: f64,
    pub max: f64,
    pub tolerance: f64,
}

impl BalanceThresholds {
    fn new(avg: f64, tolerance: f64, max: f64) -> Self {
        Self { avg, low: (avg - tolerance).max(0.), high: avg + tolerance, max, tolerance }
    }
}

pub(super) struct Allocator {
    root: Arc<RootShared>,
    liveness: Arc<Liveness>,
    cluster_stats: Arc<ClusterStats>,
    replicas_per_group: usize,
}

impl Allocator {
    pub fn new(
        root: Arc<RootShared>,
        liveness: Arc<Liveness>,
        cluster_stats: Arc<ClusterStats>,
        replicas_per_group: usize,
    ) -> Self {
        Self { root, liveness, cluster_stats, replicas_per_group }
    }

    pub fn replicas_per_group(&self) -> usize {
        self.replicas_per_group
    }

    pub(super) fn schedulable(&self, node: &NodeDesc) -> bool {
        node.status == NodeStatus::Active as i32 && !self.liveness.get(&node.id).is_dead()
    }

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        let mut candidates = self.root.schema()?.list_node().await?;
        candidates
            .retain(|node| self.schedulable(node) && !existing_replica_nodes.contains(&node.id));
        candidates.sort_by_cached_key(|node| {
            let reported = node.capacity.as_ref().map_or(0, |capacity| capacity.replica_count);
            let expected = reported
                .saturating_add_signed(self.cluster_stats.get_node_delta(node.id).replica_count);
            (expected, node.id)
        });
        Ok(candidates.into_iter().take(wanted_count).collect())
    }

    /// Select ready groups for a new shard, preferring fewer existing ranges.
    pub async fn place_group_for_shard(
        &self,
        n: usize,
        required_replicas: usize,
        unpublished: &HashSet<u64>,
    ) -> Result<Vec<GroupDesc>> {
        let schema = self.root.schema()?;
        let available: HashSet<_> = schema
            .list_node()
            .await?
            .iter()
            .filter(|node| self.schedulable(node))
            .map(|node| node.id)
            .collect();
        let mut groups: Vec<_> = schema
            .list_group()
            .await?
            .into_iter()
            .filter(|group| {
                group.id != crate::constants::ROOT_GROUP_ID
                    && !unpublished.contains(&group.id)
                    && group.replicas.len() == required_replicas
                    && group.replicas.iter().all(|replica| {
                        replica.role() == ReplicaRole::Voter && available.contains(&replica.node_id)
                    })
            })
            .collect();
        groups.sort_unstable_by_key(|group| (group.shards.len(), group.id));
        groups.truncate(n);
        Ok(groups)
    }
}
