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

use std::sync::Arc;
use std::time::Duration;

use sekas_api::server::v1::{GroupDesc, NodeDesc};

use self::policy_leader_cnt::LeaderCountPolicy;
use self::policy_replica_cnt::ReplicaCountPolicy;
use self::policy_shard_cnt::ShardCountPolicy;
use self::policy_shard_size::ShardSizeBalance;
use self::source::NodeFilter;
use super::{ClusterStats, RootShared, metrics};
use crate::{Result, RootConfig};

#[cfg(test)]
mod sim_test;

mod policy_leader_cnt;
mod policy_replica_cnt;
mod policy_shard_cnt;
mod policy_shard_size;
mod source;

pub use source::{AllocSource, SysAllocSource};

#[derive(Clone, Debug)]
pub enum GroupAction {
    Noop,
    Add(usize),
    #[allow(dead_code)]
    Remove(Vec<u64>),
}

#[derive(Clone, Debug)]
pub enum ReplicaAction {
    Migrate(ReallocateReplica),
}

#[derive(Clone, Debug)]
pub enum ShardAction {
    Migrate(ReallocateShard),
}

pub(super) enum ShardSizePlacement {
    Move,
    Split(u64),
}

pub(super) struct ShardBalancePlan {
    pub actions: Vec<ShardAction>,
    pub size: BalanceDecision,
    pub count: BalanceDecision,
}

impl ShardBalancePlan {
    fn idle(cfg: &RootConfig, decision: BalanceDecision) -> Self {
        let status = |enabled| if enabled { decision.clone() } else { BalanceDecision::Disabled };
        Self {
            actions: Vec::new(),
            size: status(cfg.enable_shard_size_balance),
            count: status(cfg.enable_shard_balance),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LeaderAction {
    Noop,
    Shed(TransferLeader),
}

#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub group: u64,
    pub src_node: u64,
    #[allow(dead_code)]
    pub src_replica: u64,
    pub target_node: u64,
    pub target_replica: u64,
}

#[derive(Clone, Debug)]
pub struct ReallocateReplica {
    pub group: u64,
    pub source_node: u64,
    pub source_replica: u64,
    pub target_node: NodeDesc,
}

#[derive(Clone, Debug)]
pub struct ReallocateShard {
    pub shard: u64,
    pub source_group: u64,
    pub target_group: u64,
}

#[derive(PartialEq, Eq, Debug)]
enum BalanceStatus {
    Overfull,
    Balanced,
    Underfull,
}

#[derive(Clone)]
pub(super) enum BalanceDecision {
    Unknown,
    Disabled,
    Deferred(&'static str),
    NoAction(String),
    ActionNeeded(String),
    Unsupported(String),
    Error(String),
}

// Use the same counts and tolerance to choose a move and explain a no-op.
struct CountBalance {
    mean: f64,
    tolerance: f64,
}

impl CountBalance {
    fn new<T>(candidates: &[(T, u64)], tolerance: impl FnOnce(f64) -> f64) -> Self {
        let total = candidates.iter().map(|(_, count)| count).sum::<u64>();
        let mean = total as f64 / candidates.len().max(1) as f64;
        Self { mean, tolerance: tolerance(mean) }
    }

    fn classify(&self, count: u64) -> BalanceStatus {
        let count = count as f64;
        if count > self.mean + self.tolerance {
            BalanceStatus::Overfull
        } else if count < self.mean - self.tolerance {
            BalanceStatus::Underfull
        } else {
            BalanceStatus::Balanced
        }
    }

    // The candidates are sorted by descending count.
    fn no_action<T>(&self, scope: &str, candidates: &[(T, u64)]) -> BalanceDecision {
        let (Some((_, max)), Some((_, min))) = (candidates.first(), candidates.last()) else {
            return BalanceDecision::NoAction(format!(
                "{scope}=0, tolerance={:.2}",
                self.tolerance
            ));
        };
        let overfull = candidates
            .iter()
            .take_while(|(_, count)| self.classify(*count) == BalanceStatus::Overfull)
            .count();
        let underfull = candidates
            .iter()
            .rev()
            .take_while(|(_, count)| self.classify(*count) == BalanceStatus::Underfull)
            .count();
        let reason = match (overfull, underfull) {
            (0, _) => "no overfull source",
            (_, 0) => "no underfull target",
            _ => "no eligible move",
        };
        BalanceDecision::NoAction(format!(
            "{reason}\n{scope}={}, mean={:.2}, tolerance={:.2}\nrange={min}..{max}, overfull={overfull}, underfull={underfull}",
            candidates.len(),
            self.mean,
            self.tolerance,
        ))
    }
}

#[derive(Clone)]
pub struct Allocator<T: AllocSource> {
    alloc_source: Arc<T>,
    cluster_stats: Arc<ClusterStats>,
    config: RootConfig,
}

impl<T: AllocSource> Allocator<T> {
    pub fn new(alloc_source: Arc<T>, cluster_stats: Arc<ClusterStats>, config: RootConfig) -> Self {
        Self { alloc_source, config, cluster_stats }
    }

    pub fn replicas_per_group(&self) -> usize {
        self.config.replicas_per_group
    }

    pub async fn refresh_for_balance(&self) -> Result<()> {
        if self.config.enable_group_balance
            || self.config.enable_replica_balance
            || self.config.enable_leader_balance
            || self.config.enable_shard_balance
            || self.config.enable_shard_size_balance
        {
            self.alloc_source.refresh_all().await?;
        }
        Ok(())
    }

    /// Compute group change action.
    pub fn compute_group_action(&self) -> (GroupAction, BalanceDecision) {
        if !self.config.enable_group_balance {
            return (GroupAction::Noop, BalanceDecision::Disabled);
        }

        let nodes = self.alloc_source.nodes(NodeFilter::NotDecommissioned);
        let node_count = nodes.len();
        let min_nodes = self.config.replicas_per_group;
        let current_groups = self.current_groups();
        if node_count < min_nodes {
            return (
                GroupAction::Noop,
                BalanceDecision::NoAction(format!(
                    "too few nodes. nodes={node_count}, min_nodes={min_nodes}, groups={current_groups}"
                )),
            );
        }

        let total_cpus = nodes
            .iter()
            .filter_map(|n| n.capacity.as_ref())
            .map(|capacity| capacity.cpu_nums)
            .sum::<f64>();
        // Size the group pool for one leader per CPU, with at least one group per node.
        let desired_groups = (total_cpus as usize).max(node_count);
        let inputs = format!(
            "nodes={node_count}, min_nodes={min_nodes}, cpus={total_cpus}, groups={current_groups}, desired_groups={desired_groups}"
        );
        match current_groups.cmp(&desired_groups) {
            std::cmp::Ordering::Less => {
                let add = desired_groups - current_groups;
                (
                    GroupAction::Add(add),
                    BalanceDecision::ActionNeeded(format!("create {add} groups\n{inputs}")),
                )
            }
            std::cmp::Ordering::Greater => {
                let want_remove = current_groups - desired_groups;
                (
                    GroupAction::Remove(self.preferred_remove_groups(want_remove)),
                    BalanceDecision::Unsupported(format!("group removal is unsupported\n{inputs}")),
                )
            }
            std::cmp::Ordering::Equal => (GroupAction::Noop, BalanceDecision::NoAction(inputs)),
        }
    }

    /// Compute replica change action.
    pub fn compute_replica_action(&self) -> (Vec<ReplicaAction>, BalanceDecision) {
        if !self.config.enable_replica_balance {
            return (vec![], BalanceDecision::Disabled);
        }

        // TODO: try QPS rebalancing.
        ReplicaCountPolicy::with(self.alloc_source.clone(), self.cluster_stats.clone())
            .compute_balance()
    }

    pub fn compute_shard_action(&self) -> ShardBalancePlan {
        let plan = self.plan_shard_balance();
        for (decision, gauge) in [
            (&plan.size, &metrics::RECONCILE_ALREADY_BALANCED_INFO.group_shard_size),
            (&plan.count, &metrics::RECONCILE_ALREADY_BALANCED_INFO.group_shard_count),
        ] {
            if !matches!(decision, BalanceDecision::Disabled) {
                gauge.set(i64::from(matches!(decision, BalanceDecision::NoAction(_))));
            }
        }
        plan
    }

    fn plan_shard_balance(&self) -> ShardBalancePlan {
        let cfg = &self.config;
        if !cfg.enable_shard_balance && !cfg.enable_shard_size_balance {
            return ShardBalancePlan::idle(cfg, BalanceDecision::Disabled);
        }
        let node_count = self.alloc_source.nodes(NodeFilter::All).len();
        let min_nodes = cfg.replicas_per_group;
        if node_count < min_nodes {
            return ShardBalancePlan::idle(
                cfg,
                BalanceDecision::NoAction(format!(
                    "too few nodes. nodes={node_count}, min_nodes={min_nodes}"
                )),
            );
        }
        let policy = ShardCountPolicy::with(self.alloc_source.clone());
        let groups = policy.current_user_groups();
        let mut plan =
            ShardBalancePlan::idle(cfg, BalanceDecision::Deferred("shard size takes priority"));
        let size_balance = if cfg.enable_shard_size_balance && groups.len() >= 2 {
            if self.cluster_stats.has_pending_shard_moves() {
                return ShardBalancePlan::idle(
                    cfg,
                    BalanceDecision::Deferred("waiting for a shard migration to settle"),
                );
            }
            let balance = match ShardSizeBalance::new(
                &groups,
                &self.cluster_stats,
                Duration::from_secs(cfg.liveness_threshold_sec),
                cfg.shard_size_balance_tolerance_bytes,
                cfg.move_shard_size_limit,
            ) {
                Ok(balance) => balance,
                Err(decision) => return ShardBalancePlan::idle(cfg, decision),
            };
            let (action, decision) =
                balance.compute_balance(&groups, &self.cluster_stats, cfg.enable_auto_shard_split);
            plan.size = decision;
            if let Some(action) = action {
                plan.actions.push(ShardAction::Migrate(action));
                return plan;
            }
            Some(balance)
        } else {
            if cfg.enable_shard_size_balance {
                plan.size = BalanceDecision::NoAction(format!("user_groups={}", groups.len()));
            }
            None
        };

        if cfg.enable_shard_balance {
            (plan.actions, plan.count) = policy.compute_balance(
                &groups,
                &self.cluster_stats,
                cfg.move_shard_size_limit,
                |source, target, shard| {
                    size_balance.as_ref().is_none_or(|balance| {
                        balance.preserves_balance(source.id, target.id, shard.id)
                    })
                },
            );
        }
        plan
    }

    pub fn validate_shard_balance_move(
        &self,
        groups: &[GroupDesc],
        action: &ReallocateShard,
        require_improvement: bool,
    ) -> Option<ShardSizePlacement> {
        let cfg = &self.config;
        let balance = ShardSizeBalance::new(
            groups,
            &self.cluster_stats,
            Duration::from_secs(cfg.liveness_threshold_sec),
            cfg.shard_size_balance_tolerance_bytes,
            cfg.move_shard_size_limit,
        )
        .ok()?;
        balance.validate_placement(action, require_improvement, cfg.enable_auto_shard_split)
    }

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replica_nodes: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        self.alloc_source.refresh_all().await?;

        ReplicaCountPolicy::with(self.alloc_source.to_owned(), self.cluster_stats.to_owned())
            .allocate_group_replica(existing_replica_nodes, wanted_count)
    }

    /// Find a group to place shard.
    pub async fn place_group_for_shard(
        &self,
        n: usize,
        required_replicas: usize,
    ) -> Result<Vec<GroupDesc>> {
        self.alloc_source.refresh_all().await?;

        ShardCountPolicy::with(self.alloc_source.to_owned()).allocate_shard(n, required_replicas)
    }

    pub fn compute_leader_action(&self) -> (Vec<LeaderAction>, BalanceDecision) {
        if !self.config.enable_leader_balance {
            return (vec![], BalanceDecision::Disabled);
        }
        let (action, decision) =
            LeaderCountPolicy::with(self.alloc_source.clone()).compute_balance();
        let actions = match action {
            LeaderAction::Noop => vec![],
            action => vec![action],
        };
        (actions, decision)
    }
}

impl<T: AllocSource> Allocator<T> {
    fn preferred_remove_groups(&self, want_remove: usize) -> Vec<u64> {
        // TODO:
        // 1 remove groups from unreachable nodes that indicated by NodeLiveness(they
        // also need repair replicas).
        // 2 remove groups from unmatched cpu-quota nodes.
        // 3. remove groups with lowest moving shard cost.
        self.alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .take(want_remove)
            .map(|n| n.id)
            .collect()
    }

    fn current_groups(&self) -> usize {
        self.alloc_source.groups().len()
    }
}

// Allocate Group's replica between nodes.
impl<T: AllocSource> Allocator<T> {}

// Allocate Group leader replica.
impl<T: AllocSource> Allocator<T> {}
