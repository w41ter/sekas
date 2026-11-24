// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use sekas_api::server::v1::{GroupDesc, NodeResourceUsage, ReplicaRole, ShardDesc};

use super::balance::{Axis, Change};
use super::{
    BalanceDecision, BalanceThresholds, ReallocateReplica, ReallocateShard, TransferLeader,
};
use crate::constants::ROOT_GROUP_ID;
use crate::root::ClusterStats;
use crate::root::schedule::ClusterView;
use crate::root::stats::ShowSnapshot;
use crate::{BalanceConfig, BalanceDimension as Dimension, RootConfig};

#[derive(Clone, Copy)]
pub(in crate::root) enum PlacementAction {
    Shard(ReallocateShard),
    Leader(TransferLeader),
    Replica(ReallocateReplica),
}

impl PlacementAction {
    fn split_bytes(&self) -> Option<u64> {
        match self {
            Self::Shard(action) => action.split_bytes,
            _ => None,
        }
    }

    fn detail(&self) -> String {
        match self {
            Self::Shard(move_shard) => format!(
                "move shard {} from g{} to g{}",
                move_shard.shard, move_shard.source_group, move_shard.target_group,
            ),
            Self::Leader(transfer) => format!(
                "transfer leader of group {} from n{} to n{}",
                transfer.group, transfer.src_node, transfer.target_node,
            ),
            Self::Replica(migration) => format!(
                "move replica {} of group {} from n{} to n{}",
                migration.source_replica,
                migration.group,
                migration.source_node,
                migration.target_node,
            ),
        }
    }

    fn tie_breaker(&self) -> (u8, u64, u64, u64) {
        match self {
            Self::Leader(a) => (0, a.group, a.src_node, a.target_node),
            Self::Shard(a) => (1, a.shard, a.source_group, a.target_group),
            Self::Replica(a) => (2, a.group, a.source_node, a.target_node),
        }
    }
}

pub(in crate::root) struct PlacementPlan {
    pub choice: Option<PlacementChoice>,
    pub decisions: Vec<(Dimension, BalanceDecision, Option<BalanceThresholds>)>,
}

pub(in crate::root) struct PlacementChoice {
    pub dimension: Dimension,
    pub action: PlacementAction,
    pub priority: PlacementPriority,
    pub reason: String,
}

pub(in crate::root) struct CapacityChoice {
    pub action: CapacityAction,
    pub priority: PlacementPriority,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::root) enum PlacementPriority {
    Imbalance,
    PlanningWatermark,
    LoadDeadline,
    CapacityLimit,
}

impl CapacityChoice {
    fn new(action: CapacityAction, over_limit: bool) -> Self {
        let priority = if over_limit {
            PlacementPriority::CapacityLimit
        } else {
            PlacementPriority::PlanningWatermark
        };
        Self { action, priority }
    }
}

pub(in crate::root) enum CapacityAction {
    CreateGroup,
    Place(ReallocateShard),
}

struct ShardTarget {
    group: u64,
    bytes: u64,
    move_budget: u64,
}

impl ShardTarget {
    fn fit(
        &self,
        source_group: u64,
        shard: u64,
        bytes: u64,
        split_goal: Option<u64>,
    ) -> Option<ReallocateShard> {
        let amount = movable_bytes(bytes, self.move_budget, split_goal)?;
        Some(ReallocateShard {
            shard,
            source_group,
            target_group: self.group,
            split_bytes: (amount < bytes).then_some(amount),
        })
    }
}

struct Placement<'a> {
    view: &'a ClusterView,
    config: &'a RootConfig,
    groups: &'a [GroupDesc],
    resources: HashMap<u64, NodeResourceUsage>,
    group_sizes: HashMap<u64, u64>,
    storage_sizes: HashMap<u64, u64>,
    serving_units: HashMap<u64, Option<f64>>,
    locality: Locality,
    axes: Vec<Axis>,
    overlapping_groups: HashSet<u64>,
    shard_targets: Vec<ShardTarget>,
}

type Changes = [Change; Dimension::ALL.len()];

#[derive(Default)]
struct Locality {
    neighbors: HashMap<u64, Vec<Neighbor>>,
    hot: HashMap<u64, bool>,
}

struct Neighbor {
    shard: u64,
    group: u64,
    weight: i32,
}

impl Locality {
    fn new(groups: &[GroupDesc], snapshot: &ShowSnapshot, cfg: &BalanceConfig) -> Self {
        let mut locality = Self::default();
        let mut ranges: HashMap<u64, Vec<(u64, &ShardDesc)>> = HashMap::new();
        for group in groups {
            for shard in &group.shards {
                if shard.range.is_some() {
                    ranges.entry(shard.table_id).or_default().push((group.id, shard));
                }
                if snapshot.leader(group.id).is_none() {
                    continue;
                }
                let Some(rates) = snapshot.shard(group.id, shard.id).rates else { continue };
                if let Some((read, write)) = rates.read_units_per_sec.zip(rates.write_units_per_sec)
                {
                    locality.hot.insert(
                        shard.id,
                        read >= cfg.hot_read_units_per_sec || write >= cfg.hot_write_units_per_sec,
                    );
                }
            }
        }
        for ranges in ranges.values_mut() {
            ranges.sort_unstable_by(|(_, a), (_, b)| {
                a.range
                    .as_ref()
                    .unwrap()
                    .start
                    .cmp(&b.range.as_ref().unwrap().start)
                    .then(a.id.cmp(&b.id))
            });
            for (offset, &(_, shard)) in ranges.iter().enumerate() {
                let start = offset.saturating_sub(2);
                let end = (offset + 3).min(ranges.len());
                let neighbors = ranges[start..end]
                    .iter()
                    .enumerate()
                    .filter_map(|(index, &(group, neighbor))| {
                        let distance = (start + index).abs_diff(offset);
                        (distance != 0).then_some(Neighbor {
                            shard: neighbor.id,
                            group,
                            weight: 3 - distance as i32,
                        })
                    })
                    .collect();
                locality.neighbors.insert(shard.id, neighbors);
            }
        }
        locality
    }

    fn score(&self, movement: &ReallocateShard) -> i32 {
        let Some(&hot) = self.hot.get(&movement.shard) else { return 0 };
        self.neighbors
            .get(&movement.shard)
            .into_iter()
            .flatten()
            .filter(|neighbor| self.hot.get(&neighbor.shard) == Some(&hot))
            .map(|neighbor| {
                let joins = i32::from(neighbor.group == movement.target_group);
                let leaves = i32::from(neighbor.group == movement.source_group);
                let direction = if hot { -1 } else { 1 };
                direction * (joins - leaves) * neighbor.weight
            })
            .sum()
    }
}

struct RankedAction {
    action: PlacementAction,
    rank: usize,
    score: (f64, f64, bool),
    locality: i32,
}

impl RankedAction {
    fn compare(&self, other: &Self) -> Ordering {
        (self.score.0 > 0.)
            .cmp(&(other.score.0 > 0.))
            .then(other.rank.cmp(&self.rank))
            .then(
                matches!(other.action, PlacementAction::Replica(_))
                    .cmp(&matches!(self.action, PlacementAction::Replica(_))),
            )
            .then(self.action.split_bytes().is_none().cmp(&other.action.split_bytes().is_none()))
            .then_with(|| self.score.0.total_cmp(&other.score.0))
            .then_with(|| self.score.1.total_cmp(&other.score.1))
            .then(self.score.2.cmp(&other.score.2))
            .then(self.locality.cmp(&other.locality))
            .then_with(|| other.action.tie_breaker().cmp(&self.action.tie_breaker()))
    }
}

impl<'a> Placement<'a> {
    fn new(
        view: &'a ClusterView,
        config: &'a RootConfig,
        options: &BalanceConfig,
        stats: &ClusterStats,
    ) -> Self {
        let groups = &view.groups[view.groups.partition_point(|group| group.id == ROOT_GROUP_ID)..];
        let snapshot = &view.samples;
        let resources = stats.node_resources(Duration::from_secs(config.liveness_threshold_sec));
        let mut group_sizes = HashMap::with_capacity(groups.len());
        let mut storage_sizes = HashMap::<u64, u64>::new();
        let mut serving_units: HashMap<_, _> =
            view.available.iter().map(|&node| (node, Some(0.))).collect();
        let mut seen = HashMap::new();
        let mut overlapping_groups = HashSet::new();
        for group in groups {
            let mut known_size = 0_u64;
            for shard in &group.shards {
                if let Some(previous) = seen.insert(shard.id, group.id) {
                    overlapping_groups.extend([previous, group.id]);
                }
                known_size = known_size
                    .saturating_add(snapshot.shard(group.id, shard.id).bytes.unwrap_or(0));
            }
            let sample = snapshot.group(group);
            if let Some(size) = sample.bytes {
                group_sizes.insert(group.id, size);
            }
            for replica in &group.replicas {
                let total = storage_sizes.entry(replica.node_id).or_default();
                *total = total.saturating_add(known_size);
            }

            if let Some(leader) = view.samples.leader(group.id) {
                if let Some(total) = serving_units.get_mut(&leader.node_id) {
                    let units = sample
                        .rates
                        .and_then(|rates| rates.read_units_per_sec.zip(rates.write_units_per_sec));
                    *total = total.zip(units).map(|(a, (read, write))| a + read + write);
                }
            } else if !group.shards.is_empty() {
                for replica in &group.replicas {
                    if let Some(total) = serving_units.get_mut(&replica.node_id) {
                        *total = None;
                    }
                }
            }
        }

        let locality = Locality::new(groups, snapshot, options);
        let mut placement = Self {
            view,
            config,
            groups,
            resources,
            group_sizes,
            storage_sizes,
            serving_units,
            locality,
            axes: Vec::new(),
            overlapping_groups,
            shard_targets: Vec::new(),
        };
        placement.shard_targets = groups
            .iter()
            .filter_map(|group| {
                if !placement.receives_shards(group) {
                    return None;
                }
                let bytes = *placement.group_sizes.get(&group.id)?;
                Some(ShardTarget {
                    group: group.id,
                    bytes,
                    move_budget: placement.shard_move_budget(bytes),
                })
            })
            .collect();
        placement.axes = options
            .dimensions()
            .filter(|kind| kind.enabled(config))
            .map(|kind| placement.axis(kind, options.deadline(kind), stats))
            .collect();
        placement
    }

    fn axis(&self, kind: Dimension, deadline: Option<f64>, stats: &ClusterStats) -> Axis {
        let (loads, expected, floor, relative) = match kind {
            Dimension::ReadUnits | Dimension::WriteUnits => {
                let loads = self
                    .groups
                    .iter()
                    .filter_map(|group| {
                        if !self.traffic_is_current(group) {
                            return None;
                        }
                        let rates = self.view.samples.group(group).rates?;
                        let value = if kind == Dimension::ReadUnits {
                            rates.read_units_per_sec
                        } else {
                            rates.write_units_per_sec
                        };
                        Some((group.id, value?))
                    })
                    .collect();
                (loads, self.groups.len(), 1., 0.1)
            }
            Dimension::ShardSize => {
                let loads =
                    self.group_sizes.iter().map(|(&id, &bytes)| (id, bytes as f64)).collect();
                (
                    loads,
                    self.groups.len(),
                    self.config.shard_size_balance_tolerance_bytes as f64,
                    0.1,
                )
            }
            Dimension::ShardCount => {
                let loads =
                    self.groups.iter().map(|group| (group.id, group.shards.len() as f64)).collect();
                (loads, self.groups.len(), 0.5, 0.)
            }
            Dimension::Cpu | Dimension::Memory | Dimension::Disk => {
                let loads = self
                    .view
                    .available
                    .iter()
                    .filter_map(|&node| {
                        let resources = self.resources.get(&node)?;
                        let (used, capacity) = match kind {
                            Dimension::Cpu => (resources.cpu_used?, resources.cpu_capacity),
                            Dimension::Memory => {
                                (resources.memory_used as f64, resources.memory_capacity as f64)
                            }
                            Dimension::Disk => {
                                (resources.disk_used as f64, resources.disk_capacity as f64)
                            }
                            _ => unreachable!(),
                        };
                        (capacity > 0.).then(|| (node, 100. * used / capacity))
                    })
                    .collect();
                (loads, self.view.available.len(), 5., 0.)
            }
            Dimension::LeaderCount | Dimension::ReplicaCount => {
                let loads = self
                    .view
                    .nodes
                    .iter()
                    .filter(|node| self.view.available.contains(&node.id))
                    .filter_map(|node| {
                        let capacity = node.capacity.as_ref()?;
                        let count = if kind == Dimension::LeaderCount {
                            capacity.leader_count as f64
                        } else {
                            let delta = stats.get_node_delta(node.id).replica_count;
                            (capacity.replica_count as f64 + delta as f64).max(0.)
                        };
                        Some((node.id, count))
                    })
                    .collect();
                let (floor, relative) =
                    if kind == Dimension::LeaderCount { (0.5, 0.) } else { (2., 0.05) };
                (loads, self.view.available.len(), floor, relative)
            }
        };
        Axis::new(kind, loads, expected, floor, relative, deadline)
    }

    fn watermark(&self) -> u64 {
        let limit = self.config.group_size_limit;
        (limit - limit / 5).max(1)
    }

    fn shard_move_budget(&self, target_bytes: u64) -> u64 {
        self.watermark().saturating_sub(target_bytes).min(self.config.move_shard_size_limit)
    }

    /// A hard-cap breach takes precedence over ordinary capacity headroom.
    fn capacity(&self) -> Option<CapacityChoice> {
        if !self.config.enable_group_balance && !self.config.enable_shard_size_balance {
            return None;
        }
        let high = self.watermark();
        let limit = self.config.group_size_limit;
        let over_limit = self.group_sizes.values().any(|&size| size > limit);
        let can_build = self.config.enable_group_balance
            && self.view.available.len() >= self.config.replicas_per_group;
        let creating = self.view.creating;
        let published =
            self.view.published_groups().filter(|group| group.id != ROOT_GROUP_ID).count();
        let known: u128 = self
            .group_sizes
            .iter()
            .filter(|(group, _)| {
                !self.overlapping_groups.contains(group)
                    && !self.view.busy.contains(group)
                    && !self.view.unpublished.contains(group)
            })
            .map(|(_, &size)| u128::from(size))
            .sum();
        if can_build
            && known.div_ceil(u128::from(high)) > published.saturating_add(creating) as u128
        {
            return Some(CapacityChoice::new(CapacityAction::CreateGroup, over_limit));
        }

        if !self.config.enable_shard_size_balance {
            return None;
        }

        let mut best = None;
        let mut can_use_new_group = false;
        let empty_target_budget = self.shard_move_budget(0);
        for source in self.groups.iter().filter(|group| self.shard_moves_ready(group.id)) {
            let Some(&source_size) = self.group_sizes.get(&source.id).filter(|&&size| size > high)
            else {
                continue;
            };
            for shard in source
                .shards
                .iter()
                .filter(|shard| shard.table_id >= sekas_schema::FIRST_USER_TABLE_ID)
            {
                let Some(bytes) =
                    self.view.samples.shard(source.id, shard.id).bytes.filter(|&size| size != 0)
                else {
                    continue;
                };
                let split_goal = self.can_split(shard.id).then_some(source_size - high);
                can_use_new_group |=
                    movable_bytes(bytes, empty_target_budget, split_goal).is_some();
                for target in self.shard_targets.iter().filter(|target| target.group != source.id) {
                    let Some(movement) = target.fit(source.id, shard.id, bytes, split_goal) else {
                        continue;
                    };
                    let amount = movement.split_bytes.unwrap_or(bytes);
                    let action = PlacementAction::Shard(movement);
                    let changes = self.changes(&action);
                    if self
                        .axes
                        .iter()
                        .any(|axis| axis.effect(&changes[axis.kind as usize]).is_none())
                    {
                        continue;
                    }
                    let score = (
                        (source_size.saturating_sub(limit)).min(amount),
                        movement.split_bytes.is_none(),
                        (source_size - high).min(amount),
                        std::cmp::Reverse(amount),
                        std::cmp::Reverse(action.tie_breaker()),
                    );
                    if best.as_ref().is_none_or(|(previous, _)| score > *previous) {
                        best = Some((score, movement));
                    }
                }
            }
        }
        if let Some((score, movement)) = best {
            return Some(CapacityChoice::new(CapacityAction::Place(movement), score.0 > 0));
        }
        if can_build
            && can_use_new_group
            && creating == 0
            && self.shard_targets.len() == self.groups.len()
        {
            return Some(CapacityChoice::new(CapacityAction::CreateGroup, over_limit));
        }
        None
    }

    fn group_available(&self, group: u64) -> bool {
        !self.overlapping_groups.contains(&group) && self.view.group_available(group)
    }

    fn shard_moves_ready(&self, group: u64) -> bool {
        self.group_available(group) && self.view.migration_idle(group)
    }

    fn can_split(&self, shard: u64) -> bool {
        self.config.enable_auto_shard_split && !self.view.split_cooldowns.contains(&shard)
    }

    pub fn plan(&self) -> PlacementPlan {
        let mut best = None::<RankedAction>;
        let mut blocked = BTreeSet::new();
        self.visit_candidates(|action| match self.rank(&action) {
            Ok(Some(candidate))
                if best.as_ref().is_none_or(|best| candidate.compare(best).is_gt()) =>
            {
                best = Some(candidate);
            }
            Err(kind) => {
                blocked.insert(kind);
            }
            _ => {}
        });
        let no_action = if blocked.is_empty() {
            "no eligible move improves balance".to_owned()
        } else {
            let policies = blocked.into_iter().map(Dimension::name).collect::<Vec<_>>().join(", ");
            format!(
                "no eligible move improves balance. Deadlines or missing load data blocked candidates for {policies}"
            )
        };
        let waiting = self
            .groups
            .iter()
            .filter(|group| !self.view.migration_idle(group.id))
            .map(|group| {
                let reason = self.view.samples.migration_report_issue(group.id).unwrap_or(
                    if self.view.samples.leader(group.id).is_some() {
                        "shard migration in progress"
                    } else {
                        "awaiting current leader report"
                    },
                );
                format!("g{} {reason}", group.id)
            })
            .collect::<Vec<_>>();
        let report_wait = (!waiting.is_empty())
            .then(|| format!("Waiting for current shard migration status\n{}", waiting.join("\n")));
        let choice = best.as_ref().map(|candidate| {
            let priority = if candidate.score.0 > 0. {
                PlacementPriority::LoadDeadline
            } else {
                PlacementPriority::Imbalance
            };
            let benefit = match priority {
                PlacementPriority::LoadDeadline => "reduces load above the configured deadline",
                _ => "reduces load imbalance",
            };
            let reason = candidate.action.split_bytes().map_or_else(
                || format!("This move {benefit}"),
                |bytes| format!("Splitting and moving up to {bytes} B {benefit}"),
            );
            PlacementChoice {
                dimension: self.axes[candidate.rank].kind,
                action: candidate.action,
                priority,
                reason,
            }
        });
        let decisions = self
            .axes
            .iter()
            .enumerate()
            .map(|(index, axis)| {
                let decision = if let Some(choice) = &choice
                    && axis.kind == choice.dimension
                {
                    let detail = choice.action.split_bytes().map_or_else(
                        || choice.action.detail(),
                        |bytes| {
                            format!(
                                "split before moving a piece of up to {bytes} bytes ({})",
                                choice.action.detail()
                            )
                        },
                    );
                    BalanceDecision::ActionNeeded(detail)
                } else if best.as_ref().is_some_and(|candidate| index > candidate.rank) {
                    BalanceDecision::Deferred("higher-priority placement takes precedence".into())
                } else if is_group_dimension(axis.kind)
                    && let Some(reason) = &report_wait
                {
                    BalanceDecision::Deferred(reason.clone().into())
                } else {
                    axis.report(&no_action)
                };
                (axis.kind, decision, axis.thresholds())
            })
            .collect();
        PlacementPlan { choice, decisions }
    }

    fn receives_shards(&self, group: &GroupDesc) -> bool {
        self.shard_moves_ready(group.id)
            && group.replicas.len() == self.config.replicas_per_group
            && group.replicas.iter().all(|r| {
                r.role == ReplicaRole::Voter as i32
                    && self.view.available.contains(&r.node_id)
                    && !self.view.copying_nodes.contains(&r.node_id)
            })
    }

    fn visit_candidates(&self, mut visit: impl FnMut(PlacementAction)) {
        let can_donate = |id, from_group| {
            self.axes
                .iter()
                .any(|axis| is_group_dimension(axis.kind) == from_group && axis.may_donate(id))
        };
        for group in self.groups {
            if !self.group_available(group.id) {
                continue;
            }
            let leader = self.leader_node(group.id);
            let shard_moves_ready = self.view.migration_idle(group.id);
            let donor_replica =
                group.replicas.iter().any(|replica| can_donate(replica.node_id, false));
            if shard_moves_ready && (donor_replica || can_donate(group.id, true)) {
                self.visit_shard_candidates(group, &mut visit);
            }
            if !donor_replica {
                continue;
            }
            for replica in &group.replicas {
                if replica.role != ReplicaRole::Voter as i32 || !can_donate(replica.node_id, false)
                {
                    continue;
                }
                if leader == Some(replica.node_id) {
                    if !self.config.enable_leader_balance {
                        continue;
                    }
                    for voter in &group.replicas {
                        if voter.node_id != replica.node_id
                            && voter.role == ReplicaRole::Voter as i32
                            && self.view.available.contains(&voter.node_id)
                        {
                            visit(PlacementAction::Leader(TransferLeader {
                                group: group.id,
                                src_replica: replica.id,
                                src_node: replica.node_id,
                                target_replica: voter.id,
                                target_node: voter.node_id,
                            }));
                        }
                    }
                } else if self.config.enable_replica_balance
                    && leader.is_some()
                    && shard_moves_ready
                    && group.replicas.len() >= self.config.replicas_per_group
                {
                    for &target in &self.view.available {
                        if !self.view.copying_nodes.contains(&target)
                            && !self.view.occupied.contains(&(group.id, target))
                            && !group.replicas.iter().any(|replica| replica.node_id == target)
                        {
                            visit(PlacementAction::Replica(ReallocateReplica {
                                group: group.id,
                                source_replica: replica.id,
                                source_node: replica.node_id,
                                target_node: target,
                            }));
                        }
                    }
                }
            }
        }
    }

    fn visit_shard_candidates(&self, source: &GroupDesc, visit: &mut impl FnMut(PlacementAction)) {
        for shard in
            source.shards.iter().filter(|shard| shard.table_id >= sekas_schema::FIRST_USER_TABLE_ID)
        {
            let Some(bytes) = self.view.samples.shard(source.id, shard.id).bytes else {
                continue;
            };
            let split_source_size =
                if self.config.enable_shard_size_balance && self.can_split(shard.id) {
                    self.group_sizes.get(&source.id).copied()
                } else {
                    None
                };
            for target in self.shard_targets.iter().filter(|target| target.group != source.id) {
                let split_goal =
                    split_source_size.map(|size| size.saturating_sub(target.bytes) / 2);
                if let Some(movement) = target.fit(source.id, shard.id, bytes, split_goal) {
                    visit(PlacementAction::Shard(movement));
                }
            }
        }
    }

    fn rank(&self, action: &PlacementAction) -> Result<Option<RankedAction>, Dimension> {
        let split_bytes = action.split_bytes();
        let changes = self.changes(action);
        let mut choice: Option<RankedAction> = None;
        let mut preserves_higher = true;
        for (rank, axis) in self.axes.iter().enumerate() {
            let effect = axis.effect(&changes[axis.kind as usize]).ok_or(axis.kind)?;
            if preserves_higher
                && (split_bytes.is_none() || axis.kind == Dimension::ShardSize)
                && let Some(score) = axis.score(&effect)
                && choice.as_ref().is_none_or(|previous| score.0 > 0. && previous.score.0 == 0.)
            {
                let locality = match action {
                    PlacementAction::Shard(movement) => self.locality.score(movement),
                    _ => 0,
                };
                choice = Some(RankedAction { action: *action, rank, score, locality });
            }
            preserves_higher &= !axis.drives_placement() || effect.preserves_balance();
        }
        Ok(choice)
    }

    fn leader_node(&self, group: u64) -> Option<u64> {
        Some(self.view.samples.leader(group)?.node_id)
    }

    fn traffic_is_current(&self, group: &GroupDesc) -> bool {
        group.shards.is_empty() || self.view.samples.leader(group.id).is_some()
    }

    fn changes(&self, action: &PlacementAction) -> Changes {
        let mut changes: Changes = Default::default();
        match action {
            PlacementAction::Shard(movement) => {
                let split_bytes = movement.split_bytes;
                let source = self.view.group(movement.source_group).unwrap();
                let target = self.view.group(movement.target_group).unwrap();
                let sample = self.view.samples.shard(source.id, movement.shard);
                let bytes = split_bytes.or(sample.bytes).map(|size| size as f64);
                let scale = match (split_bytes, sample.bytes) {
                    // Recheck the new piece's own samples before migrating it.
                    (Some(piece), Some(whole)) => piece as f64 / whole as f64,
                    _ => 1.,
                };
                let rates = sample.rates.filter(|_| self.traffic_is_current(source));
                let read =
                    rates.and_then(|rates| rates.read_units_per_sec).map(|rate| rate * scale);
                let write =
                    rates.and_then(|rates| rates.write_units_per_sec).map(|rate| rate * scale);
                for (kind, amount) in [
                    (Dimension::ShardCount, Some(1.)),
                    (Dimension::ShardSize, bytes),
                    (Dimension::ReadUnits, read),
                    (Dimension::WriteUnits, write),
                ] {
                    changes[kind as usize].transfer(source.id, target.id, amount);
                }
                if split_bytes.is_some() {
                    // The source has an extra shard until a piece migrates.
                    changes[Dimension::ShardCount as usize].add(source.id, 1., 1.);
                }
                let origins = source.replicas.iter().map(|r| r.node_id);
                let destinations = target.replicas.iter().map(|r| r.node_id);
                self.storage_changes(&mut changes, origins, destinations, bytes);
                self.serving_change(
                    &mut changes,
                    self.leader_node(source.id),
                    self.leader_node(target.id),
                    read.zip(write).map(|(r, w)| r + w),
                );
            }
            PlacementAction::Replica(movement) => {
                let source = movement.source_node;
                let target = movement.target_node;
                changes[Dimension::ReplicaCount as usize].transfer(source, target, Some(1.));
                self.storage_changes(
                    &mut changes,
                    [source],
                    [target],
                    self.group_sizes.get(&movement.group).map(|&bytes| bytes as f64),
                );
            }
            PlacementAction::Leader(transfer) => {
                changes[Dimension::LeaderCount as usize].transfer(
                    transfer.src_node,
                    transfer.target_node,
                    Some(1.),
                );
                let group = self.view.group(transfer.group).unwrap();
                let units = self
                    .view
                    .samples
                    .group(group)
                    .rates
                    .filter(|_| self.traffic_is_current(group))
                    .and_then(|rates| {
                        rates.read_units_per_sec.zip(rates.write_units_per_sec).map(|(r, w)| r + w)
                    });
                self.serving_change(
                    &mut changes,
                    Some(transfer.src_node),
                    Some(transfer.target_node),
                    units,
                );
            }
        }
        changes
    }

    fn serving_change(
        &self,
        changes: &mut Changes,
        source: Option<u64>,
        target: Option<u64>,
        units: Option<f64>,
    ) {
        let change = &mut changes[Dimension::Cpu as usize];
        let Some((source, target)) = source.zip(target) else {
            change.unknown = units != Some(0.);
            return;
        };
        if source == target || units == Some(0.) {
            return;
        }
        let projected = || {
            let units = units?;
            let total = self.serving_units.get(&source).copied().flatten()?.max(units);
            let src = self.resources.get(&source)?;
            let dest = self.resources.get(&target)?;
            let cores = src.process_cpu_used?.min(src.cpu_used?) * units / total;
            Some((percent(cores, src.cpu_capacity)?, percent(cores, dest.cpu_capacity)?))
        };
        if let Some((release, receive)) = projected() {
            change.add(source, -release, 0.);
            change.add(target, receive, 0.);
        } else {
            change.unknown = true;
        }
    }

    fn storage_changes(
        &self,
        changes: &mut Changes,
        sources: impl IntoIterator<Item = u64>,
        destinations: impl IntoIterator<Item = u64>,
        bytes: Option<f64>,
    ) {
        let Some(bytes) = bytes else {
            changes[Dimension::Disk as usize].unknown = true;
            changes[Dimension::Memory as usize].unknown = true;
            return;
        };
        if bytes == 0. {
            return;
        }

        // Shared process memory has no exact per-group accounting. Attribute
        // a bounded share of resident memory to each copy using stored bytes.
        let mut incoming_memory = None::<f64>;
        let mut source_memory = Vec::new();
        for source in sources {
            let resource = self.resources.get(&source);
            let disk = resource.and_then(|usage| percent(bytes, usage.disk_capacity as f64));
            if let Some(disk) = disk {
                changes[Dimension::Disk as usize].add(source, -disk, 0.);
            }
            let memory = resource.and_then(|usage| {
                let stored = (*self.storage_sizes.get(&source)? as f64).max(bytes);
                let resident = (usage.process_memory_used as f64).min(stored);
                let part = resident * bytes / stored;
                source_memory.push((source, part));
                incoming_memory = Some(incoming_memory.map_or(part, |known| known.max(part)));
                percent(part, usage.memory_capacity as f64)
            });
            if let Some(memory) = memory {
                changes[Dimension::Memory as usize].add(source, -memory, 0.);
            }
        }
        for target in destinations {
            let resource = self.resources.get(&target);
            if let Some(disk) =
                resource.and_then(|usage| percent(bytes, usage.disk_capacity as f64))
            {
                changes[Dimension::Disk as usize].add(target, disk, disk);
            } else {
                changes[Dimension::Disk as usize].unknown = true;
            }
            let incoming = source_memory
                .iter()
                .find(|&&(node, _)| node == target)
                .map(|&(_, bytes)| bytes)
                .or(incoming_memory);
            if let Some(memory) = incoming
                .zip(resource)
                .and_then(|(part, usage)| percent(part, usage.memory_capacity as f64))
            {
                changes[Dimension::Memory as usize].add(target, memory, memory);
            } else {
                changes[Dimension::Memory as usize].unknown = true;
            }
        }
    }
}

fn percent(amount: f64, capacity: f64) -> Option<f64> {
    (capacity > 0.).then(|| 100. * amount / capacity)
}

fn is_group_dimension(kind: Dimension) -> bool {
    matches!(
        kind,
        Dimension::ShardSize | Dimension::ShardCount | Dimension::ReadUnits | Dimension::WriteUnits
    )
}

/// Prefer the whole shard. When it does not fit, a caller's split goal is
/// bounded by the same move budget, including when testing an empty group as a
/// target.
fn movable_bytes(bytes: u64, move_budget: u64, split_goal: Option<u64>) -> Option<u64> {
    if bytes <= move_budget {
        return Some(bytes);
    }
    let piece = split_goal?.min(move_budget);
    (piece > 0).then_some(piece)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locality_joins_quiet_neighbors_and_separates_hot_neighbors() {
        let mut locality = Locality::default();
        locality.neighbors.insert(
            3,
            vec![
                Neighbor { shard: 1, group: 1, weight: 1 },
                Neighbor { shard: 2, group: 2, weight: 2 },
            ],
        );
        let movement =
            ReallocateShard { shard: 3, source_group: 1, target_group: 2, split_bytes: None };
        locality.hot.extend([(1, false), (2, false), (3, false)]);
        assert_eq!(locality.score(&movement), 1);
        locality.hot.extend([(1, true), (2, true), (3, true)]);
        assert_eq!(locality.score(&movement), -1);
        locality.hot.insert(2, false);
        assert_eq!(locality.score(&movement), 1);
    }
}

pub(in crate::root) fn plan_round(
    view: &ClusterView,
    config: &RootConfig,
    options: &BalanceConfig,
    stats: &ClusterStats,
) -> (Option<CapacityChoice>, PlacementPlan) {
    let placement = Placement::new(view, config, options, stats);
    let capacity = placement.capacity();
    (capacity, placement.plan())
}
