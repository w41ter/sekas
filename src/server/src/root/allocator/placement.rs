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

use sekas_api::server::v1::{
    GroupDesc, NodeResourceUsage, ReplicaReplacement, ReplicaRole, ShardDesc,
};

use super::balance::{Axis, Change};
use super::{
    BalanceDecision, BalanceThresholds, ReallocateReplica, ReallocateShard, TransferLeader,
};
use crate::constants::ROOT_GROUP_ID;
use crate::root::ClusterStats;
use crate::root::schedule::{Action, ClusterView};
use crate::root::stats::{RequestUnits, ShowSnapshot};
use crate::schedule::{ReplacementStep, replacement_step};
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
    #[cfg(test)]
    pub requests: HashMap<u64, Option<f64>>,
    pub hot_split: Option<HotSplit>,
}

#[derive(Clone, Copy)]
pub(in crate::root) struct HotSplit {
    pub group: u64,
    pub shard: u64,
    pub move_budget: u64,
    pub dimension: Dimension,
}

pub(in crate::root) struct PlacementChoice {
    pub dimension: Dimension,
    pub action: PlacementAction,
    pub priority: PlacementPriority,
    pub reason: String,
    pub node_request: bool,
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
    resource_nodes: [HashMap<u64, u64>; Dimension::ALL.len()],
    group_sizes: HashMap<u64, u64>,
    planned_group_sizes: HashMap<u64, u64>,
    storage_sizes: HashMap<u64, u64>,
    serving_units: HashMap<u64, Option<f64>>,
    requests: HashMap<u64, RequestUnits>,
    requests_ready: bool,
    planned_requests: HashMap<u64, RequestUnits>,
    planned_requests_ready: bool,
    locality: Locality,
    axes: Vec<Axis>,
    request_axes: Vec<Axis>,
    overlapping_groups: HashSet<u64>,
    shard_targets: Vec<ShardTarget>,
}

#[derive(Default)]
struct Changes {
    placement: [Change; Dimension::ALL.len()],
    requests: [Change; 2],
}

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
                let Some(rates) = snapshot.shard(shard.id).rates else { continue };
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
    dimension: Dimension,
    score: (f64, f64, bool),
    group_gain: f64,
    node_request: bool,
    locality: i32,
    request_transfer: bool,
}

impl RankedAction {
    fn compare(&self, other: &Self) -> Ordering {
        (self.score.0 > 0.)
            .cmp(&(other.score.0 > 0.))
            .then(other.rank.cmp(&self.rank))
            .then_with(|| self.request_transfer.cmp(&other.request_transfer))
            .then(
                matches!(other.action, PlacementAction::Replica(_))
                    .cmp(&matches!(self.action, PlacementAction::Replica(_))),
            )
            .then(self.action.split_bytes().is_none().cmp(&other.action.split_bytes().is_none()))
            .then_with(|| self.score.0.total_cmp(&other.score.0))
            .then_with(|| {
                if self.node_request && other.node_request {
                    // Use Group load to break ties between comparable node gains.
                    (self.score.1 * 100.)
                        .floor()
                        .total_cmp(&(other.score.1 * 100.).floor())
                        .then_with(|| self.group_gain.total_cmp(&other.group_gain))
                } else {
                    Ordering::Equal
                }
            })
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
        let mut resource_nodes = std::array::from_fn(|_| HashMap::new());
        for kind in [Dimension::Cpu, Dimension::Memory, Dimension::Disk] {
            let mut domains = HashMap::<&str, Vec<u64>>::new();
            for &node in &view.available {
                let Some(domain) = options.resource_domains.get(&node.to_string()) else {
                    continue;
                };
                let name = match kind {
                    Dimension::Cpu => &domain.cpu,
                    Dimension::Memory => &domain.memory,
                    Dimension::Disk => &domain.disk,
                    _ => unreachable!(),
                };
                if let Some(name) = name {
                    domains.entry(name).or_default().push(node);
                }
            }
            for nodes in domains.values() {
                let representative = *nodes
                    .iter()
                    .min_by_key(|node| (!resources.contains_key(node), *node))
                    .unwrap();
                resource_nodes[kind as usize]
                    .extend(nodes.iter().map(|&node| (node, representative)));
            }
        }
        let mut group_sizes = HashMap::with_capacity(groups.len());
        let mut storage_sizes = HashMap::<u64, u64>::new();
        let mut serving_units: HashMap<_, _> =
            view.available.iter().map(|&node| (node, Some(0.))).collect();
        let mut requests: HashMap<_, _> =
            view.available.iter().map(|&node| (node, RequestUnits::ZERO)).collect();
        let mut planned_requests = requests.clone();
        let mut requests_ready = true;
        let mut planned_requests_ready = true;
        let mut seen = HashMap::new();
        let mut overlapping_groups = HashSet::new();
        let mut unexpected_overlaps = HashSet::new();
        let moving_shards: HashMap<_, _> = view
            .placements
            .iter()
            .filter_map(|action| {
                let Action::MoveShard(action) = action else { return None };
                let desc = &action.desc;
                let shard = desc.get_shard_desc();
                let source = view.group(desc.src_group_id)?;
                let target = view.group(desc.dest_group_id)?;
                let source_reports_shard = source.shard(shard.id) == Some(shard);
                let target_reports_shard = target.shard(shard.id) == Some(shard);
                planned_requests_ready &= source_reports_shard || target_reports_shard;
                (source_reports_shard && target_reports_shard)
                    .then_some((shard.id, (source.id, target.id)))
            })
            .collect();
        for group in groups {
            let mut known_size = 0_u64;
            for shard in &group.shards {
                if let Some(previous) = seen.insert(shard.id, group.id) {
                    overlapping_groups.extend([previous, group.id]);
                    if !moving_shards.get(&shard.id).is_some_and(|&(source, target)| {
                        (source, target) == (previous, group.id)
                            || (target, source) == (previous, group.id)
                    }) {
                        unexpected_overlaps.extend([previous, group.id]);
                    }
                }
                known_size = known_size.saturating_add(snapshot.shard(shard.id).bytes.unwrap_or(0));
            }
            let sample = snapshot.group(group);
            if let Some(size) = sample.bytes {
                group_sizes.insert(group.id, size);
            }
            for replica in &group.replicas {
                let total = storage_sizes.entry(replica.node_id).or_default();
                *total = total.saturating_add(sample.bytes.unwrap_or(known_size));
            }

            if let Some(leader) = view.samples.leader(group.id) {
                if !view.unpublished.contains(&group.id) {
                    let units = snapshot.group_request_units(group);
                    if let Some(total) = requests.get_mut(&leader.node_id) {
                        *total = total.add(units);
                    }
                    if let Some(total) = planned_requests.get_mut(&leader.node_id) {
                        let planned = if view.busy.contains(&group.id) {
                            group.shards.iter().fold(RequestUnits::ZERO, |total, shard| {
                                if moving_shards
                                    .get(&shard.id)
                                    .is_some_and(|&(source, _)| source == group.id)
                                {
                                    return total;
                                }
                                total.add(Self::planning_shard_units(snapshot, group.id, shard.id))
                            })
                        } else {
                            units
                        };
                        *total = total.add(planned);
                    }
                    let available =
                        view.available.contains(&leader.node_id) || group.shards.is_empty();
                    requests_ready &= available;
                    planned_requests_ready &= available;
                }
                if let Some(total) = serving_units.get_mut(&leader.node_id) {
                    let units = sample.rates.and_then(|rates| rates.request_units().total());
                    *total = total.zip(units).map(|(a, b)| a + b);
                }
            } else if !group.shards.is_empty() {
                for replica in &group.replicas {
                    if let Some(total) = serving_units.get_mut(&replica.node_id) {
                        *total = None;
                    }
                    if !view.unpublished.contains(&group.id) {
                        requests_ready = false;
                        planned_requests_ready = false;
                        if let Some(total) = requests.get_mut(&replica.node_id) {
                            *total = RequestUnits::default();
                        }
                        if let Some(total) = planned_requests.get_mut(&replica.node_id) {
                            *total = RequestUnits::default();
                        }
                    }
                }
            }
        }
        for group in groups.iter().filter(|group| unexpected_overlaps.contains(&group.id)) {
            planned_requests_ready = false;
            if let Some(leader) = view.samples.leader(group.id)
                && let Some(total) = planned_requests.get_mut(&leader.node_id)
            {
                *total = RequestUnits::default();
            }
        }

        for group in groups.iter().filter(|group| overlapping_groups.contains(&group.id)) {
            requests_ready = false;
            if let Some(leader) = view.samples.leader(group.id)
                && let Some(total) = requests.get_mut(&leader.node_id)
            {
                *total = RequestUnits::default();
            }
        }
        let locality = Locality::new(groups, snapshot, options);
        let mut placement = Self {
            view,
            config,
            groups,
            resources,
            resource_nodes,
            group_sizes,
            planned_group_sizes: HashMap::new(),
            storage_sizes,
            serving_units,
            requests,
            requests_ready,
            planned_requests,
            planned_requests_ready,
            locality,
            axes: Vec::new(),
            request_axes: Vec::new(),
            overlapping_groups,
            shard_targets: Vec::new(),
        };
        let pending = placement.pending_changes();
        let size_changes = &pending.placement[Dimension::ShardSize as usize];
        placement.planned_group_sizes = placement
            .group_sizes
            .iter()
            .map(|(&group, &bytes)| {
                (group, (bytes as f64 + size_changes.steady_delta(group)) as u64)
            })
            .collect();
        placement.shard_targets = groups
            .iter()
            .filter_map(|group| {
                if !placement.receives_shards(group) {
                    return None;
                }
                let bytes = *placement.planned_group_sizes.get(&group.id)?;
                Some(ShardTarget {
                    group: group.id,
                    bytes,
                    move_budget: placement.shard_move_budget(bytes),
                })
            })
            .collect();
        placement.axes = options
            .dimensions(config.enable_node_request_balance)
            .filter(|kind| kind.enabled(config))
            .map(|kind| placement.axis(kind, options, &pending.placement[kind as usize]))
            .collect();
        if config.enable_node_request_balance {
            // Evaluate Group traffic after node resources and before shard size.
            placement.axes.sort_by_key(|axis| {
                if axis.kind.is_resource() {
                    0
                } else if axis.kind.request_index().is_some() {
                    1
                } else {
                    2
                }
            });
            placement.request_axes = Dimension::REQUESTS
                .into_iter()
                .filter(|kind| kind.enabled(config))
                .map(|kind| {
                    placement.request_axis(kind, options).with_pending(
                        &pending.requests[kind.request_index().unwrap()],
                        1.,
                        0.1,
                    )
                })
                .collect();
        }
        placement
    }

    fn request_axis(&self, kind: Dimension, options: &BalanceConfig) -> Axis {
        let axis = |requests: &HashMap<_, RequestUnits>, ready| {
            let totals: HashMap<_, _> =
                requests.iter().filter_map(|(&id, &load)| Some((id, load.get(kind)?))).collect();
            let ready = ready && totals.len() == self.view.available.len();
            let weights =
                self.view.available.iter().map(|&id| (id, options.request_weight(id))).collect();
            Axis::requests(kind, totals, weights, ready)
        };
        let observed = axis(&self.requests, self.requests_ready).thresholds();
        axis(&self.planned_requests, self.planned_requests_ready).with_observed_thresholds(observed)
    }

    fn planning_shard_units(snapshot: &ShowSnapshot, group: u64, shard: u64) -> RequestUnits {
        snapshot
            .shard_request_units(group, shard)
            .with_fallback(snapshot.recent_shard_requests(shard))
    }

    fn resource_node(&self, kind: Dimension, node: u64) -> u64 {
        self.resource_nodes[kind as usize].get(&node).copied().unwrap_or(node)
    }

    fn axis(&self, kind: Dimension, options: &BalanceConfig, pending: &Change) -> Axis {
        let deadline = options.deadline(kind);
        let request_first = self.config.enable_node_request_balance;
        let (loads, expected, floor, relative): (HashMap<u64, f64>, _, _, _) = match kind {
            Dimension::ReadUnits | Dimension::WriteUnits => {
                let loads = self
                    .groups
                    .iter()
                    .filter_map(|group| {
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
                let domains: HashSet<_> = self
                    .view
                    .available
                    .iter()
                    .map(|&node| self.resource_node(kind, node))
                    .collect();
                let loads = domains
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
                (loads, domains.len(), 5., 0.)
            }
            Dimension::LeaderCount | Dimension::ReplicaCount => {
                let mut loads: HashMap<_, _> =
                    self.view.available.iter().map(|&id| (id, 0.)).collect();
                let leader_count = kind == Dimension::LeaderCount;
                let mut complete = true;
                for group in self.view.published_groups() {
                    if leader_count {
                        if let Some(leader) = self.view.samples.leader(group.id) {
                            if let Some(count) = loads.get_mut(&leader.node_id) {
                                *count += 1.;
                            }
                        } else {
                            complete = false;
                        }
                    } else {
                        for replica in &group.replicas {
                            if let Some(count) = loads.get_mut(&replica.node_id) {
                                *count += 1.;
                            }
                        }
                    }
                }
                if request_first {
                    let disk_known = self.view.available.iter().all(|id| {
                        self.resources.get(id).is_some_and(|sample| sample.disk_capacity > 0)
                    });
                    let weights = self
                        .view
                        .available
                        .iter()
                        .map(|&id| {
                            let weight = if leader_count {
                                options.request_weight(id)
                            } else if disk_known {
                                self.resources[&id].disk_capacity as f64
                            } else {
                                1.
                            };
                            (id, weight)
                        })
                        .collect();
                    return Axis::new(
                        kind,
                        loads,
                        self.view.available.len() + usize::from(!complete),
                        0.5,
                        0.,
                        deadline,
                    )
                    .with_pending(pending, 0.5, 0.)
                    .with_count_targets(weights)
                    .with_soft_balance(complete);
                }
                let (floor, relative) =
                    if kind == Dimension::LeaderCount { (0.5, 0.) } else { (2., 0.05) };
                (loads, self.view.available.len() + usize::from(!complete), floor, relative)
            }
        };
        let shard_counts = (request_first && kind == Dimension::ShardCount)
            .then(|| loads.keys().map(|&group| (group, 1.)).collect());
        let axis = Axis::new(kind, loads, expected, floor, relative, deadline)
            .with_pending(pending, floor, relative);
        let axis =
            if let Some(weights) = shard_counts { axis.with_count_targets(weights) } else { axis };
        if request_first { axis.with_soft_balance(true) } else { axis }
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
        let over_limit = self.planned_group_sizes.values().any(|&size| size > limit);
        let can_build = self.config.enable_group_balance
            && self.view.available.len() >= self.config.replicas_per_group;
        let creating = self.view.creating;
        let published =
            self.view.published_groups().filter(|group| group.id != ROOT_GROUP_ID).count();
        let known: u128 = self
            .planned_group_sizes
            .iter()
            .filter(|(group, _)| {
                !self.overlapping_groups.contains(group) && !self.view.unpublished.contains(group)
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
        let mut existing_space = false;
        let empty_target_budget = self.shard_move_budget(0);
        for source in self.groups.iter().filter(|group| self.shard_moves_ready(group.id)) {
            let Some(&source_size) =
                self.planned_group_sizes.get(&source.id).filter(|&&size| size > high)
            else {
                continue;
            };
            for shard in source
                .shards
                .iter()
                .filter(|shard| shard.table_id >= sekas_schema::FIRST_USER_TABLE_ID)
            {
                let Some(bytes) = self.view.samples.shard(shard.id).bytes.filter(|&size| size != 0)
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
                    existing_space = true;
                    let amount = movement.split_bytes.unwrap_or(bytes);
                    let action = PlacementAction::Shard(movement);
                    let changes = self.changes(&action);
                    let planning_watermark =
                        source_size <= limit && self.config.enable_node_request_balance;
                    if planning_watermark && !self.request_score(&changes).0 {
                        continue;
                    }
                    if self.axes.iter().any(|axis| {
                        match axis.effect(&changes.placement[axis.kind as usize]) {
                            None => true,
                            Some(effect) => {
                                planning_watermark
                                    && axis.kind.is_resource()
                                    && !effect.preserves_balance()
                            }
                        }
                    }) {
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
            && (!self.config.enable_node_request_balance || over_limit || !existing_space)
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
            let policies =
                blocked.iter().copied().map(Dimension::name).collect::<Vec<_>>().join(", ");
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
            let kind = candidate.dimension;
            let reason = if candidate.node_request && candidate.score.0 == 0. {
                format!("Reduces node {} imbalance", kind.name())
            } else if self.config.enable_node_request_balance
                && kind.request_index().is_some()
                && candidate.score.0 == 0.
            {
                format!("Reduces Group {} imbalance within node tolerances", kind.name())
            } else {
                candidate.action.split_bytes().map_or_else(
                    || format!("This move {benefit}"),
                    |bytes| format!("Splitting and moving up to {bytes} B {benefit}"),
                )
            };
            PlacementChoice {
                dimension: kind,
                action: candidate.action,
                priority,
                reason,
                node_request: candidate.node_request,
            }
        });
        let hot_split = if choice.as_ref().is_none_or(|choice| {
            !choice.node_request && choice.priority == PlacementPriority::Imbalance
        }) {
            self.hot_split()
        } else {
            None
        };
        let decisions = self
            .axes
            .iter()
            .enumerate()
            .map(|(index, axis)| {
                let node_axis = self.request_axes.iter().find(|node| node.kind == axis.kind);
                let decision = if hot_split.is_none()
                    && let Some(choice) = &choice
                    && axis.kind == choice.dimension
                {
                    let detail = choice.action.split_bytes().map_or_else(
                        || choice.action.detail(),
                        |bytes| format!("split before moving a piece of up to {bytes} bytes ({})", choice.action.detail()),
                    );
                    BalanceDecision::ActionNeeded(if self.config.enable_node_request_balance && axis.kind.request_index().is_some() {
                        format!("{detail}. {}", choice.reason)
                    } else { detail })
                } else if let Some(split) = hot_split
                    && split.dimension == axis.kind
                {
                    BalanceDecision::ActionNeeded(format!("Split hot shard {} in g{} and remeasure both pieces", split.shard, split.group))
                } else if let Some(node) = node_axis {
                    if !node.is_ready() {
                        BalanceDecision::Deferred("waiting for current node load samples".into())
                    } else if node.has_imbalance() {
                        if hot_split.is_some() || best.as_ref().is_some_and(|candidate| candidate.node_request) {
                            BalanceDecision::Deferred("another request placement takes precedence".into())
                        } else if blocked.is_empty() {
                            BalanceDecision::Blocked("no available voter or movable shard improves node balance without worsening peer traffic".into())
                        } else {
                            BalanceDecision::Blocked(no_action.clone())
                        }
                    } else {
                        BalanceDecision::NoAction("Projected node load is within the tolerance band".into())
                    }
                } else if hot_split.is_some()
                    || best.as_ref().is_some_and(|candidate| candidate.rank < index + usize::from(self.config.enable_node_request_balance))
                {
                    BalanceDecision::Deferred("higher-priority placement takes precedence".into())
                } else if is_group_dimension(axis.kind) && let Some(reason) = &report_wait {
                    BalanceDecision::Deferred(reason.clone().into())
                } else {
                    axis.report(&no_action)
                };
                let thresholds = node_axis.map_or_else(|| axis.thresholds(), Axis::thresholds);
                (axis.kind, decision, thresholds)
            })
            .collect();
        PlacementPlan {
            choice,
            decisions,
            #[cfg(test)]
            requests: self.requests.iter().map(|(&node, &units)| (node, units.total())).collect(),
            hot_split,
        }
    }

    fn hot_split(&self) -> Option<HotSplit> {
        if !self.config.enable_hot_shard_split || !self.planned_requests_ready {
            return None;
        }
        let mut best = None::<(f64, HotSplit)>;
        for source in self.groups.iter().filter(|g| self.shard_moves_ready(g.id)) {
            let Some(from) = self.leader_node(source.id) else { continue };
            if !self.request_axes.iter().any(|axis| axis.may_donate(from)) {
                continue;
            }
            for shard in &source.shards {
                if shard.table_id < sekas_schema::FIRST_USER_TABLE_ID || !self.can_split(shard.id) {
                    continue;
                }
                let units = self.view.samples.shard_request_units(source.id, shard.id);
                let Some(bytes) = self.view.samples.shard(shard.id).bytes else { continue };
                let min_side = self.config.min_split_side_bytes();
                if bytes < min_side.saturating_mul(2) {
                    continue;
                }
                for target in self.shard_targets.iter().filter(|target| target.group != source.id) {
                    let Some(to) = self.leader_node(target.group) else { continue };
                    // Check whether a roughly even split is worth trying. Root
                    // measures the actual traffic on both sides before moving either.
                    if from == to || target.move_budget < bytes.div_ceil(2) {
                        continue;
                    }
                    let mut requests = Changes::default();
                    let half = RequestUnits {
                        read: units.read.map(|value| value / 2.),
                        write: units.write.map(|value| value / 2.),
                    };
                    self.request_change(&mut requests, Some(from), Some(to), half);
                    let (_, Some((dimension, gain))) = self.request_score(&requests) else {
                        continue;
                    };
                    let action = PlacementAction::Shard(ReallocateShard {
                        shard: shard.id,
                        source_group: source.id,
                        target_group: target.group,
                        split_bytes: Some(target.move_budget.min(bytes)),
                    });
                    let changes = self.changes(&action);
                    if self
                        .axes
                        .iter()
                        .any(|axis| axis.effect(&changes.placement[axis.kind as usize]).is_none())
                    {
                        continue;
                    }
                    let candidate = HotSplit {
                        group: source.id,
                        shard: shard.id,
                        move_budget: target.move_budget,
                        dimension,
                    };
                    if best.is_none_or(|(score, previous)| {
                        gain > score
                            || gain == score
                                && (
                                    candidate.group,
                                    candidate.shard,
                                    std::cmp::Reverse(candidate.move_budget),
                                ) < (
                                    previous.group,
                                    previous.shard,
                                    std::cmp::Reverse(previous.move_budget),
                                )
                    }) {
                        best = Some((gain, candidate));
                    }
                }
            }
        }
        best.map(|(_, candidate)| candidate)
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
            self.axes.iter().any(|axis| {
                is_group_dimension(axis.kind) == from_group
                    && axis.may_donate(self.resource_node(axis.kind, id))
            })
        };
        let underfilled = Dimension::REQUESTS.map(|kind| {
            self.request_axes.iter().any(|axis| axis.kind == kind && axis.has_undersupplied_node())
        });
        for group in self.groups {
            if !self.group_available(group.id) {
                continue;
            }
            let leader = self.leader_node(group.id);
            let shard_moves_ready = self.view.migration_idle(group.id);
            let donor_replica =
                group.replicas.iter().any(|replica| can_donate(replica.node_id, false));
            let request_source = leader.is_some_and(|node| {
                let units = self.view.samples.group_request_units(group);
                self.request_axes.iter().any(|axis| {
                    (underfilled[axis.kind.request_index().unwrap()] || axis.may_donate(node))
                        && units.get(axis.kind).is_some_and(|units| units > 0.)
                })
            });
            if shard_moves_ready && (request_source || donor_replica || can_donate(group.id, true))
            {
                self.visit_shard_candidates(group, &mut visit);
            }
            if !donor_replica && !request_source {
                continue;
            }
            for replica in &group.replicas {
                let is_leader = leader == Some(replica.node_id);
                if replica.role() != ReplicaRole::Voter
                    || !(can_donate(replica.node_id, false) || is_leader && request_source)
                {
                    continue;
                }
                if is_leader {
                    if !self.config.enable_leader_balance {
                        continue;
                    }
                    for voter in &group.replicas {
                        if voter.node_id != replica.node_id
                            && voter.role == ReplicaRole::Voter as i32
                            && self.view.available.contains(&voter.node_id)
                            && !self.view.leader_cooldowns.contains(&(group.id, voter.id))
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
            let Some(bytes) = self.view.samples.shard(shard.id).bytes else {
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

    fn request_score(&self, changes: &Changes) -> (bool, Option<(Dimension, f64)>) {
        let mut preserves = true;
        let mut best = None::<(Dimension, f64)>;
        for axis in &self.request_axes {
            let effect =
                axis.effect(&changes.requests[axis.kind.request_index().unwrap()]).unwrap();
            preserves &= effect.preserves_balance();
            if axis.score(&effect).is_some() {
                let gain = axis.relative_gain(&effect);
                if best.is_none_or(|(_, previous)| gain > previous) {
                    best = Some((axis.kind, gain));
                }
            }
        }
        (preserves, best.filter(|_| preserves))
    }

    fn rank(&self, action: &PlacementAction) -> Result<Option<RankedAction>, Dimension> {
        let changes = self.changes(action);
        let split = action.split_bytes().is_some();
        let request_mode = self.config.enable_node_request_balance;
        let locality = match action {
            PlacementAction::Shard(movement) => self.locality.score(movement),
            _ => 0,
        };
        let (mut preserves_higher, request_choice) = self.request_score(&changes);
        let mut choice = if !split {
            request_choice.map(|(dimension, gain)| RankedAction {
                action: *action,
                rank: 0,
                dimension,
                score: (0., gain, false),
                locality,
                group_gain: 0.,
                node_request: true,
                request_transfer: matches!(action, PlacementAction::Leader(_)),
            })
        } else {
            None
        };
        let nodes_balanced =
            self.request_axes.iter().all(|axis| axis.is_ready() && !axis.has_imbalance());
        let mut group_gain = 0.;
        for (index, axis) in self.axes.iter().enumerate() {
            let effect = axis.effect(&changes.placement[axis.kind as usize]).ok_or(axis.kind)?;
            let group_traffic = request_mode && axis.kind.request_index().is_some();
            if group_traffic {
                group_gain += axis.relative_gain(&effect);
            }
            if (!split || axis.kind == Dimension::ShardSize)
                && let Some(score) = axis.score(&effect)
                && (preserves_higher || request_mode && score.0 > 0.)
                && (!group_traffic || nodes_balanced || score.0 > 0.)
                && choice.as_ref().is_none_or(|previous| score.0 > 0. && previous.score.0 == 0.)
            {
                choice = Some(RankedAction {
                    action: *action,
                    rank: index + usize::from(request_mode),
                    dimension: axis.kind,
                    score,
                    locality,
                    group_gain: 0.,
                    node_request: false,
                    request_transfer: false,
                });
            }
            if !group_traffic || nodes_balanced {
                preserves_higher &= !axis.drives_placement() || effect.preserves_balance();
            }
        }
        if let Some(candidate) = &mut choice
            && candidate.node_request
        {
            candidate.group_gain = group_gain;
        }
        Ok(choice)
    }

    fn leader_node(&self, group: u64) -> Option<u64> {
        Some(self.view.samples.leader(group)?.node_id)
    }

    fn pending_changes(&self) -> Changes {
        let mut pending = Changes::default();
        for action in &self.view.placements {
            let Some(change) = self.remaining_changes(action) else { continue };
            for (pending, change) in pending.placement.iter_mut().zip(change.placement) {
                pending.include_pending(change);
            }
            for (pending, change) in pending.requests.iter_mut().zip(change.requests) {
                pending.include_pending(change);
            }
        }
        pending
    }

    /// Project only the effects that are still missing from the observed
    /// topology.
    fn remaining_changes(&self, action: &Action) -> Option<Changes> {
        let mut changes = Changes::default();
        match action {
            Action::TransferLeader(action) => {
                let group = self.view.group(action.group)?;
                let leader = self.view.samples.leader(action.group)?;
                if leader.replica_id != action.source
                    || leader.term != action.version.term
                    || group.epoch != action.version.epoch
                {
                    return None;
                }
                let target = group.replicas.iter().find(|replica| {
                    replica.id == action.target && replica.role() == ReplicaRole::Voter
                })?;
                if !self.view.available.contains(&target.node_id) {
                    return None;
                }
                if group.id == ROOT_GROUP_ID {
                    changes.placement[Dimension::LeaderCount as usize].transfer(
                        leader.node_id,
                        target.node_id,
                        Some(1.),
                    );
                } else {
                    changes = self.changes(&PlacementAction::Leader(TransferLeader {
                        group: group.id,
                        src_replica: leader.replica_id,
                        src_node: leader.node_id,
                        target_replica: target.id,
                        target_node: target.node_id,
                    }));
                    let units = group.shards.iter().fold(RequestUnits::ZERO, |total, shard| {
                        total.add(Self::planning_shard_units(
                            &self.view.samples,
                            group.id,
                            shard.id,
                        ))
                    });
                    changes.requests = Default::default();
                    self.request_change(
                        &mut changes,
                        Some(leader.node_id),
                        Some(target.node_id),
                        units,
                    );
                }
            }
            Action::MoveShard(action) => {
                let desc = &action.desc;
                let shard = desc.get_shard_desc();
                let source = self.view.group(desc.src_group_id)?;
                let target = self.view.group(desc.dest_group_id)?;
                let target_reports_shard = target.shard(shard.id) == Some(shard);
                if source.shard(shard.id) != Some(shard) {
                    if !target_reports_shard {
                        self.storage_changes(
                            &mut changes,
                            [],
                            target.replicas.iter().map(|r| r.node_id),
                            None,
                        );
                        return Some(changes);
                    }
                    return None;
                }
                if target_reports_shard {
                    // Both descriptors may report the range before source cleanup.
                    let sample = self.view.samples.shard(shard.id);
                    let bytes = sample.bytes.map(|bytes| bytes as f64);
                    let rates = sample.rates.map(|rates| rates.request_units()).unwrap_or_default();
                    for (kind, amount) in [
                        (Dimension::ShardCount, Some(1.)),
                        (Dimension::ShardSize, bytes),
                        (Dimension::ReadUnits, rates.read),
                        (Dimension::WriteUnits, rates.write),
                    ] {
                        if let Some(amount) = amount {
                            changes.placement[kind as usize].add(source.id, -amount, 0.);
                        }
                    }
                    self.storage_changes(
                        &mut changes,
                        source.replicas.iter().map(|r| r.node_id),
                        [],
                        bytes,
                    );
                } else {
                    if target.epoch != desc.dest_group_epoch
                        || self.view.samples.leader(target.id)?.term != action.target_term
                    {
                        return None;
                    }
                    changes = self.changes(&PlacementAction::Shard(ReallocateShard {
                        shard: shard.id,
                        source_group: source.id,
                        target_group: target.id,
                        split_bytes: None,
                    }));
                    changes.requests = Default::default();
                    self.request_change(
                        &mut changes,
                        self.leader_node(source.id),
                        self.leader_node(target.id),
                        Self::planning_shard_units(&self.view.samples, source.id, shard.id),
                    );
                }
            }
            Action::MoveReplica(action) => {
                let group = self.view.group(action.group)?;
                let source = group.replicas.iter().find(|r| r.id == action.outgoing)?;
                let target_active = self.view.available.contains(&action.target_node);
                let target = if let Some(learner) = action.learner {
                    let intent = ReplicaReplacement {
                        incoming_id: learner.id,
                        outgoing_id: source.id,
                        target_node: action.target_node,
                        add_epoch: learner.epoch,
                    };
                    match replacement_step(group, intent, target_active).ok()? {
                        ReplacementStep::AddLearner
                        | ReplacementStep::CatchUpAndReplace
                        | ReplacementStep::LeaveJoint => {}
                        ReplacementStep::RemoveLearner(id) if id == source.id => {}
                        _ => return None,
                    }
                    group.replicas.iter().find(|r| r.id == learner.id)
                } else {
                    if !target_active {
                        return None;
                    }
                    None
                };
                let count = &mut changes.placement[Dimension::ReplicaCount as usize];
                count.add(source.node_id, -1., 0.);
                if target.is_none() {
                    count.add(action.target_node, 1., 0.);
                }
                let needs_copy =
                    target.is_none_or(|replica| replica.role() == ReplicaRole::Learner);
                self.storage_changes(
                    &mut changes,
                    [source.node_id],
                    needs_copy.then_some(action.target_node),
                    self.group_sizes.get(&group.id).map(|&bytes| bytes as f64),
                );
            }
            Action::SplitShard(action) => {
                let group = self.view.group(action.group)?;
                if group.epoch != action.epoch
                    || group.shard(action.shard.id) != Some(&action.shard)
                {
                    return None;
                }
                changes.placement[Dimension::ShardCount as usize].add(group.id, 1., 0.);
            }
            Action::MergeShards(action) => {
                let group = self.view.group(action.group)?;
                if group.epoch != action.version.epoch
                    || self.view.samples.leader(group.id)?.term != action.version.term
                    || group.shard(action.left.id) != Some(&action.left)
                    || group.shard(action.right.id) != Some(&action.right)
                {
                    return None;
                }
                changes.placement[Dimension::ShardCount as usize].add(group.id, -1., 0.);
            }
        }
        Some(changes)
    }

    fn changes(&self, action: &PlacementAction) -> Changes {
        let mut changes: Changes = Default::default();
        match action {
            PlacementAction::Shard(movement) => {
                let split_bytes = movement.split_bytes;
                let source = self.view.group(movement.source_group).unwrap();
                let target = self.view.group(movement.target_group).unwrap();
                let sample = self.view.samples.shard(movement.shard);
                let bytes = split_bytes.or(sample.bytes).map(|size| size as f64);
                let scale = match (split_bytes, sample.bytes) {
                    // Recheck the new piece's own samples before migrating it.
                    (Some(piece), Some(whole)) => piece as f64 / whole as f64,
                    _ => 1.,
                };
                let rates = sample.rates;
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
                    changes.placement[kind as usize].transfer(source.id, target.id, amount);
                }
                if split_bytes.is_some() {
                    // The source has an extra shard until a piece migrates.
                    changes.placement[Dimension::ShardCount as usize].add(source.id, 1., 1.);
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
                if movement.split_bytes.is_none() {
                    self.request_change(
                        &mut changes,
                        self.leader_node(source.id),
                        self.leader_node(target.id),
                        self.view.samples.shard_request_units(source.id, movement.shard),
                    );
                }
            }
            PlacementAction::Replica(movement) => {
                let source = movement.source_node;
                let target = movement.target_node;
                changes.placement[Dimension::ReplicaCount as usize].transfer(
                    source,
                    target,
                    Some(1.),
                );
                self.storage_changes(
                    &mut changes,
                    [source],
                    [target],
                    self.group_sizes.get(&movement.group).map(|&bytes| bytes as f64),
                );
            }
            PlacementAction::Leader(transfer) => {
                changes.placement[Dimension::LeaderCount as usize].transfer(
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
                    .and_then(|rates| rates.request_units().total());
                self.serving_change(
                    &mut changes,
                    Some(transfer.src_node),
                    Some(transfer.target_node),
                    units,
                );
                self.request_change(
                    &mut changes,
                    Some(transfer.src_node),
                    Some(transfer.target_node),
                    self.view.samples.group_request_units(group),
                );
            }
        }
        changes
    }

    fn request_change(
        &self,
        changes: &mut Changes,
        source: Option<u64>,
        target: Option<u64>,
        units: RequestUnits,
    ) {
        if !self.config.enable_node_request_balance {
            return;
        }
        let transfer = |change: &mut Change, amount| {
            if let Some((source, target)) = source.zip(target) {
                change.transfer(source, target, amount);
            } else {
                change.unknown = amount != Some(0.);
            }
        };
        for (change, kind) in changes.requests.iter_mut().zip(Dimension::REQUESTS) {
            transfer(change, units.get(kind));
        }
    }

    fn serving_change(
        &self,
        changes: &mut Changes,
        source: Option<u64>,
        target: Option<u64>,
        units: Option<f64>,
    ) {
        let change = &mut changes.placement[Dimension::Cpu as usize];
        let Some((source, target)) = source.zip(target) else {
            change.unknown = units != Some(0.);
            if let Some(target) = target.filter(|_| change.unknown) {
                change.mark_unknown_receiver(self.resource_node(Dimension::Cpu, target));
            }
            return;
        };
        let source_domain = self.resource_node(Dimension::Cpu, source);
        let target_domain = self.resource_node(Dimension::Cpu, target);
        if source_domain == target_domain || units == Some(0.) {
            return;
        }
        let projected = || {
            let units = units?;
            let total = self.serving_units.get(&source).copied().flatten()?.max(units);
            let src = self.resources.get(&source)?;
            let cores = src.process_cpu_used?.min(src.cpu_used?) * units / total;
            let src_capacity = self.resources.get(&source_domain)?.cpu_capacity;
            let dest_capacity = self.resources.get(&target_domain)?.cpu_capacity;
            Some((percent(cores, src_capacity)?, percent(cores, dest_capacity)?))
        };
        if let Some((release, receive)) = projected() {
            change.add(source_domain, -release, 0.);
            change.add(target_domain, receive, 0.);
        } else {
            change.mark_unknown_receiver(target_domain);
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
            changes.placement[Dimension::Disk as usize].unknown = true;
            changes.placement[Dimension::Memory as usize].unknown = true;
            for target in destinations {
                for kind in [Dimension::Disk, Dimension::Memory] {
                    changes.placement[kind as usize]
                        .mark_unknown_receiver(self.resource_node(kind, target));
                }
            }
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
            let disk_node = self.resource_node(Dimension::Disk, source);
            let memory_node = self.resource_node(Dimension::Memory, source);
            let disk = self
                .resources
                .get(&disk_node)
                .and_then(|usage| percent(bytes, usage.disk_capacity as f64));
            if let Some(disk) = disk {
                changes.placement[Dimension::Disk as usize].add(disk_node, -disk, 0.);
            }
            let memory = resource.and_then(|usage| {
                let stored = (*self.storage_sizes.get(&source)? as f64).max(bytes);
                let resident = (usage.process_memory_used as f64).min(stored);
                let part = resident * bytes / stored;
                source_memory.push((source, part));
                incoming_memory = Some(incoming_memory.map_or(part, |known| known.max(part)));
                percent(part, self.resources.get(&memory_node)?.memory_capacity as f64)
            });
            if let Some(memory) = memory {
                changes.placement[Dimension::Memory as usize].add(memory_node, -memory, 0.);
            }
        }
        for target in destinations {
            let disk_node = self.resource_node(Dimension::Disk, target);
            let memory_node = self.resource_node(Dimension::Memory, target);
            if let Some(disk) = self
                .resources
                .get(&disk_node)
                .and_then(|usage| percent(bytes, usage.disk_capacity as f64))
            {
                changes.placement[Dimension::Disk as usize].add(disk_node, disk, disk);
            } else {
                changes.placement[Dimension::Disk as usize].mark_unknown_receiver(disk_node);
            }
            let incoming = source_memory
                .iter()
                .find(|&&(node, _)| node == target)
                .map(|&(_, bytes)| bytes)
                .or(incoming_memory);
            if let Some(memory) = incoming
                .zip(self.resources.get(&memory_node))
                .and_then(|(part, usage)| percent(part, usage.memory_capacity as f64))
            {
                changes.placement[Dimension::Memory as usize].add(memory_node, memory, memory);
            } else {
                changes.placement[Dimension::Memory as usize].mark_unknown_receiver(memory_node);
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
