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
    GroupDesc, NodeDesc, NodeResourceUsage, RaftRole, ReplicaRole, ShardDesc,
};

use super::balance::{Axis, Change};
use super::{
    AllocSource, Allocator, BalanceDecision, BalanceThresholds, ReallocateReplica, ReallocateShard,
    ShardSizePlacement, TransferLeader,
};
use crate::constants::ROOT_GROUP_ID;
use crate::root::ClusterStats;
use crate::root::stats::ShowSnapshot;
use crate::{BalanceConfig, BalanceDimension as Dimension, RootConfig};

#[derive(Clone)]
pub(in crate::root) enum PlacementAction {
    Shard(ReallocateShard),
    Leader(TransferLeader),
    Replica(ReallocateReplica),
}

impl PlacementAction {
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
                migration.target_node.id,
            ),
        }
    }

    fn tie_breaker(&self) -> (u8, u64, u64, u64) {
        match self {
            Self::Leader(a) => (0, a.group, a.src_node, a.target_node),
            Self::Shard(a) => (1, a.shard, a.source_group, a.target_group),
            Self::Replica(a) => (2, a.group, a.source_node, a.target_node.id),
        }
    }
}

pub(in crate::root) struct PlacementPlan {
    pub action: Option<PlacementAction>,
    pub decisions: Vec<(Dimension, BalanceDecision, Option<BalanceThresholds>)>,
}

struct Placement<'a, T: AllocSource> {
    allocator: &'a Allocator<T>,
    groups: &'a [GroupDesc],
    nodes: HashMap<u64, NodeDesc>,
    resources: HashMap<u64, NodeResourceUsage>,
    leaders: HashMap<u64, u64>,
    group_sizes: HashMap<u64, u64>,
    current_traffic_groups: HashSet<u64>,
    migration_ready_groups: HashSet<u64>,
    storage_sizes: HashMap<u64, u64>,
    serving_units: HashMap<u64, Option<f64>>,
    occupied: HashSet<(u64, u64)>,
    snapshot: ShowSnapshot,
    locality: Locality,
    axes: Vec<Axis>,
    duplicate_shards: bool,
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
    fn new(
        groups: &[GroupDesc],
        snapshot: &ShowSnapshot,
        current_traffic_groups: &HashSet<u64>,
        cfg: &BalanceConfig,
    ) -> Self {
        let mut locality = Self::default();
        let mut ranges: HashMap<u64, Vec<(u64, &ShardDesc)>> = HashMap::new();
        for group in groups {
            for shard in &group.shards {
                if shard.range.is_some() {
                    ranges.entry(shard.table_id).or_default().push((group.id, shard));
                }
                if !current_traffic_groups.contains(&group.id) {
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
    split_bytes: Option<u64>,
}

impl RankedAction {
    fn compare(&self, other: &Self) -> Ordering {
        other
            .rank
            .cmp(&self.rank)
            .then(self.split_bytes.is_none().cmp(&other.split_bytes.is_none()))
            .then_with(|| self.score.0.total_cmp(&other.score.0))
            .then_with(|| self.score.1.total_cmp(&other.score.1))
            .then(self.score.2.cmp(&other.score.2))
            .then(self.locality.cmp(&other.locality))
            .then_with(|| other.action.tie_breaker().cmp(&self.action.tie_breaker()))
    }
}

impl<'a, T: AllocSource> Placement<'a, T> {
    pub fn new(
        allocator: &'a Allocator<T>,
        groups: &'a [GroupDesc],
        options: &BalanceConfig,
    ) -> Self {
        use super::source::NodeFilter;

        let max_age = Duration::from_secs(allocator.config.liveness_threshold_sec);
        let snapshot = allocator.cluster_stats.show_snapshot(groups, max_age);
        let nodes: HashMap<_, _> = allocator
            .alloc_source
            .nodes(NodeFilter::Schedulable)
            .into_iter()
            .map(|node| (node.id, node))
            .collect();
        let resources = allocator.cluster_stats.node_resources(max_age);
        let states = allocator.alloc_source.replica_states();
        let mut group_terms = HashMap::new();
        let mut occupied = HashSet::new();
        for state in states {
            occupied.insert((state.group_id, state.node_id));
            if state.role == RaftRole::Leader as i32 {
                let leader = group_terms.entry(state.group_id).or_insert((0, 0));
                *leader = (*leader).max((state.term, state.node_id));
            }
        }
        let mut leaders = HashMap::with_capacity(groups.len());
        let mut group_sizes = HashMap::with_capacity(groups.len());
        let mut current_traffic_groups = HashSet::new();
        let mut migration_ready_groups = HashSet::new();
        let mut storage_sizes = HashMap::<u64, u64>::new();
        let mut serving_units: HashMap<_, _> = nodes.keys().map(|&node| (node, Some(0.))).collect();
        let mut seen = HashSet::new();
        let mut duplicate_shards = false;
        for group in groups {
            let mut known_size = 0_u64;
            for shard in &group.shards {
                duplicate_shards |= !seen.insert(shard.id);
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

            let reported = snapshot.leader(group.id);
            let elected = group_terms.get(&group.id).copied().max(reported);
            if reported.is_some() && reported == elected && snapshot.migration_idle(group.id) {
                migration_ready_groups.insert(group.id);
            }
            let leader = elected.map(|(_, node)| node);
            let traffic_current =
                group.shards.is_empty() || (reported.is_some() && reported == elected);
            if traffic_current {
                current_traffic_groups.insert(group.id);
            }
            if let Some(leader) =
                leader.filter(|node| group.replicas.iter().any(|r| r.node_id == *node))
            {
                leaders.insert(group.id, leader);
                if let Some(total) = serving_units.get_mut(&leader) {
                    let units = if traffic_current {
                        sample.rates.and_then(|rates| {
                            rates.read_units_per_sec.zip(rates.write_units_per_sec)
                        })
                    } else {
                        None
                    };
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

        let locality = Locality::new(groups, &snapshot, &current_traffic_groups, options);
        let mut placement = Self {
            allocator,
            groups,
            nodes,
            resources,
            leaders,
            group_sizes,
            current_traffic_groups,
            migration_ready_groups,
            storage_sizes,
            serving_units,
            occupied,
            snapshot,
            locality,
            axes: Vec::new(),
            duplicate_shards,
        };
        placement.axes = options
            .dimensions()
            .filter(|kind| kind.enabled(&allocator.config))
            .map(|kind| placement.axis(kind, options.deadline(kind)))
            .collect();
        placement
    }

    fn axis(&self, kind: Dimension, deadline: Option<f64>) -> Axis {
        let (loads, expected, floor, relative) = match kind {
            Dimension::ReadUnits | Dimension::WriteUnits => {
                let loads = self
                    .groups
                    .iter()
                    .filter_map(|group| {
                        if !self.current_traffic_groups.contains(&group.id) {
                            return None;
                        }
                        let rates = self.snapshot.group(group).rates?;
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
                    self.config().shard_size_balance_tolerance_bytes as f64,
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
                    .nodes
                    .keys()
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
                (loads, self.nodes.len(), 5., 0.)
            }
            Dimension::LeaderCount | Dimension::ReplicaCount => {
                let loads = self
                    .nodes
                    .values()
                    .filter_map(|node| {
                        let capacity = node.capacity.as_ref()?;
                        let count = if kind == Dimension::LeaderCount {
                            capacity.leader_count as f64
                        } else {
                            let delta = self.stats().get_node_delta(node.id).replica_count;
                            (capacity.replica_count as f64 + delta as f64).max(0.)
                        };
                        Some((node.id, count))
                    })
                    .collect();
                let (floor, relative) =
                    if kind == Dimension::LeaderCount { (0.5, 0.) } else { (2., 0.05) };
                (loads, self.nodes.len(), floor, relative)
            }
        };
        Axis::new(kind, loads, expected, floor, relative, deadline)
    }

    fn config(&self) -> &RootConfig {
        &self.allocator.config
    }

    fn stats(&self) -> &ClusterStats {
        &self.allocator.cluster_stats
    }

    fn shard_moves_ready(&self, group: u64) -> bool {
        !self.duplicate_shards
            && !self.snapshot.migration_in_progress
            && self.migration_ready_groups.contains(&group)
    }

    pub fn plan(&self) -> PlacementPlan {
        let mut best = None::<RankedAction>;
        let mut blocked = BTreeSet::new();
        let migration_wait =
            self.stats().shard_migration_wait(&self.snapshot.active_migration_groups());
        let active_replica_change = self.stats().has_pending_replica_changes();
        if migration_wait.is_none() && !active_replica_change {
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
        }
        let no_action = if blocked.is_empty() {
            "no eligible move improves balance".to_owned()
        } else {
            let policies = blocked.into_iter().map(Dimension::name).collect::<Vec<_>>().join(", ");
            format!(
                "no eligible move improves balance. Deadlines or missing load data blocked candidates for {policies}"
            )
        };
        let report_wait = (self.migration_ready_groups.len() != self.groups.len()).then(|| {
            let groups = self
                .groups
                .iter()
                .filter(|group| !self.migration_ready_groups.contains(&group.id))
                .map(|group| {
                    if let Some(reason) = self.snapshot.migration_report_issue(group.id) {
                        format!("g{} {reason}", group.id)
                    } else if let Some(node) = self.leaders.get(&group.id) {
                        format!("g{} awaiting current leader report from n{node}", group.id)
                    } else {
                        format!("g{} awaiting current leader report", group.id)
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            format!("Waiting for current shard migration status\n{groups}")
        });
        let decisions = self
            .axes
            .iter()
            .enumerate()
            .map(|(index, axis)| {
                let decision = match &best {
                    Some(choice) if index == choice.rank => {
                        let detail = choice.split_bytes.map_or_else(
                            || choice.action.detail(),
                            |bytes| {
                                format!(
                                    "split before moving a piece of up to {bytes} bytes ({})",
                                    choice.action.detail()
                                )
                            },
                        );
                        BalanceDecision::ActionNeeded(detail)
                    }
                    Some(choice) if index > choice.rank => BalanceDecision::Deferred(
                        "higher-priority placement takes precedence".into(),
                    ),
                    _ if let Some(reason) = &migration_wait => {
                        BalanceDecision::Deferred(reason.clone().into())
                    }
                    _ if active_replica_change => {
                        BalanceDecision::Deferred("waiting for a replica change to settle".into())
                    }
                    _ if self.duplicate_shards && is_group_dimension(axis.kind) => {
                        BalanceDecision::Deferred("waiting for shard ownership to settle".into())
                    }
                    _ if is_group_dimension(axis.kind)
                        && let Some(reason) = &report_wait =>
                    {
                        BalanceDecision::Deferred(reason.clone().into())
                    }
                    _ => axis.report(&no_action),
                };
                (axis.kind, decision, axis.thresholds())
            })
            .collect();
        PlacementPlan { action: best.map(|choice| choice.action), decisions }
    }

    fn revalidate(&self, action: &PlacementAction) -> Option<RankedAction> {
        if self.stats().has_pending_replica_changes()
            || (!matches!(action, PlacementAction::Shard(_))
                && self.stats().has_pending_shard_moves())
            || !self.eligible(action)
        {
            return None;
        }
        self.rank(action).ok().flatten()
    }

    fn eligible(&self, action: &PlacementAction) -> bool {
        if self.snapshot.migration_in_progress {
            return false;
        }
        match action {
            PlacementAction::Shard(a) => {
                self.shard_moves_ready(a.source_group)
                    && self.group(a.source_group).is_some_and(|g| g.shard(a.shard).is_some())
                    && self.group(a.target_group).is_some_and(|g| self.receives_shards(g))
                    && !self.stats().is_replica_change_pending(a.source_group)
            }
            PlacementAction::Leader(a) => {
                self.leaders.get(&a.group) == Some(&a.src_node)
                    && self.nodes.contains_key(&a.target_node)
                    && self.group(a.group).is_some_and(|g| {
                        g.replicas.iter().any(|r| {
                            r.id == a.target_replica
                                && r.node_id == a.target_node
                                && r.role == ReplicaRole::Voter as i32
                        })
                    })
                    && !self.stats().is_replica_change_pending(a.group)
            }
            PlacementAction::Replica(a) => {
                self.shard_moves_ready(a.group)
                    && self.nodes.contains_key(&a.target_node.id)
                    && self.leaders.get(&a.group).is_some_and(|&node| node != a.source_node)
                    && !self.occupied.contains(&(a.group, a.target_node.id))
                    && self.group(a.group).is_some_and(|g| {
                        g.replicas.len() >= self.config().replicas_per_group
                            && !g.replicas.iter().any(|r| r.node_id == a.target_node.id)
                            && g.replicas.iter().any(|r| {
                                r.id == a.source_replica
                                    && r.node_id == a.source_node
                                    && r.role == ReplicaRole::Voter as i32
                            })
                    })
                    && !self.stats().is_replica_change_pending(a.group)
            }
        }
    }

    fn receives_shards(&self, group: &GroupDesc) -> bool {
        self.shard_moves_ready(group.id)
            && group.replicas.len() == self.config().replicas_per_group
            && self.leaders.contains_key(&group.id)
            && group
                .replicas
                .iter()
                .all(|r| r.role == ReplicaRole::Voter as i32 && self.nodes.contains_key(&r.node_id))
            && !self.stats().is_replica_change_pending(group.id)
    }

    fn shard_placement(&self, action: &ReallocateShard) -> Option<ShardSizePlacement> {
        let size = self.snapshot.shard(action.source_group, action.shard).bytes?;
        let limit = self.config().move_shard_size_limit;
        if size <= limit {
            return Some(ShardSizePlacement::Move);
        }
        if !self.config().enable_shard_size_balance
            || !self.config().enable_auto_shard_split
            || self.stats().is_shard_split_cooling_down(action.shard)
        {
            return None;
        }
        let source = *self.group_sizes.get(&action.source_group)?;
        let target = *self.group_sizes.get(&action.target_group)?;
        let piece = (source.saturating_sub(target) / 2).min(limit);
        (piece > 0).then_some(ShardSizePlacement::Split(piece))
    }

    fn visit_candidates(&self, mut visit: impl FnMut(PlacementAction)) {
        let can_donate = |id, from_group| {
            self.axes
                .iter()
                .any(|axis| is_group_dimension(axis.kind) == from_group && axis.may_donate(id))
        };
        let shard_targets: Vec<_> =
            self.groups.iter().filter(|group| self.receives_shards(group)).collect();
        for group in self.groups {
            let leader = self.leaders.get(&group.id).copied();
            if self.stats().is_replica_change_pending(group.id) {
                continue;
            }
            let donor_replica = group.replicas.iter().any(|r| can_donate(r.node_id, false));
            if self.shard_moves_ready(group.id) && (donor_replica || can_donate(group.id, true)) {
                for shard in &group.shards {
                    if self.stats().is_shard_scheduled(shard.id) {
                        continue;
                    }
                    for target in &shard_targets {
                        if target.id != group.id {
                            visit(PlacementAction::Shard(ReallocateShard {
                                shard: shard.id,
                                source_group: group.id,
                                target_group: target.id,
                            }));
                        }
                    }
                }
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
                    for voter in &group.replicas {
                        if voter.node_id != replica.node_id
                            && voter.role == ReplicaRole::Voter as i32
                            && self.nodes.contains_key(&voter.node_id)
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
                } else if leader.is_some()
                    && self.shard_moves_ready(group.id)
                    && group.replicas.len() >= self.config().replicas_per_group
                {
                    for target in self.nodes.values() {
                        if !self.occupied.contains(&(group.id, target.id))
                            && !group.replicas.iter().any(|r| r.node_id == target.id)
                        {
                            visit(PlacementAction::Replica(ReallocateReplica {
                                group: group.id,
                                source_replica: replica.id,
                                source_node: replica.node_id,
                                target_node: target.clone(),
                            }));
                        }
                    }
                }
            }
        }
    }

    fn rank(&self, action: &PlacementAction) -> Result<Option<RankedAction>, Dimension> {
        let split_bytes = if let PlacementAction::Shard(migration) = action {
            match self.shard_placement(migration) {
                Some(ShardSizePlacement::Move) => None,
                Some(ShardSizePlacement::Split(bytes)) => Some(bytes),
                None => return Ok(None),
            }
        } else {
            None
        };
        let changes = self.changes(action, split_bytes);
        let mut choice = None;
        let mut preserves_higher = true;
        for (rank, axis) in self.axes.iter().enumerate() {
            let effect = axis.effect(&changes[axis.kind as usize]).ok_or(axis.kind)?;
            if choice.is_none()
                && preserves_higher
                && (split_bytes.is_none() || axis.kind == Dimension::ShardSize)
                && let Some(score) = effect.score()
            {
                let locality = match action {
                    PlacementAction::Shard(movement) => self.locality.score(movement),
                    _ => 0,
                };
                choice = Some(RankedAction {
                    action: action.clone(),
                    rank,
                    score,
                    locality,
                    split_bytes,
                });
            }
            preserves_higher &= effect.preserves_balance();
        }
        Ok(choice)
    }

    fn group(&self, id: u64) -> Option<&GroupDesc> {
        self.groups.binary_search_by_key(&id, |group| group.id).ok().map(|idx| &self.groups[idx])
    }

    fn changes(&self, action: &PlacementAction, split_bytes: Option<u64>) -> Changes {
        let mut changes: Changes = Default::default();
        match action {
            PlacementAction::Shard(movement) => {
                let source = self.group(movement.source_group).unwrap();
                let target = self.group(movement.target_group).unwrap();
                let sample = self.snapshot.shard(source.id, movement.shard);
                let bytes = split_bytes.or(sample.bytes).map(|size| size as f64);
                let scale = match (split_bytes, sample.bytes) {
                    // Recheck the new piece's own samples before migrating it.
                    (Some(piece), Some(whole)) => piece as f64 / whole as f64,
                    _ => 1.,
                };
                let rates =
                    sample.rates.filter(|_| self.current_traffic_groups.contains(&source.id));
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
                    self.leaders.get(&source.id).copied(),
                    self.leaders.get(&target.id).copied(),
                    read.zip(write).map(|(r, w)| r + w),
                );
            }
            PlacementAction::Replica(movement) => {
                let source = movement.source_node;
                let target = movement.target_node.id;
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
                let group = self.group(transfer.group).unwrap();
                let units = self
                    .snapshot
                    .group(group)
                    .rates
                    .filter(|_| self.current_traffic_groups.contains(&group.id))
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
        let movement = ReallocateShard { shard: 3, source_group: 1, target_group: 2 };
        locality.hot.extend([(1, false), (2, false), (3, false)]);
        assert_eq!(locality.score(&movement), 1);
        locality.hot.extend([(1, true), (2, true), (3, true)]);
        assert_eq!(locality.score(&movement), -1);
        locality.hot.insert(2, false);
        assert_eq!(locality.score(&movement), 1);
    }
}

impl<T: AllocSource> Allocator<T> {
    pub(in crate::root) fn plan_placement(&self, options: &BalanceConfig) -> PlacementPlan {
        let mut groups: Vec<_> = self
            .alloc_source
            .groups()
            .into_values()
            .filter(|group| group.id != ROOT_GROUP_ID)
            .collect();
        groups.sort_unstable_by_key(|group| group.id);
        Placement::new(self, &groups, options).plan()
    }

    pub(in crate::root) fn validate_placement(
        &self,
        groups: &[GroupDesc],
        options: &BalanceConfig,
        action: &PlacementAction,
    ) -> bool {
        Placement::new(self, groups, options).revalidate(action).is_some()
    }

    pub(in crate::root) fn validate_placement_shard(
        &self,
        groups: &[GroupDesc],
        options: &BalanceConfig,
        action: ReallocateShard,
    ) -> Option<ShardSizePlacement> {
        let choice =
            Placement::new(self, groups, options).revalidate(&PlacementAction::Shard(action))?;
        Some(choice.split_bytes.map_or(ShardSizePlacement::Move, ShardSizePlacement::Split))
    }
}
