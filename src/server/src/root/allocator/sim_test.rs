// Copyright 2023-present The Sekas Authors.
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

use std::time::Duration;

use sekas_api::server::v1::*;

use super::placement::{PlacementChoice, PlacementPlan};
use super::*;
use crate::root::schedule::ClusterView;
use crate::{BalanceConfig, BalanceDimension, RootConfig};

struct Fixture {
    nodes: Vec<NodeDesc>,
    groups: Vec<GroupDesc>,
    states: Vec<ReplicaState>,
    stats: ClusterStats,
    config: RootConfig,
}

impl Fixture {
    fn new() -> Self {
        Self {
            nodes: vec![NodeDesc {
                id: 1,
                capacity: Some(NodeCapacity::default()),
                ..Default::default()
            }],
            groups: Vec::new(),
            states: Vec::new(),
            stats: ClusterStats::default(),
            config: RootConfig {
                replicas_per_group: 1,
                enable_shard_size_balance: true,
                shard_size_balance_tolerance_bytes: 0,
                move_shard_size_limit: 64,
                ..Default::default()
            },
        }
    }

    fn view(&self) -> ClusterView {
        ClusterView::new(
            self.nodes.clone(),
            self.groups.clone(),
            self.states.clone(),
            self.nodes.iter().map(|node| node.id).collect(),
            &self.stats,
            Duration::from_secs(self.config.liveness_threshold_sec),
        )
    }

    fn plan_placement(&self, options: &BalanceConfig) -> PlacementPlan {
        plan_round(&self.view(), &self.config, options, &self.stats).1
    }

    fn plan_capacity(
        &self,
        options: &BalanceConfig,
        creating: usize,
        copying_nodes: HashSet<u64>,
    ) -> Option<CapacityAction> {
        let mut view = self.view();
        view.creating = creating;
        view.copying_nodes = copying_nodes;
        plan_round(&view, &self.config, options, &self.stats).0.map(|choice| choice.action)
    }
}

fn group_with_sizes(id: u64, contents: &[(u64, u64)]) -> (GroupDesc, GroupStats) {
    let table_id = sekas_schema::FIRST_USER_TABLE_ID;
    let group = GroupDesc {
        id,
        epoch: 1,
        replicas: vec![ReplicaDesc { id, node_id: 1, role: ReplicaRole::Voter as i32 }],
        shards: contents.iter().map(|(shard, _)| ShardDesc::whole(*shard, table_id)).collect(),
        ..Default::default()
    };
    let stats = GroupStats {
        group_id: id,
        sample: Some(GroupStatsSample {
            group_epoch: group.epoch,
            replica_id: id,
            shard_migration: None,
            ..Default::default()
        }),
        shard_stats: contents
            .iter()
            .map(|&(shard_id, shard_size)| ShardStats {
                shard_id,
                table_id,
                shard_size,
                traffic: None,
                range: None,
            })
            .collect(),
        ..Default::default()
    };
    (group, stats)
}

#[test]
fn placement_uses_priority_and_checks_lower_priority_deadlines() {
    let mut fixture = Fixture::new();
    fixture.config.enable_shard_size_balance = false;
    fixture.config.enable_shard_balance = false;
    fixture.config.enable_leader_balance = false;
    fixture.config.enable_replica_balance = false;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    let (left, left_stats) = group_with_sizes(1, &[(1, 20), (2, 20), (3, 20)]);
    let (mut right, right_stats) = group_with_sizes(2, &[(4, 20)]);
    right.replicas[0].node_id = 2;
    fixture.nodes = (1..=2)
        .map(|id| NodeDesc { id, capacity: Some(NodeCapacity::default()), ..Default::default() })
        .collect();
    fixture.groups = vec![left, right];
    for (group, sample, rates) in
        [(1, left_stats, vec![(60, 55), (100, 10), (40, 45)]), (2, right_stats, vec![(0, 0)])]
    {
        let mut sample = sample;
        for shard in &mut sample.shard_stats {
            shard.traffic = Some(ShardTraffic {
                read_units: Some(0),
                write_units: Some(0),
                ..Default::default()
            });
        }
        fixture.stats.handle_show_group_stats(&sample, group);
        sample.sample.as_mut().unwrap().elapsed_nanos = 1_000_000_000;
        for (shard, (read, write)) in sample.shard_stats.iter_mut().zip(rates) {
            let traffic = shard.traffic.as_mut().unwrap();
            traffic.read_units = Some(read);
            traffic.write_units = Some(write);
        }
        fixture.stats.handle_show_group_stats(&sample, group);
    }
    let report_disk = |node, used| {
        fixture.stats.handle_node_resources(
            node,
            Some(NodeResourceUsage { disk_capacity: 100, disk_used: used, ..Default::default() }),
        );
    };
    report_disk(1, 80);
    report_disk(2, 20);

    let assert_shard = |options: &crate::BalanceConfig, dimension, id| {
        let plan = fixture.plan_placement(options);
        assert!(
            matches!(plan.choice, Some(PlacementChoice { dimension: policy, action: PlacementAction::Shard(movement), .. }) if policy == dimension && movement.shard == id && movement.target_group == 2)
        );
    };
    let mut options = crate::BalanceConfig::default();
    assert_shard(&options, BalanceDimension::ReadUnits, 2);
    options.set("priority", "write_units,read_units").unwrap();
    assert_shard(&options, BalanceDimension::WriteUnits, 1);
    options.set("deadline.read_units", "45").unwrap();
    assert_shard(&options, BalanceDimension::ReadUnits, 3);
    report_disk(2, 50);
    options.set("deadline.disk", "65").unwrap();
    let blocked = fixture.plan_placement(&options);
    assert!(blocked.choice.is_none());
    assert!(blocked.decisions.iter().any(|(kind, decision, _)| matches!(
        (kind, decision),
        (BalanceDimension::WriteUnits, BalanceDecision::NoAction(detail)) if detail.contains("read_units, disk")
    )));

    report_disk(1, 95);
    report_disk(2, 20);
    options.set("priority", "disk,write_units,read_units").unwrap();
    assert_shard(&options, BalanceDimension::Disk, 3);
    let plan = fixture.plan_placement(&options);
    assert!(matches!(
        plan.decisions[0],
        (BalanceDimension::Disk, BalanceDecision::ActionNeeded(_), _)
    ));
    assert!(plan.choice.unwrap().reason.contains("deadline"));
}

#[test]
fn placement_needs_migration_status_only_for_groups_in_the_move() {
    let mut fixture = Fixture::new();
    fixture.config.enable_read_balance = false;
    fixture.config.enable_write_balance = false;
    fixture.config.enable_leader_balance = false;
    fixture.config.enable_replica_balance = false;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;

    let (mut heavy, mut heavy_stats) = group_with_sizes(1, &[(1, 20), (2, 20), (3, 20)]);
    let (light, light_stats) = group_with_sizes(2, &[]);
    let (unreported, _) = group_with_sizes(3, &[]);
    for (shard, stats) in heavy.shards.iter().zip(&mut heavy_stats.shard_stats) {
        stats.range = shard.range.clone();
    }
    heavy.epoch = sekas_api::Epoch(heavy.epoch).apply_shard_delta().0;
    fixture.groups = vec![heavy, light, unreported];
    fixture.stats.handle_show_group_stats(&heavy_stats, 1);

    let options = crate::BalanceConfig::default();
    let waiting = fixture.plan_placement(&options);
    assert!(waiting.choice.is_none());
    assert!(waiting.decisions.iter().any(|(dimension, decision, _)| {
        *dimension == BalanceDimension::ShardSize
            && matches!(decision, BalanceDecision::Deferred(reason) if reason.contains("g2 no leader stats received"))
    }));
    fixture.stats.handle_show_group_stats(&light_stats, 1);
    let plan = fixture.plan_placement(&options);
    let Some(PlacementChoice {
        dimension: BalanceDimension::ShardSize,
        action: PlacementAction::Shard(action),
        ..
    }) = plan.choice
    else {
        panic!("placement must use the groups that have migration status");
    };
    assert_eq!((action.source_group, action.target_group), (1, 2));
    let mut light_stats = light_stats;
    if let Some(sample) = &mut light_stats.sample {
        sample.elapsed_nanos += 1;
        sample.shard_migration = Some(GroupShardMigration::default());
    }
    fixture.stats.handle_show_group_stats(&light_stats, 1);
    assert!(fixture.plan_placement(&options).choice.is_none());
}

#[test]
fn capacity_counts_inflight_groups_without_placing_into_them() {
    let mut fixture = Fixture::new();
    fixture.config.group_size_limit = 100;
    fixture.config.move_shard_size_limit = 64;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;
    let (first, first_stats) = group_with_sizes(1, &[(1, 75), (2, 35)]);
    let (second, second_stats) = group_with_sizes(2, &[(3, 70)]);
    fixture.groups = vec![first, second];
    fixture.stats.handle_show_group_stats(&first_stats, 1);
    fixture.stats.handle_show_group_stats(&second_stats, 1);
    let opts = crate::BalanceConfig::default();
    let plan = |creating| fixture.plan_capacity(&opts, creating, HashSet::new());
    assert!(matches!(plan(0), Some(CapacityAction::CreateGroup)));
    assert!(!matches!(plan(1), Some(CapacityAction::CreateGroup)));

    // A new Group can report a leader before its creation job has finished.
    let (third, third_stats) = group_with_sizes(3, &[]);
    fixture.groups.push(third);
    fixture.stats.handle_show_group_stats(&third_stats, 1);
    let mut view = fixture.view();
    view.creating = 1;
    view.unpublished.insert(3);
    assert_eq!(view.published_groups().count(), 2);
    let (capacity, _) = plan_round(&view, &fixture.config, &opts, &fixture.stats);
    assert!(
        matches!(capacity, Some(choice) if matches!(choice.action, CapacityAction::Place(action) if action.target_group == 2))
    );
}

#[test]
fn capacity_reuses_space_in_a_healthy_group() {
    let mut fixture = Fixture::new();
    fixture.config.group_size_limit = 100;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;
    let (first, first_stats) = group_with_sizes(1, &[(1, 75), (2, 35)]);
    let (second, second_stats) = group_with_sizes(2, &[(3, 40)]);
    fixture.groups = vec![first, second];
    fixture.stats.handle_show_group_stats(&first_stats, 1);
    fixture.stats.handle_show_group_stats(&second_stats, 1);
    let action = fixture.plan_capacity(&BalanceConfig::default(), 0, HashSet::new());
    assert!(matches!(action, Some(CapacityAction::Place(movement))
        if movement.shard == 2 && movement.source_group == 1 && movement.target_group == 2
            && movement.split_bytes.is_none()));
    let blocked = fixture.plan_capacity(&BalanceConfig::default(), 0, HashSet::from([1]));
    assert!(blocked.is_none(), "the existing copy on node 1 must reserve incoming capacity");

    fixture.config.enable_group_balance = true;
    fixture.config.enable_shard_size_balance = false;
    assert!(fixture.plan_capacity(&BalanceConfig::default(), 0, HashSet::new()).is_none());
}

#[test]
fn shard_move_limit_applies_to_capacity_and_size_balance() {
    let mut fixture = Fixture::new();
    fixture.config.group_size_limit = 100;
    fixture.config.enable_read_balance = false;
    fixture.config.enable_write_balance = false;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;
    let (heavy, heavy_stats) = group_with_sizes(1, &[(1, 75), (2, 35)]);
    let (light, light_stats) = group_with_sizes(2, &[(3, 40)]);
    fixture.groups = vec![heavy, light];
    fixture.stats.handle_show_group_stats(&heavy_stats, 1);
    fixture.stats.handle_show_group_stats(&light_stats, 1);

    let opts = crate::BalanceConfig::default();
    for (limit, expected_split) in [(64, None), (24, Some(24))] {
        fixture.config.move_shard_size_limit = limit;
        let capacity = fixture.plan_capacity(&opts, 0, HashSet::new());
        let balance = fixture.plan_placement(&opts);
        let Some(CapacityAction::Place(capacity)) = capacity else {
            panic!("capacity should find a placement at move limit {limit}");
        };
        let Some(PlacementChoice {
            dimension: BalanceDimension::ShardSize,
            action: PlacementAction::Shard(balance),
            ..
        }) = balance.choice
        else {
            panic!("size balance should find a placement at move limit {limit}");
        };
        for action in [capacity, balance] {
            assert_eq!((action.source_group, action.target_group), (1, 2));
            assert_eq!(action.split_bytes, expected_split);
        }
    }

    fixture.config.move_shard_size_limit = 0;
    assert!(
        fixture.plan_capacity(&opts, 0, HashSet::new()).is_none(),
        "an empty group would still have no move budget"
    );
    assert!(fixture.plan_placement(&opts).choice.is_none());
}

#[test]
fn placement_ignores_an_old_leader_report_after_observing_a_newer_term() {
    let mut fixture = Fixture::new();
    fixture.config.enable_read_balance = false;
    fixture.config.enable_write_balance = false;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;

    for (id, shards) in [(1, &[(10, 40), (11, 20)][..]), (2, &[][..]), (3, &[][..])] {
        let (group, mut stats) = group_with_sizes(id, shards);
        stats.sample.as_mut().unwrap().term = 2;
        fixture.groups.push(group);
        fixture.stats.handle_show_group_stats(&stats, 1);
    }
    let options = BalanceConfig::default();
    let target = |fixture: &Fixture| match fixture.plan_placement(&options).choice {
        Some(PlacementChoice {
            dimension: BalanceDimension::ShardSize,
            action: PlacementAction::Shard(action),
            ..
        }) => action.target_group,
        _ => panic!("an independent healthy target should remain available"),
    };
    assert_eq!(target(&fixture), 2);

    fixture.states.push(ReplicaState {
        group_id: 2,
        replica_id: 2,
        node_id: 1,
        term: 3,
        role: RaftRole::Follower as i32,
        ..Default::default()
    });
    assert!(fixture.view().samples.leader(2).is_none());
    assert_eq!(target(&fixture), 3);

    let (_, mut stats) = group_with_sizes(2, &[]);
    stats.sample.as_mut().unwrap().term = 3;
    fixture.stats.handle_show_group_stats(&stats, 1);
    assert_eq!(fixture.view().samples.leader(2).unwrap().term, 3);
    assert_eq!(target(&fixture), 2);
}
