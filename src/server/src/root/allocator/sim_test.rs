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
use crate::root::schedule::action::{
    Action, GroupVersion, MoveShard, TransferLeader as TransferLeaderJob,
};
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
                enable_node_request_balance: false,
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

    fn report_requests(&self, group: u64, leader: u64, term: u64, units: u64, baseline_only: bool) {
        self.report_traffic(group, leader, term, (0, units), baseline_only);
    }

    fn report_traffic(
        &self,
        group: u64,
        leader: u64,
        term: u64,
        (reads, writes): (u64, u64),
        baseline_only: bool,
    ) {
        let group = self.groups.iter().find(|candidate| candidate.id == group).unwrap();
        let replica = group.replicas.iter().find(|replica| replica.node_id == leader).unwrap();
        let mut report = GroupStats {
            group_id: group.id,
            shard_count: group.shards.len() as u64,
            sample: Some(GroupStatsSample {
                replica_id: replica.id,
                group_epoch: group.epoch,
                term,
                elapsed_nanos: 1_000_000_000,
                ..Default::default()
            }),
            shard_stats: group
                .shards
                .iter()
                .map(|shard| ShardStats {
                    shard_id: shard.id,
                    table_id: shard.table_id,
                    shard_size: 32,
                    range: shard.range.clone(),
                    traffic: Some(ShardTraffic {
                        read_units: Some(0),
                        write_units: Some(0),
                        ..Default::default()
                    }),
                })
                .collect(),
            ..Default::default()
        };
        self.stats.handle_show_group_stats(&report, leader);
        if !baseline_only {
            report.sample.as_mut().unwrap().elapsed_nanos += 1_000_000_000;
            for shard in &mut report.shard_stats {
                let traffic = shard.traffic.as_mut().unwrap();
                traffic.read_units = Some(reads);
                traffic.write_units = Some(writes);
            }
            self.stats.handle_show_group_stats(&report, leader);
        }
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

fn request_fixture(node_count: u64, voters: &[&[u64]]) -> Fixture {
    let mut fixture = Fixture::new();
    fixture.config.enable_node_request_balance = true;
    fixture.config.enable_shard_size_balance = false;
    fixture.config.enable_shard_balance = false;
    fixture.config.enable_replica_balance = false;
    fixture.config.enable_cpu_balance = false;
    fixture.config.enable_memory_balance = false;
    fixture.config.enable_disk_balance = false;
    fixture.config.replicas_per_group = voters[0].len();
    fixture.nodes = (0..node_count).map(|id| NodeDesc { id, ..Default::default() }).collect();
    for (index, voters) in voters.iter().enumerate() {
        let id = index as u64 + 1;
        let (mut group, _) = group_with_sizes(id, &[(id, 32)]);
        group.replicas = voters
            .iter()
            .map(|&node_id| ReplicaDesc {
                id: id * 10 + node_id,
                node_id,
                role: ReplicaRole::Voter as i32,
            })
            .collect();
        fixture.groups.push(group);
    }
    fixture
}

#[test]
fn requests_rebalance_the_observed_four_node_placement() {
    let voters: &[&[u64]] =
        &[&[0, 1, 2], &[1, 2, 3], &[1, 2, 3], &[1, 2, 3], &[0, 1, 3], &[0, 1, 3], &[0, 1, 3]];
    let mut leaders = [1, 1, 3, 1, 1, 1, 1];
    let writes = [700, 703, 776, 702, 689, 701, 1525];
    let mut terms = [1; 7];
    let mut fixture = request_fixture(4, voters);
    fixture.config.enable_hot_shard_split = false;
    for index in 0..leaders.len() {
        fixture.report_requests(
            index as u64 + 1,
            leaders[index],
            terms[index],
            writes[index],
            false,
        );
    }
    let options = BalanceConfig::default();
    let before = fixture.plan_placement(&options);
    assert_eq!(before.requests[&2], Some(0.));
    assert_eq!(before.requests[&1], Some(5020.));
    let mut moves = 0;
    for _ in 0..8 {
        let plan = fixture.plan_placement(&options);
        let Some(choice) = plan.choice else { break };
        if !choice.node_request {
            break;
        }
        let PlacementAction::Leader(transfer) = choice.action else {
            panic!("expected a leader transfer")
        };
        let index = transfer.group as usize - 1;
        assert!(voters[index].contains(&transfer.target_node));
        leaders[index] = transfer.target_node;
        terms[index] += 1;
        fixture.report_requests(transfer.group, leaders[index], terms[index], writes[index], false);
        moves += 1;
    }
    let after = fixture.plan_placement(&options);
    assert!(after.choice.is_none_or(|choice| !choice.node_request));
    assert_eq!(moves, 4);
    assert_eq!(after.requests.values().copied().flatten().fold(0., f64::max), 1525.);
    assert!(after.requests.values().all(|load| load.unwrap() > 1300.));
}

#[test]
fn requests_use_capacity_weights_and_wait_for_a_new_term() {
    let fixture = request_fixture(2, &[&[0, 1], &[0, 1]]);
    fixture.report_requests(1, 0, 1, 60, false);
    fixture.report_requests(2, 0, 1, 30, false);
    let mut options = BalanceConfig::default();
    options.node_request_weights.insert("1".into(), 2.);
    let plan = fixture.plan_placement(&options);
    assert!(matches!(plan.choice,
        Some(PlacementChoice { action: PlacementAction::Leader(transfer), dimension: BalanceDimension::WriteUnits, node_request: true, .. })
        if transfer.group == 1 && transfer.target_node == 1));
    fixture.report_requests(1, 1, 2, 60, true);
    let warming = fixture.plan_placement(&options);
    assert_eq!(warming.requests[&1], None);
    assert!(warming.choice.is_none());
    let view = fixture.view();
    let group = &fixture.groups[0];
    assert_eq!(view.samples.leader(group.id).unwrap().node_id, 1);
    assert_eq!(view.samples.group(group).rates.unwrap().request_units().write, Some(60.));
    fixture.report_requests(1, 1, 2, 60, false);
    let balanced = fixture.plan_placement(&options);
    assert_eq!(balanced.requests[&0], Some(30.));
    assert_eq!(balanced.requests[&1], Some(60.));
    assert!(balanced.choice.is_none_or(|choice| !choice.node_request));
}

#[test]
fn pending_transfer_projects_load_and_allows_independent_placement() {
    let fixture = request_fixture(3, &[&[0, 1, 2], &[0, 1, 2], &[0, 1, 2]]);
    for group in &fixture.groups {
        fixture.report_requests(group.id, 0, 1, 60, false);
    }
    let options = BalanceConfig::default();
    let action = Action::TransferLeader(TransferLeaderJob {
        group: 1,
        source: 10,
        target: 11,
        version: GroupVersion { epoch: fixture.groups[0].epoch, term: 1 },
        ..Default::default()
    });
    let next = || {
        let mut view = fixture.view();
        view.track_placement(action.clone());
        plan_round(&view, &fixture.config, &options, &fixture.stats).1
    };
    let assert_next_target = |plan: &PlacementPlan| {
        assert!(matches!(
            &plan.choice,
            Some(PlacementChoice {
                action: PlacementAction::Leader(transfer),
                dimension: BalanceDimension::WriteUnits,
                node_request: true,
                ..
            }) if transfer.group != 1 && transfer.target_node == 2
        ));
    };
    let measured_max = |plan: &PlacementPlan| {
        plan.decisions
            .iter()
            .find(|(dimension, ..)| *dimension == BalanceDimension::WriteUnits)
            .and_then(|(_, _, thresholds)| thresholds.as_ref().map(|thresholds| thresholds.max))
    };
    let plan = next();
    assert_next_target(&plan);
    assert_eq!(measured_max(&plan), Some(180.));

    fixture.report_requests(1, 1, 2, 60, true);
    let warming = next();
    assert_next_target(&warming);
    assert_eq!(measured_max(&warming), None);

    fixture.report_requests(1, 1, 2, 60, false);
    let plan = next();
    assert_next_target(&plan);
    assert_eq!(measured_max(&plan), Some(120.));
}

#[test]
fn missing_initial_samples_pause_request_placement() {
    let fixture = request_fixture(3, &[&[0, 1, 2], &[0, 1, 2], &[0, 1, 2]]);
    for group in [1, 2] {
        fixture.report_requests(group, 0, 1, 60, false);
    }
    let options = BalanceConfig::default();
    let initial = fixture.plan_placement(&options);
    assert!(initial.choice.is_none());
    assert!(initial.decisions.iter().any(|(dimension, decision, _)| {
        *dimension == BalanceDimension::WriteUnits
            && matches!(decision, BalanceDecision::Deferred(reason) if reason.contains("samples"))
    }));
    fixture.report_requests(3, 0, 1, 60, false);
    assert!(fixture.plan_placement(&options).choice.is_some_and(|choice| choice.node_request));
}

#[test]
fn pending_shard_move_counts_the_range_once_after_target_admission() {
    let mut fixture = request_fixture(4, &[&[0], &[1], &[2], &[3]]);
    fixture.config.enable_hot_shard_split = false;
    for (index, group) in fixture.groups.iter_mut().enumerate() {
        if index < 2 {
            group.shards.push(ShardDesc::whole(10 + group.id, sekas_schema::FIRST_USER_TABLE_ID));
        } else {
            group.shards.clear();
        }
    }
    for group in &fixture.groups {
        fixture.report_requests(group.id, group.id - 1, 1, 50, false);
    }
    let source = &fixture.groups[0];
    let shard = source.shards[1].clone();
    let action = Action::MoveShard(MoveShard {
        desc: MoveShardDesc {
            src_group_id: 1,
            src_group_epoch: source.epoch,
            dest_group_id: 3,
            dest_group_epoch: fixture.groups[2].epoch,
            shard_desc: Some(shard.clone()),
        },
        target_term: 1,
    });
    let options = BalanceConfig::default();
    let assert_next = |fixture: &Fixture| {
        let mut view = fixture.view();
        view.track_placement(action.clone());
        let plan = plan_round(&view, &fixture.config, &options, &fixture.stats).1;
        assert!(matches!(
            plan.choice,
            Some(PlacementChoice {
                action: PlacementAction::Shard(movement),
                node_request: true,
                ..
            }) if movement.source_group == 2 && movement.target_group == 4
        ));
    };
    assert_next(&fixture);

    fixture.groups[2].shards.push(shard);
    fixture.groups[2].epoch = sekas_api::Epoch(fixture.groups[2].epoch).apply_shard_delta().0;
    fixture.report_requests(3, 2, 2, 50, true);
    assert_next(&fixture);
}

#[test]
fn equal_node_totals_do_not_hide_separate_read_and_write_hotspots() {
    let fixture = request_fixture(2, &[&[0, 1], &[0, 1], &[0, 1], &[0, 1]]);
    for (group, node, traffic) in
        [(1, 0, (60, 0)), (2, 0, (40, 0)), (3, 1, (0, 60)), (4, 1, (0, 40))]
    {
        fixture.report_traffic(group, node, 1, traffic, false);
    }
    let options = BalanceConfig::default();
    let first = fixture.plan_placement(&options);
    assert_eq!(first.requests[&0], Some(100.));
    assert_eq!(first.requests[&1], Some(100.));
    let first = first.choice.unwrap();
    let PlacementAction::Leader(transfer) = first.action else {
        panic!("expected leader transfer")
    };
    assert!(first.node_request);
    let traffic = [(60, 0), (40, 0), (0, 60), (0, 40)][transfer.group as usize - 1];
    fixture.report_traffic(transfer.group, transfer.target_node, 2, traffic, false);
    let second = fixture.plan_placement(&options).choice.unwrap();
    assert!(second.node_request);
    assert_ne!(first.dimension, second.dimension);
}

#[test]
fn node_read_placement_preserves_node_write_balance() {
    let mut fixture = request_fixture(2, &[&[0, 1], &[0], &[1]]);
    fixture.config.enable_hot_shard_split = false;
    fixture.config.move_shard_size_limit = 0;
    fixture.report_traffic(1, 0, 1, (60, 40), false);
    fixture.report_traffic(2, 0, 1, (40, 0), false);
    fixture.report_traffic(3, 1, 1, (0, 100), false);
    let options = BalanceConfig::default();
    assert!(fixture.plan_placement(&options).choice.is_none());
    fixture.config.enable_write_balance = false;
    assert!(matches!(
        fixture.plan_placement(&options).choice,
        Some(PlacementChoice {
            dimension: BalanceDimension::ReadUnits,
            node_request: true,
            action: PlacementAction::Leader(_),
            ..
        })
    ));
}

#[test]
fn group_traffic_can_improve_after_both_node_axes_are_balanced() {
    let mut fixture = request_fixture(2, &[&[0], &[0], &[1]]);
    fixture.groups[0].shards.push(ShardDesc::whole(10, sekas_schema::FIRST_USER_TABLE_ID));
    fixture.groups[1].shards.clear();
    fixture.report_traffic(1, 0, 1, (50, 0), false);
    fixture.report_traffic(2, 0, 1, (0, 0), false);
    fixture.report_traffic(3, 1, 1, (100, 0), false);
    let mut options = BalanceConfig::default();
    let plan = fixture.plan_placement(&options);
    assert_eq!(plan.requests[&0], Some(100.));
    assert_eq!(plan.requests[&1], Some(100.));
    assert!(matches!(plan.choice, Some(PlacementChoice {
        dimension: BalanceDimension::ReadUnits, priority: crate::root::allocator::PlacementPriority::Imbalance,
        node_request: false, action: PlacementAction::Shard(movement), ..
    }) if movement.source_group == 1 && movement.target_group == 2));

    options.set("deadline.read_units", "90").unwrap();
    assert!(matches!(fixture.plan_placement(&options).choice, Some(PlacementChoice {
        dimension: BalanceDimension::ReadUnits, priority: crate::root::allocator::PlacementPriority::LoadDeadline,
        node_request: false, action: PlacementAction::Shard(movement), ..
    }) if movement.source_group == 1 && movement.target_group == 2));
}

#[test]
fn requests_check_resource_deadlines_and_ignore_cpu_changes_within_a_shared_host() {
    let mut fixture = request_fixture(2, &[&[0, 1], &[0, 1]]);
    fixture.config.enable_cpu_balance = true;
    fixture.config.enable_hot_shard_split = false;
    fixture.report_requests(1, 0, 1, 70, false);
    fixture.report_requests(2, 0, 1, 60, false);
    for (node, used) in [(0, 80.), (1, 85.)] {
        fixture.stats.handle_node_resources(
            node,
            Some(NodeResourceUsage {
                cpu_used: Some(used),
                cpu_capacity: 100.,
                process_cpu_used: Some(30.),
                ..Default::default()
            }),
        );
    }
    let mut options = BalanceConfig::default();
    assert!(fixture.plan_placement(&options).choice.is_none());
    for id in ["0", "1"] {
        options.resource_domains.insert(
            id.into(),
            crate::ResourceDomains { cpu: Some("shared".into()), ..Default::default() },
        );
    }
    assert!(matches!(fixture.plan_placement(&options).choice,
        Some(PlacementChoice { dimension: BalanceDimension::WriteUnits, node_request: true,
            action: PlacementAction::Leader(transfer), .. }) if transfer.target_node == 1));
}

#[test]
fn requests_split_a_hot_shard_when_whole_moves_cannot_help() {
    let mut fixture = request_fixture(2, &[&[0, 1], &[0, 1]]);
    fixture.report_requests(1, 0, 1, 100, false);
    fixture.report_requests(2, 1, 1, 20, false);
    let options = BalanceConfig::default();
    let plan = fixture.plan_placement(&options);
    assert!(plan.choice.is_none());
    assert!(plan.hot_split.is_some_and(|split| split.group == 1 && split.shard == 1));
    fixture.config.enable_hot_shard_split = false;
    assert!(fixture.plan_placement(&options).hot_split.is_none());
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
