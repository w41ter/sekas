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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use sekas_api::server::v1::*;
use sekas_runtime::ExecutorOwner;

use super::*;
use crate::BalanceDimension;
use crate::constants::REPLICA_PER_GROUP;

fn size_balance_fixture() -> (Arc<MockInfoProvider>, Arc<ClusterStats>, Allocator<MockInfoProvider>)
{
    let source = Arc::new(MockInfoProvider::new());
    source.set_nodes(vec![NodeDesc {
        id: 1,
        capacity: Some(NodeCapacity::default()),
        ..Default::default()
    }]);
    let stats = Arc::new(ClusterStats::default());
    let cfg = RootConfig {
        replicas_per_group: 1,
        enable_shard_size_balance: true,
        shard_size_balance_tolerance_bytes: 0,
        move_shard_size_limit: 64,
        ..Default::default()
    };
    let allocator = Allocator::new(source.clone(), stats.clone(), cfg);
    (source, stats, allocator)
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
            shard_migration_in_progress: Some(false),
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
fn shard_size_can_change_counts_and_count_cannot_undo_the_byte_balance() {
    let (source, stats, allocator) = size_balance_fixture();
    let (heavy, heavy_stats) = group_with_sizes(1, &[(1, 40), (2, 30)]);
    let (light, light_stats) = group_with_sizes(2, &[(3, 5), (4, 5)]);
    source.set_groups(vec![heavy, light]);
    stats.handle_show_group_stats(&heavy_stats, 1);
    stats.handle_show_group_stats(&light_stats, 1);

    let plan = allocator.compute_shard_action();
    assert!(matches!(plan.size, BalanceDecision::ActionNeeded(_)));
    assert!(
        matches!(plan.count, BalanceDecision::Deferred(ref reason) if reason == "shard size takes priority")
    );
    assert!(matches!(
        plan.actions.as_slice(),
        [ShardAction::Migrate(ReallocateShard { shard: 2, source_group: 1, target_group: 2 })]
    ));

    stats.reset();
    let (heavy, heavy_stats) = group_with_sizes(1, &[(1, 40)]);
    let (light, light_stats) = group_with_sizes(2, &[(2, 30), (3, 5), (4, 5)]);
    source.set_groups(vec![heavy, light]);
    stats.handle_show_group_stats(&heavy_stats, 1);
    stats.handle_show_group_stats(&light_stats, 1);
    let plan = allocator.compute_shard_action();
    assert!(matches!(plan.size, BalanceDecision::NoAction(_)));
    assert!(matches!(plan.count, BalanceDecision::NoAction(_)));
    assert!(plan.actions.is_empty());
}

#[test]
fn shard_count_can_move_a_shard_that_preserves_byte_balance() {
    let (source, stats, mut allocator) = size_balance_fixture();
    allocator.config.enable_auto_shard_split = false;
    let (left, left_stats) = group_with_sizes(1, &[(1, 5), (2, 40), (3, 0)]);
    let (right, mut right_stats) = group_with_sizes(2, &[(4, 30)]);
    let (large, large_stats) = group_with_sizes(3, &[(5, 75)]);
    source.set_groups(vec![left, right, large]);
    stats.handle_show_group_stats(&left_stats, 1);
    stats.handle_show_group_stats(&large_stats, 1);
    right_stats.sample.as_mut().unwrap().group_epoch = 0;
    stats.handle_show_group_stats(&right_stats, 1);
    let plan = allocator.compute_shard_action();
    assert!(matches!(plan.size, BalanceDecision::Deferred(_)));
    assert!(matches!(plan.count, BalanceDecision::Deferred(_)));
    assert!(plan.actions.is_empty());

    right_stats.sample.as_mut().unwrap().group_epoch = 1;
    stats.handle_show_group_stats(&right_stats, 1);
    let plan = allocator.compute_shard_action();
    assert!(matches!(plan.size, BalanceDecision::NoAction(_)));
    assert!(matches!(plan.count, BalanceDecision::ActionNeeded(_)));
    // Moving five bytes to group 2 would balance counts at the same total
    // penalty, but it would push group 1 out of the byte tolerance band.
    assert!(matches!(
        plan.actions.as_slice(),
        [ShardAction::Migrate(ReallocateShard { shard: 3, source_group: 1, .. })]
    ));
}

#[test]
fn shard_size_plans_a_split_for_an_oversized_shard_only_if_a_piece_can_help() {
    let (source, stats, allocator) = size_balance_fixture();
    let (heavy, heavy_stats) = group_with_sizes(1, &[(1, 160)]);
    let (light, light_stats) = group_with_sizes(2, &[]);
    source.set_groups(vec![heavy, light]);
    stats.handle_show_group_stats(&heavy_stats, 1);
    stats.handle_show_group_stats(&light_stats, 1);
    let plan = allocator.compute_shard_action();
    assert!(
        matches!(plan.size, BalanceDecision::ActionNeeded(detail) if detail.contains("split shard 1"))
    );
    assert!(matches!(
        plan.actions.as_slice(),
        [ShardAction::Migrate(ReallocateShard { shard: 1, source_group: 1, target_group: 2 })]
    ));

    stats.defer_shard_split(1, Duration::from_secs(60));
    assert!(allocator.compute_shard_action().actions.is_empty());

    stats.reset();
    let (heavy, heavy_stats) = group_with_sizes(1, &[(1, 130)]);
    let (light, light_stats) = group_with_sizes(2, &[(2, 100)]);
    source.set_groups(vec![heavy, light]);
    stats.handle_show_group_stats(&heavy_stats, 1);
    stats.handle_show_group_stats(&light_stats, 1);
    let plan = allocator.compute_shard_action();
    assert!(
        matches!(plan.size, BalanceDecision::ActionNeeded(detail) if detail.contains("up to 15 bytes"))
    );
}

#[test]
fn placement_uses_priority_and_checks_lower_priority_deadlines() {
    let (source, stats, mut allocator) = size_balance_fixture();
    allocator.config.enable_shard_size_balance = false;
    allocator.config.enable_shard_balance = false;
    allocator.config.enable_leader_balance = false;
    allocator.config.enable_replica_balance = false;
    allocator.config.enable_cpu_balance = false;
    allocator.config.enable_memory_balance = false;
    let (left, left_stats) = group_with_sizes(1, &[(1, 20), (2, 20), (3, 20)]);
    let (mut right, right_stats) = group_with_sizes(2, &[(4, 20)]);
    right.replicas[0].node_id = 2;
    source.set_nodes(
        (1..=2)
            .map(|id| NodeDesc {
                id,
                capacity: Some(NodeCapacity::default()),
                ..Default::default()
            })
            .collect(),
    );
    source.set_groups(vec![left, right]);
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
        stats.handle_show_group_stats(&sample, group);
        sample.sample.as_mut().unwrap().elapsed_nanos = 1_000_000_000;
        for (shard, (read, write)) in sample.shard_stats.iter_mut().zip(rates) {
            let traffic = shard.traffic.as_mut().unwrap();
            traffic.read_units = Some(read);
            traffic.write_units = Some(write);
        }
        stats.handle_show_group_stats(&sample, group);
    }
    let report_disk = |node, used| {
        stats.handle_node_resources(
            node,
            Some(NodeResourceUsage { disk_capacity: 100, disk_used: used, ..Default::default() }),
        );
    };
    report_disk(1, 80);
    report_disk(2, 20);

    let assert_shard = |options: &crate::BalanceConfig, id| {
        let plan = allocator.plan_placement(options);
        assert!(
            matches!(plan.action, Some(PlacementAction::Shard(movement)) if movement.shard == id && movement.target_group == 2)
        );
    };
    let mut options = crate::BalanceConfig::default();
    assert_shard(&options, 2);
    options.set("priority", "write_units,read_units").unwrap();
    assert_shard(&options, 1);
    options.set("deadline.read_units", "45").unwrap();
    assert_shard(&options, 3);
    report_disk(2, 50);
    options.set("deadline.disk", "65").unwrap();
    let blocked = allocator.plan_placement(&options);
    assert!(blocked.action.is_none());
    assert!(blocked.decisions.iter().any(|(kind, decision, _)| matches!(
        (kind, decision),
        (BalanceDimension::WriteUnits, BalanceDecision::NoAction(detail)) if detail.contains("read_units, disk")
    )));

    report_disk(1, 95);
    report_disk(2, 20);
    options.set("priority", "disk,write_units,read_units").unwrap();
    assert_shard(&options, 3);
    assert!(matches!(
        allocator.plan_placement(&options).decisions[0],
        (BalanceDimension::Disk, BalanceDecision::ActionNeeded(_), _)
    ));
}

#[test]
fn placement_needs_migration_status_only_for_groups_in_the_move() {
    let (source, stats, mut allocator) = size_balance_fixture();
    allocator.config.enable_shard_size_balance = false;
    allocator.config.enable_read_balance = false;
    allocator.config.enable_write_balance = false;
    allocator.config.enable_leader_balance = false;
    allocator.config.enable_replica_balance = false;
    allocator.config.enable_cpu_balance = false;
    allocator.config.enable_memory_balance = false;
    allocator.config.enable_disk_balance = false;

    let (mut heavy, mut heavy_stats) = group_with_sizes(1, &[(1, 20), (2, 20), (3, 20)]);
    let (light, light_stats) = group_with_sizes(2, &[]);
    let (unreported, _) = group_with_sizes(3, &[]);
    for (shard, stats) in heavy.shards.iter().zip(&mut heavy_stats.shard_stats) {
        stats.range = shard.range.clone();
    }
    heavy.epoch = sekas_api::Epoch(heavy.epoch).apply_shard_delta().0;
    let groups = vec![heavy, light, unreported];
    source.set_groups(groups.clone());
    stats.handle_show_group_stats(&heavy_stats, 1);

    let options = crate::BalanceConfig::default();
    let waiting = allocator.plan_placement(&options);
    assert!(waiting.action.is_none());
    assert!(waiting.decisions.iter().any(|(dimension, decision, _)| {
        *dimension == BalanceDimension::ShardCount
            && matches!(decision, BalanceDecision::Deferred(reason) if reason.contains("g2 no leader stats received"))
    }));
    stats.handle_show_group_stats(&light_stats, 1);
    let plan = allocator.plan_placement(&options);
    let Some(PlacementAction::Shard(mut action)) = plan.action else {
        panic!("placement must use the groups that have migration status");
    };
    assert_eq!((action.source_group, action.target_group), (1, 2));
    assert!(allocator.validate_placement_shard(&groups, &options, action.clone()).is_some());
    action.target_group = 3;
    assert!(allocator.validate_placement_shard(&groups, &options, action).is_none());
}

#[test]
fn sim_boostrap_join_node_balance() {
    let executor_owner = ExecutorOwner::new(1);
    let executor = executor_owner.executor();
    executor.block_on(async {
        let p = Arc::new(MockInfoProvider::new());
        let d = Arc::new(ClusterStats::default());
        let config = RootConfig { enable_shard_size_balance: false, ..Default::default() };
        let a = Allocator::new(p.clone(), d.clone(), config);

        println!("1. boostrap and no need rebalance");
        p.set_groups(vec![GroupDesc {
            id: 1,
            epoch: 0,
            shards: vec![],
            replicas: vec![ReplicaDesc { id: 1, node_id: 1, role: ReplicaRole::Voter.into() }],
            ..Default::default()
        }]);
        p.set_nodes(vec![NodeDesc {
            id: 1,
            addr: "".into(),
            capacity: Some(NodeCapacity { cpu_nums: 2.0, replica_count: 1, leader_count: 1 }),
            status: NodeStatus::Active as i32,
        }]);
        p.set_replica_states(vec![ReplicaState {
            replica_id: 1,
            group_id: 1,
            term: 0,
            voted_for: 0,
            role: RaftRole::Leader.into(),
            node_id: 1,
        }]);

        let act = a.compute_group_action().0;
        assert!(matches!(act, GroupAction::Noop));
        p.display();

        println!("2. two node joined");
        let mut nodes = p.nodes(NodeFilter::All);
        nodes.extend_from_slice(&[
            NodeDesc {
                id: 2,
                addr: "".into(),
                capacity: Some(NodeCapacity { cpu_nums: 2.0, replica_count: 0, leader_count: 0 }),
                status: NodeStatus::Active as i32,
            },
            NodeDesc {
                id: 3,
                addr: "".into(),
                capacity: Some(NodeCapacity { cpu_nums: 2.0, replica_count: 0, leader_count: 0 }),
                status: NodeStatus::Active as i32,
            },
        ]);
        p.set_nodes(nodes);
        p.display();

        println!("3. group0 be repaired");
        p.set_groups(vec![GroupDesc {
            id: 1,
            epoch: 1,
            shards: vec![],
            replicas: vec![
                ReplicaDesc { id: 1, node_id: 1, role: ReplicaRole::Voter.into() },
                ReplicaDesc { id: 2, node_id: 2, role: ReplicaRole::Voter.into() },
                ReplicaDesc { id: 3, node_id: 3, role: ReplicaRole::Voter.into() },
            ],
            ..Default::default()
        }]);
        p.set_replica_states(vec![
            ReplicaState {
                replica_id: 1,
                group_id: 1,
                term: 1,
                voted_for: 0,
                role: RaftRole::Leader.into(),
                node_id: 1,
            },
            ReplicaState {
                replica_id: 2,
                group_id: 1,
                term: 1,
                voted_for: 0,
                role: RaftRole::Follower.into(),
                node_id: 2,
            },
            ReplicaState {
                replica_id: 3,
                group_id: 1,
                term: 1,
                voted_for: 0,
                role: RaftRole::Follower.into(),
                node_id: 3,
            },
        ]);
        p.display();

        let mut group_id_gen = 2;
        let mut replica_id_gen = 4;

        let act = a.compute_group_action().0;
        match act {
            GroupAction::Add(n) => {
                assert_eq!(n, 5); // Six CPUs and one existing group.
                for _ in 0..n {
                    let nodes = a.allocate_group_replica(vec![], REPLICA_PER_GROUP).await.unwrap();
                    println!(
                        "alloc group {} in {:?}",
                        group_id_gen,
                        nodes.iter().map(|n| n.id).collect::<Vec<u64>>()
                    );
                    let mut groups = p.groups();
                    let mut replica_states = p.replica_states();
                    let mut replicas = Vec::new();
                    let mut first = true;
                    for n in nodes {
                        replicas.push(ReplicaDesc {
                            id: replica_id_gen,
                            node_id: n.id,
                            role: ReplicaRole::Voter.into(),
                        });
                        let role = if first {
                            first = false;
                            RaftRole::Leader.into()
                        } else {
                            RaftRole::Follower.into()
                        };
                        replica_states.push(ReplicaState {
                            replica_id: replica_id_gen,
                            group_id: group_id_gen,
                            term: 0,
                            voted_for: 0,
                            role,
                            node_id: n.id,
                        });
                        replica_id_gen += 1;
                    }
                    groups.insert(
                        group_id_gen,
                        GroupDesc {
                            id: group_id_gen,
                            epoch: 0,
                            shards: vec![],
                            replicas,
                            ..Default::default()
                        },
                    );
                    p.set_groups(groups.values().map(ToOwned::to_owned).collect());
                    p.set_replica_states(replica_states);
                    group_id_gen += 1;
                }
            }
            _ => unreachable!(),
        }
        println!("4. group alloc works and group & replicas balanced");
        let act = a.compute_group_action().0;
        assert!(matches!(act, GroupAction::Noop));
        let ract = a.compute_replica_action().0;
        assert!(ract.is_empty());
        p.display();

        println!("5. assign shard in groups");
        let cg = a.place_group_for_shard(9, REPLICA_PER_GROUP).await.unwrap();
        for id in 0..9 {
            let group = cg.get(id % cg.len()).unwrap();
            p.assign_shard(group.id);
            println!("assign shard to group {}, prev_shard_cnt: {}", group.id, group.shards.len());
        }

        println!("6. node 4 joined");
        let mut nodes = p.nodes(NodeFilter::All);
        nodes.extend_from_slice(&[NodeDesc {
            id: 4,
            addr: "".into(),
            capacity: Some(NodeCapacity { cpu_nums: 2.0, replica_count: 0, leader_count: 0 }),
            status: NodeStatus::Active as i32,
        }]);
        p.set_nodes(nodes);
        p.display();

        println!("7. balance group for new node");
        let act = a.compute_group_action().0;
        match act {
            GroupAction::Add(n) => {
                assert_eq!(n, 2); // The new node adds two CPUs.
                for _ in 0..n {
                    let nodes = a.allocate_group_replica(vec![], REPLICA_PER_GROUP).await.unwrap();
                    println!(
                        "alloc group {} in {:?}",
                        group_id_gen,
                        nodes.iter().map(|n| n.id).collect::<Vec<u64>>()
                    );
                    let mut groups = p.groups();
                    let mut replica_states = p.replica_states();
                    let mut replicas = Vec::new();
                    let mut first = true;
                    for n in nodes {
                        replicas.push(ReplicaDesc {
                            id: replica_id_gen,
                            node_id: n.id,
                            role: ReplicaRole::Voter.into(),
                        });
                        let role = if first {
                            first = false;
                            RaftRole::Leader.into()
                        } else {
                            RaftRole::Follower.into()
                        };
                        replica_states.push(ReplicaState {
                            replica_id: replica_id_gen,
                            group_id: group_id_gen,
                            term: 0,
                            voted_for: 0,
                            role,
                            node_id: n.id,
                        });
                        replica_id_gen += 1;
                    }
                    groups.insert(
                        group_id_gen,
                        GroupDesc {
                            id: group_id_gen,
                            epoch: 0,
                            shards: vec![],
                            replicas,
                            ..Default::default()
                        },
                    );
                    p.set_groups(groups.values().map(ToOwned::to_owned).collect());
                    p.set_replica_states(replica_states);
                    group_id_gen += 1;
                }
            }
            _ => unreachable!(),
        }

        // cluster group balanced.
        let act = a.compute_group_action().0;
        assert!(matches!(act, GroupAction::Noop));
        p.display();

        println!("8. balance replica between nodes");
        loop {
            let racts = a.compute_replica_action().0;
            if racts.is_empty() {
                break;
            }
            for act in &racts {
                match act {
                    ReplicaAction::Migrate(ReallocateReplica {
                        group,
                        source_node: _,
                        source_replica,
                        target_node,
                    }) => {
                        println!(
                            "move group {} replica {} to {}",
                            group, source_replica, target_node.id
                        );
                        p.move_replica(*source_replica, target_node.id)
                    }
                }
            }
        }
        let (racts, decision, _) = a.compute_replica_action();
        assert!(racts.is_empty());
        let BalanceDecision::NoAction(detail) = decision else {
            panic!("balanced replica counts must report no action");
        };
        assert!(detail.contains("schedulable_nodes=4"), "{detail}");
        assert!(detail.contains("tolerance=2.00"), "{detail}");
        p.display();

        println!("9. balance shards between groups");
        let sact = a.compute_shard_action().actions;
        assert!(!sact.is_empty());
        for act in &sact {
            match act {
                ShardAction::Migrate(ReallocateShard { shard, source_group, target_group }) => {
                    println!("move shard {} from {} to {}", shard, source_group, target_group);
                    p.move_shards(
                        source_group.to_owned(),
                        target_group.to_owned(),
                        shard.to_owned(),
                    );
                }
            }
        }
        let sact = a.compute_shard_action().actions;
        assert!(!sact.is_empty());
        for act in &sact {
            match act {
                ShardAction::Migrate(ReallocateShard { shard, source_group, target_group }) => {
                    println!("move shard {} from {} to {}", shard, source_group, target_group);
                    p.move_shards(
                        source_group.to_owned(),
                        target_group.to_owned(),
                        shard.to_owned(),
                    );
                }
            }
        }
        let sact = a.compute_shard_action().actions;
        assert!(sact.is_empty());
        p.display();

        println!("10. try balance leader between nodes");
        loop {
            let lact = a.compute_leader_action().0;
            if lact.is_empty() {
                break;
            }
            for act in &lact {
                match act {
                    LeaderAction::Noop => unreachable!(),
                    LeaderAction::Shed(action) => {
                        println!(
                            "transfer group {} leader from {} to {}",
                            action.group, action.src_node, action.target_node,
                        );
                        p.transfer_leader(action.src_replica, action.target_replica);
                    }
                }
            }
        }
        p.display();

        println!("done");
    });
}

pub struct MockInfoProvider {
    nodes: Arc<Mutex<Vec<NodeDesc>>>,
    groups: Arc<Mutex<GroupInfo>>,
    replicas: Arc<Mutex<HashMap<u64, ReplicaState>>>,
    shard_id_gen: AtomicU64,
}

#[derive(Default)]
struct GroupInfo {
    descs: HashMap<u64, GroupDesc>,
    node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64)>>,
}

impl MockInfoProvider {
    pub fn new() -> Self {
        Self {
            nodes: Default::default(),
            groups: Default::default(),
            replicas: Default::default(),
            shard_id_gen: AtomicU64::new(1),
        }
    }
}

#[crate::async_trait]
impl AllocSource for MockInfoProvider {
    async fn refresh_all(&self) -> Result<()> {
        Ok(())
    }

    fn nodes(&self, _: NodeFilter) -> Vec<NodeDesc> {
        let nodes = self.nodes.lock().unwrap();
        nodes.to_owned()
    }

    fn groups(&self) -> HashMap<u64, GroupDesc> {
        let groups = self.groups.lock().unwrap();
        groups.descs.to_owned()
    }

    fn node_replicas(&self, node_id: &u64) -> Vec<(ReplicaDesc, u64)> {
        let groups = self.groups.lock().unwrap();
        groups.node_replicas.get(node_id).map(ToOwned::to_owned).unwrap_or_default()
    }

    fn replica_state(&self, replica_id: &u64) -> Option<ReplicaState> {
        let replica_info = self.replicas.lock().unwrap();
        replica_info.get(replica_id).map(ToOwned::to_owned)
    }

    fn replica_states(&self) -> Vec<ReplicaState> {
        let replica_info = self.replicas.lock().unwrap();
        replica_info.iter().map(|e| e.1.to_owned()).collect()
    }
}

impl MockInfoProvider {
    fn set_nodes(&self, ns: Vec<NodeDesc>) {
        let mut nodes = self.nodes.lock().unwrap();
        let _ = std::mem::replace(&mut *nodes, ns);
    }

    fn set_groups(&self, gs: Vec<GroupDesc>) {
        let mut groups = self.groups.lock().unwrap();
        let mut node_replicas: HashMap<u64, Vec<(ReplicaDesc, u64)>> = HashMap::new();
        for group in gs.iter() {
            for replica in &group.replicas {
                match node_replicas.entry(replica.node_id) {
                    Entry::Occupied(mut ent) => {
                        (*ent.get_mut()).push((replica.to_owned(), group.id.to_owned()));
                    }
                    Entry::Vacant(ent) => {
                        ent.insert(vec![(replica.to_owned(), group.id.to_owned())]);
                    }
                };
            }
        }

        // test only fix node.replica logic
        let mut nodes = self.nodes(NodeFilter::All);
        for n in nodes.iter_mut() {
            let mut cap = n.capacity.take().unwrap();
            cap.replica_count = node_replicas.get(&n.id).unwrap().len() as u64;
            n.capacity = Some(cap)
        }
        self.set_nodes(nodes);

        let descs = gs.into_iter().map(|g| (g.id, g)).collect();
        let _ = std::mem::replace(&mut *groups, GroupInfo { descs, node_replicas });
    }

    fn set_replica_states(&self, rs: Vec<ReplicaState>) {
        let mut replicas = self.replicas.lock().unwrap();

        // test only, maintain leader count in node.
        let mut nodes = self.nodes(NodeFilter::All);
        let groups = self.groups();
        let mut node_leader = HashMap::new();
        for r in &rs {
            if r.role != RaftRole::Leader as i32 {
                continue;
            }
            let group = groups.get(&r.group_id).unwrap();
            let desc = group.replicas.iter().find(|d| d.id == r.replica_id).unwrap();
            match node_leader.entry(&desc.node_id) {
                Entry::Occupied(mut ent) => {
                    let v = ent.get_mut();
                    *v += 1;
                }
                Entry::Vacant(ent) => {
                    ent.insert(1);
                }
            }
        }
        for n in nodes.iter_mut() {
            let mut cap = n.capacity.take().unwrap();
            cap.leader_count = node_leader.get(&n.id).map(ToOwned::to_owned).unwrap_or_default();
            n.capacity = Some(cap);
        }
        self.set_nodes(nodes);

        let id_to_state =
            rs.into_iter().map(|r| (r.replica_id, r)).collect::<HashMap<u64, ReplicaState>>();
        let _ = std::mem::replace(&mut *replicas, id_to_state);
    }

    pub fn move_replica(&self, replica_id: u64, node: u64) {
        let mut groups = self.groups();
        for group in groups.values_mut() {
            for replica in group.replicas.iter_mut() {
                if replica.id == replica_id {
                    replica.node_id = node;
                    break;
                }
            }
        }
        self.set_groups(groups.values().map(ToOwned::to_owned).collect());
    }

    pub fn transfer_leader(&self, src_replica: u64, desc_replica: u64) {
        let mut states = self.replica_states();
        for state in states.iter_mut() {
            if state.replica_id == src_replica {
                state.role = RaftRole::Follower.into();
            }
            if state.replica_id == desc_replica {
                state.role = RaftRole::Leader.into();
            }
        }
        self.set_replica_states(states);
    }

    pub fn move_shards(&self, sgroup: u64, tgroup: u64, shard: u64) {
        let mut groups = self.groups();

        let mut shard_desc = None;
        for group in groups.values_mut() {
            if group.id == sgroup {
                group.shards.retain(|s| {
                    if s.id == shard {
                        shard_desc = Some(s.to_owned())
                    }
                    s.id != shard
                });
                break;
            }
        }

        if let Some(shard_desc) = shard_desc {
            for group in groups.values_mut() {
                if group.id == tgroup {
                    group.shards.push(shard_desc);
                    break;
                }
            }
        }

        self.set_groups(groups.values().map(ToOwned::to_owned).collect());
    }

    pub fn assign_shard(&self, group_id: u64) {
        let mut groups = self.groups();
        for group in groups.values_mut() {
            if group.id == group_id {
                let s = ShardDesc {
                    id: self.shard_id_gen.fetch_add(1, Ordering::Relaxed),
                    ..Default::default()
                };
                group.shards.push(s);
            }
        }
        self.set_groups(groups.values().map(ToOwned::to_owned).collect());
    }

    pub fn display(&self) {
        let groups = self.groups.lock().unwrap();
        println!("----------");
        for (n, g) in &groups.node_replicas {
            println!("node replicas: {} -> {:?}", n, g.iter().map(|r| r.0.id).collect::<Vec<u64>>())
        }
        let descs = &groups.descs;
        for g in descs.values() {
            let shards = g.shards.iter().map(|s| s.id).collect::<Vec<u64>>();
            println!("group shards: {} -> {:?}", g.id, shards);
        }

        let nodes = self.nodes.lock().unwrap();
        println!("cluster_nodes: {:?}", nodes.iter().map(|n| n.id).collect::<Vec<u64>>());

        let state = self.replicas.lock().unwrap();
        let mut node_leaders: HashMap<u64, Vec<u64>> = HashMap::new();
        let rs = state.values();
        for r in rs.filter(|s| s.role == RaftRole::Leader as i32) {
            let n = groups
                .descs
                .get(&r.group_id)
                .unwrap()
                .replicas
                .iter()
                .find(|d| d.id == r.replica_id)
                .unwrap()
                .node_id;
            match node_leaders.entry(n) {
                Entry::Occupied(mut ent) => {
                    ent.get_mut().push(r.group_id);
                }
                Entry::Vacant(ent) => {
                    ent.insert(vec![r.group_id]);
                }
            }
        }
        for (n, g) in node_leaders {
            println!("node group leader: {} -> {:?}", n, g,)
        }

        println!("----------");
    }
}
