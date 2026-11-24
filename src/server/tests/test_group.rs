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
mod helper;

use std::time::Duration;

use helper::context::TestContext;
use log::info;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_rock::fn_name;

use crate::helper::client::*;
use crate::helper::init::setup_panic_hook;

#[ctor::ctor(unsafe)]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

async fn create_group(c: &ClusterClient, group_id: u64, nodes: Vec<u64>) {
    let replicas = nodes
        .iter()
        .cloned()
        .map(|node_id| {
            let replica_id = group_id * 10 + node_id;
            ReplicaDesc { id: replica_id, node_id, role: ReplicaRole::Voter as i32 }
        })
        .collect::<Vec<_>>();
    let group_desc = GroupDesc {
        id: group_id,
        shards: vec![],
        replicas: replicas.clone(),
        ..Default::default()
    };
    for replica in replicas {
        c.create_replica(replica.node_id, replica.id, group_desc.clone()).await;
    }
}

#[sekas_macro::test]
async fn group_add_replica() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(2).await;
    let c = ClusterClient::new(nodes).await;

    let group_id = 0;
    let new_replica_id = 123;

    let root_group = GroupDesc { id: group_id, ..Default::default() };

    info!("create new replica {new_replica_id} of group {group_id}");

    // 1. create replica firstly
    c.create_replica(1, new_replica_id, root_group).await;
    c.assert_group_leader(group_id).await;

    info!("try add replica {new_replica_id} into group {group_id}");

    // 2. add replica to group
    let mut group_client = c.group(group_id);
    group_client.add_learner(new_replica_id, 1).await.unwrap();

    ctx.wait_election_timeout().await;

    group_client.add_replica(new_replica_id, 1).await.unwrap();

    c.assert_group_contains_member(group_id, new_replica_id).await;
}

#[sekas_macro::test]
async fn group_create_with_multi_replicas() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(4).await;
    let c = ClusterClient::new(nodes).await;

    let group_id = 100000000;
    let group_desc = GroupDesc {
        id: group_id,
        replicas: vec![
            ReplicaDesc { id: 100, node_id: 0, role: ReplicaRole::Voter as i32 },
            ReplicaDesc { id: 101, node_id: 1, role: ReplicaRole::Voter as i32 },
            ReplicaDesc { id: 102, node_id: 2, role: ReplicaRole::Voter as i32 },
        ],
        ..Default::default()
    };

    // 1. create group
    c.create_replica(0, 100, group_desc.clone()).await;
    c.create_replica(1, 101, group_desc.clone()).await;
    c.create_replica(2, 102, group_desc.clone()).await;
    c.assert_group_leader(group_id).await;

    info!("create new replica 103");

    // 2. create single replica
    let empty_desc = GroupDesc { id: group_id, ..Default::default() };
    c.create_replica(3, 103, empty_desc).await;

    info!("add replica 103 to group 1");

    let mut group_client = c.group(group_id);
    group_client.add_replica(103, 3).await.unwrap();
    ctx.wait_election_timeout().await;

    c.assert_group_contains_member(group_id, 103).await;
}

/// The root group can be promoted to cluster mode as long as enough nodes are
/// added to the cluster.
#[sekas_macro::test]
async fn group_promote_to_cluster_from_single_node() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;

    let root_group_id = 0;
    for _ in 0..10000 {
        let members = c.group_members(root_group_id).await;
        if members.into_iter().filter(|(_, v)| *v == ReplicaRole::Voter as i32).count() == 3 {
            return;
        }

        ctx.wait_election_timeout().await;
    }
    panic!("could not promote root group to cluster");
}

#[sekas_macro::test]
async fn group_cure_automatic() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(4).await;
    let c = ClusterClient::new(nodes).await;

    let group_id = 100000000;
    let group_desc = GroupDesc {
        id: group_id,
        replicas: vec![
            ReplicaDesc { id: 100, node_id: 0, role: ReplicaRole::Voter as i32 },
            ReplicaDesc { id: 101, node_id: 1, role: ReplicaRole::Voter as i32 },
            ReplicaDesc { id: 103, node_id: 3, role: ReplicaRole::Voter as i32 },
        ],
        ..Default::default()
    };

    // 1. create group
    c.create_replica(0, 100, group_desc.clone()).await;
    c.create_replica(1, 101, group_desc.clone()).await;
    c.create_replica(3, 103, group_desc.clone()).await;
    c.assert_group_leader(group_id).await;

    info!("shutdown node 3 and replica 103");
    c.assert_group_contains_member(group_id, 103).await;
    c.assert_root_group_has_promoted().await;

    ctx.stop_server(3).await;
    ctx.wait_election_timeout().await;

    info!("wait curing group {group_id}");
    c.assert_group_not_contains_member(group_id, 103).await;
}

#[sekas_macro::test]
async fn group_move_replica() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(4).await;
    let c = ClusterClient::new(nodes.clone()).await;
    let group_id = 10;
    let mut node_id_list = nodes.keys().cloned().collect::<Vec<_>>();
    let node_id = node_id_list.pop().unwrap();
    create_group(&c, group_id, node_id_list).await;

    info!("issue moving replicas request");
    c.assert_group_leader(group_id).await;
    let follower = c.must_group_any_follower(group_id).await;
    let follower_id = follower.id;
    let mut group = c.group(group_id);
    group
        .move_replicas(
            vec![ReplicaDesc { id: 123123, node_id, role: ReplicaRole::Voter as i32 }],
            vec![follower],
        )
        .await
        .unwrap();

    c.assert_group_not_contains_member(group_id, follower_id).await;
    c.assert_group_contains_member(group_id, 123123).await;
}

#[sekas_macro::test]
async fn group_scheduled_replacement_reserves_membership_and_recovers() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_node_scheduler();
    ctx.set_heartbeat_interval_sec(1);
    let cluster = ClusterClient::new(ctx.bootstrap_servers(4).await).await;
    cluster.assert_root_group_has_promoted().await;
    let group_id = 10;
    create_group(&cluster, group_id, vec![0, 1, 2]).await;
    cluster.assert_group_leader(group_id).await;

    let mut group = cluster.group(group_id);
    let state = group.inspect_group().await.unwrap();
    let desc = state.group.unwrap();
    let outgoing = *desc.replicas.iter().find(|replica| replica.id != state.leader_id).unwrap();
    let intent = ReplicaReplacement {
        incoming_id: 123123,
        outgoing_id: outgoing.id,
        target_node: 3,
        add_epoch: desc.epoch,
    };
    advance_replacement(&mut group, intent, true).await.unwrap();
    let state = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let state = group.inspect_group().await.unwrap();
            if state.group.as_ref().unwrap().replicas.iter().any(|replica| {
                replica.id == intent.incoming_id && replica.role() == ReplicaRole::Learner
            }) {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("learner admission observed in Group membership");
    let added = state.group.unwrap();

    // The scheduler may accept a submission while the active replacement owns
    // the membership lock. The conflicting move must leave the outgoing voter in
    // place.
    let submitted = group
        .execute_scheduled(
            GroupRequest {
                group_id,
                epoch: added.epoch,
                request: Some(GroupRequestUnion {
                    request: Some(Request::MoveReplicas(MoveReplicasRequest {
                        outgoing_voters: vec![outgoing],
                        ..Default::default()
                    })),
                }),
                expected_term: None,
            },
            state.term,
        )
        .await
        .unwrap();
    assert!(matches!(submitted, Response::MoveReplicas(_)));

    let old_leader = added.replicas.iter().find(|replica| replica.id == state.leader_id).unwrap();
    ctx.stop_server(old_leader.node_id).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            group = cluster.group(group_id);
            group.set_timeout(Duration::from_secs(1));
            if let Ok(current) = group.inspect_group().await
                && current.term > state.term
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Group election after learner admission");

    // A fresh Group scheduler must recover the same learner from Raft and
    // remove it when Root says the target is no longer eligible.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let desc = group.inspect_group().await.unwrap().group.unwrap();
            if !desc.replicas.iter().any(|replica| replica.id == intent.incoming_id) {
                assert!(desc.epoch > intent.add_epoch);
                break;
            }
            match advance_replacement(&mut group, intent, false).await {
                Ok(())
                | Err(sekas_client::Error::EpochNotMatch(_))
                | Err(sekas_client::Error::AlreadyExists(_)) => {}
                Err(err) => panic!("submit replacement cleanup: {err}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replacement cleanup after the election");

    let desc = group.inspect_group().await.unwrap().group.unwrap();
    assert!(!desc.replicas.iter().any(|replica| replica.id == intent.incoming_id));
    assert!(desc.replicas.contains(&outgoing));
}

async fn advance_replacement(
    group: &mut sekas_client::GroupClient,
    intent: ReplicaReplacement,
    target_active: bool,
) -> Result<(), sekas_client::Error> {
    let state = group.inspect_group().await?;
    let desc = state.group.unwrap();
    let request = GroupRequest {
        group_id: desc.id,
        epoch: desc.epoch,
        request: Some(GroupRequestUnion {
            request: Some(Request::MoveReplicas(MoveReplicasRequest {
                replacement: Some(intent),
                replacement_target_active: target_active,
                ..Default::default()
            })),
        }),
        expected_term: None,
    };
    let Response::MoveReplicas(_) = group.execute_scheduled(request, state.term).await? else {
        panic!("expected replica replacement response");
    };
    Ok(())
}

#[sekas_macro::test]
async fn group_move_replica_add_only() {
    let mut ctx = TestContext::new(fn_name!());
    let nodes = ctx.bootstrap_servers(4).await;
    let c = ClusterClient::new(nodes.clone()).await;
    let group_id = 10;
    let mut node_id_list = nodes.keys().cloned().collect::<Vec<_>>();
    let node_id = node_id_list.pop().unwrap();
    create_group(&c, group_id, node_id_list).await;

    info!("issue add-only moving replicas request");
    c.assert_group_leader(group_id).await;
    let mut group = c.group(group_id);
    group
        .move_replicas(
            vec![ReplicaDesc { id: 123123, node_id, role: ReplicaRole::Voter as i32 }],
            vec![],
        )
        .await
        .unwrap();

    c.assert_group_contains_member(group_id, 123123).await;
}

#[sekas_macro::test]
async fn group_move_replica_remove_only() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(4).await;
    let c = ClusterClient::new(nodes.clone()).await;
    let group_id = 10;
    create_group(&c, group_id, nodes.keys().cloned().collect()).await;

    info!("issue remove-only moving replicas request");
    c.assert_group_leader(group_id).await;
    let follower = c.must_group_any_follower(group_id).await;
    let follower_id = follower.id;
    let mut group = c.group(group_id);
    group.move_replicas(vec![], vec![follower]).await.unwrap();

    c.assert_group_not_contains_member(group_id, follower_id).await;
}
