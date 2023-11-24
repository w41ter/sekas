// Copyright 2023 The Sekas Authors.
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

use log::{debug, error, info, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_client::RetryState;
use sekas_rock::fn_name;

use crate::helper::client::*;
use crate::helper::context::*;
use crate::helper::init::setup_panic_hook;
use crate::helper::runtime::*;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

async fn is_not_in_migration(c: &ClusterClient, dest_group_id: u64) -> bool {
    use collect_migration_state_response::State;
    if let Some(leader_node_id) = c.get_group_leader_node_id(dest_group_id).await {
        debug!("group {dest_group_id} node {leader_node_id} collect migration state",);
        if let Ok(resp) = c.collect_migration_state(dest_group_id, leader_node_id).await {
            debug!(
                "group {dest_group_id} node {leader_node_id} collect migration state: {:?}",
                resp.state
            );
            if resp.state == State::None as i32 {
                // migration is finished or aborted.
                return true;
            }
        }
    }
    false
}

async fn move_shard(
    c: &ClusterClient,
    shard_desc: &ShardDesc,
    dest_group_id: u64,
    src_group_id: u64,
) {
    'OUTER: for _ in 0..16 {
        let src_group_epoch = c.must_group_epoch(src_group_id).await;

        // Shard migration is finished.
        if c.group_contains_shard(dest_group_id, shard_desc.id) {
            return;
        }

        let mut g = c.group(dest_group_id);
        if let Err(e) = g.accept_shard(src_group_id, src_group_epoch, shard_desc).await {
            warn!(
                "accept shard {} from {src_group_id} to {dest_group_id} with src epoch {src_group_epoch}: {e:?}",
                shard_desc.id
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
        for _ in 0..1000 {
            if is_not_in_migration(c, dest_group_id).await {
                continue 'OUTER;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        panic!("migration task is timeout");
    }

    panic!("move shard is failed after 16 retries");
}

async fn validate(c: &ClusterClient, group_id: u64, shard_id: u64, range: std::ops::Range<u64>) {
    let mut c = c.group(group_id);
    for i in range {
        let key = format!("key-{i}");
        let expected_value = format!("value-{i}").as_bytes().to_vec();
        let req = Request::Get(ShardGetRequest {
            shard_id,
            start_version: u64::MAX,
            key: key.as_bytes().to_vec(),
        });

        let mut retry_state = RetryState::default();
        loop {
            match c.request(&req).await {
                Ok(resp) => {
                    let Response::Get(resp) = resp else { panic!("Invalid response type") };
                    assert!(matches!(resp.value, Some(Value { content: Some(content), version: _})
                            if content == expected_value));
                    break;
                }
                Err(err) => {
                    retry_state.retry(err).await.unwrap();
                }
            }
        }
    }
}

async fn insert(c: &ClusterClient, group_id: u64, shard_id: u64, range: std::ops::Range<u64>) {
    let mut c = c.group(group_id);
    for i in range {
        let key = format!("key-{}", i);
        let value = format!("value-{}", i);
        let put = PutRequest {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            ..Default::default()
        };
        let req =
            Request::Write(ShardWriteRequest { shard_id, puts: vec![put], ..Default::default() });

        let mut retry_state = RetryState::default();
        loop {
            match c.request(&req).await {
                Ok(_) => break,
                Err(err) => {
                    retry_state.retry(err).await.unwrap();
                }
            }
        }
    }
}

/// Migration test within groups which have only one member, shard is empty.
#[sekas_macro::test]
async fn migration_single_replica_empty_shard() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(2).await;
    let c = ClusterClient::new(nodes).await;
    let node_1_id = 0;
    let node_2_id = 1;
    let group_id_1 = 100000;
    let group_id_2 = 100001;
    let replica_1 = 1000000;
    let replica_2 = 2000000;
    let shard_id = 10000000;

    info!(
        "create group {} at node {} with replica {} and shard {}",
        group_id_1, node_1_id, replica_1, shard_id,
    );

    let shard_desc = ShardDesc::whole(shard_id, shard_id);
    let replica_desc_1 =
        ReplicaDesc { id: replica_1, node_id: node_1_id, role: ReplicaRole::Voter as i32 };
    let group_desc_1 = GroupDesc {
        id: group_id_1,
        shards: vec![shard_desc.clone()],
        replicas: vec![replica_desc_1.clone()],
        ..Default::default()
    };
    c.create_replica(node_1_id, replica_1, group_desc_1.clone()).await;

    info!("create group {} at node {} with replica {}", group_id_2, node_2_id, replica_2);
    let replica_desc_2 =
        ReplicaDesc { id: replica_2, node_id: node_2_id, role: ReplicaRole::Voter as i32 };
    let group_desc_2 = GroupDesc {
        id: group_id_2,
        shards: vec![],
        replicas: vec![replica_desc_2.clone()],
        ..Default::default()
    };
    c.create_replica(node_2_id, replica_2, group_desc_2.clone()).await;
    c.assert_group_leader(group_id_1).await;
    c.assert_group_leader(group_id_2).await;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    move_shard(&c, &shard_desc, group_id_2, group_id_1).await;
}

/// Migration test within groups which have only one member, shard have 1000 key
/// values.
#[sekas_macro::test]
async fn migration_single_replica() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(2).await;
    let c = ClusterClient::new(nodes).await;
    let node_1_id = 0;
    let node_2_id = 1;
    let group_id_1 = 100000;
    let group_id_2 = 100001;
    let replica_1 = 1000000;
    let replica_2 = 2000000;
    let shard_id = 10000000;

    info!(
        "create group {} at node {} with replica {} and shard {}",
        group_id_1, node_1_id, replica_1, shard_id,
    );

    let shard_desc = ShardDesc::whole(shard_id, shard_id);
    let replica_desc_1 =
        ReplicaDesc { id: replica_1, node_id: node_1_id, role: ReplicaRole::Voter as i32 };
    let group_desc_1 = GroupDesc {
        id: group_id_1,
        shards: vec![shard_desc.clone()],
        replicas: vec![replica_desc_1.clone()],
        ..Default::default()
    };
    c.create_replica(node_1_id, replica_1, group_desc_1.clone()).await;

    info!("insert data into group {} shard {}", group_id_1, shard_id);
    insert(&c, group_id_1, shard_id, 0..1000).await;

    info!("create group {} at node {} with replica {}", group_id_2, node_2_id, replica_2);
    let replica_desc_2 =
        ReplicaDesc { id: replica_2, node_id: node_2_id, role: ReplicaRole::Voter as i32 };
    let group_desc_2 = GroupDesc {
        id: group_id_2,
        shards: vec![],
        replicas: vec![replica_desc_2.clone()],
        ..Default::default()
    };
    c.create_replica(node_2_id, replica_2, group_desc_2.clone()).await;
    c.assert_group_leader(group_id_1).await;
    c.assert_group_leader(group_id_2).await;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    move_shard(&c, &shard_desc, group_id_2, group_id_1).await;
    validate(&c, group_id_2, shard_id, 0..1000).await;
}

async fn create_group(c: &ClusterClient, group_id: u64, nodes: Vec<u64>, shards: Vec<ShardDesc>) {
    let replicas = nodes
        .iter()
        .cloned()
        .map(|node_id| {
            let replica_id = group_id * 10 + node_id;
            ReplicaDesc { id: replica_id, node_id, role: ReplicaRole::Voter as i32 }
        })
        .collect::<Vec<_>>();
    let group_desc =
        GroupDesc { id: group_id, shards, replicas: replicas.clone(), ..Default::default() };
    for replica in replicas {
        c.create_replica(replica.node_id, replica.id, group_desc.clone()).await;
    }
}

async fn create_two_groups(
    c: &ClusterClient,
    nodes: Vec<u64>,
    num_keys: u64,
) -> (u64, u64, ShardDesc) {
    let group_id_1 = 100000;
    let group_id_2 = 100001;
    let shard_id = 10000000;

    info!("create group {} with shard {}", group_id_1, shard_id,);

    let shard_desc = ShardDesc::whole(shard_id, shard_id);
    create_group(c, group_id_1, nodes.clone(), vec![shard_desc.clone()]).await;

    info!("insert data into group {} shard {}", group_id_1, shard_id);
    insert(c, group_id_1, shard_id, 0..num_keys).await;

    info!("create group {} ", group_id_2);
    create_group(c, group_id_2, nodes, vec![]).await;

    c.assert_group_leader(group_id_1).await;
    c.assert_group_leader(group_id_2).await;
    (group_id_1, group_id_2, shard_desc)
}

/// The basic migration test.
#[sekas_macro::test]
async fn migration_basic() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(3).await;
    let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
    let c = ClusterClient::new(nodes).await;
    let (group_id_1, group_id_2, shard_desc) = create_two_groups(&c, node_ids, 1000).await;
    let shard_id = shard_desc.id;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    move_shard(&c, &shard_desc, group_id_2, group_id_1).await;
}

#[sekas_macro::test]
async fn migration_abort() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(3).await;
    let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
    let c = ClusterClient::new(nodes).await;
    let (group_id_1, group_id_2, shard_desc) = create_two_groups(&c, node_ids, 0).await;
    let shard_id = shard_desc.id;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    let src_epoch = c.must_group_epoch(group_id_1).await;
    c.group(group_id_1).add_learner(123123, 1231231231).await.unwrap();

    let mut group_client = c.group(group_id_2);
    // It will be reject by service busy?
    // Ensure issue at least one shard migration.
    while let Err(e) = group_client.accept_shard(group_id_1, src_epoch, &shard_desc).await {
        error!("accept shard: {e:?}");
        ctx.wait_election_timeout().await;
    }
    // Ensure the former shard migration is aborted by epoch not match.
    while group_client.accept_shard(group_id_1, src_epoch, &shard_desc).await.is_err() {
        ctx.wait_election_timeout().await;
    }
}

#[sekas_macro::test]
async fn migration_with_offline_peers() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(3).await;
    let mut node_ids = nodes.keys().cloned().collect::<Vec<_>>();
    node_ids.sort_unstable();
    let c = ClusterClient::new(nodes).await;
    let (group_id_1, group_id_2, shard_desc) = create_two_groups(&c, node_ids.clone(), 0).await;
    let shard_id = shard_desc.id;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    c.assert_root_group_has_promoted().await;
    ctx.stop_server(*node_ids.last().unwrap()).await;
    ctx.wait_election_timeout().await;

    move_shard(&c, &shard_desc, group_id_2, group_id_1).await;
}

#[sekas_macro::test]
async fn migration_source_group_receive_duplicate_accepting_shard_request() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(3).await;
    let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
    let c = ClusterClient::new(nodes).await;
    let (group_id_1, group_id_2, shard_desc) = create_two_groups(&c, node_ids.clone(), 0).await;

    for _ in 0..10 {
        let mut g = c.group(group_id_1);
        let src_group_epoch = c.must_group_epoch(group_id_1).await;
        let desc = MigrationDesc {
            shard_desc: Some(shard_desc.clone()),
            src_group_id: group_id_1,
            src_group_epoch,
            dest_group_id: group_id_2,
            dest_group_epoch: 1,
        };
        match g.setup_migration(&desc).await {
            Err(sekas_client::Error::EpochNotMatch(_)) => {
                continue;
            }
            Ok(_) => {}
            Err(e) => panic!("setup migration receive: {e:?}"),
        }
        // retry
        g.setup_migration(&desc).await.unwrap();

        g.commit_migration(&desc).await.unwrap();
        // retry
        g.commit_migration(&desc).await.unwrap();
        break;
    }
}

#[sekas_macro::test]
async fn migration_source_group_receive_many_accepting_shard_request() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    let nodes = ctx.bootstrap_servers(3).await;
    let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
    let c = ClusterClient::new(nodes).await;
    let (group_id_1, group_id_2, shard_desc) = create_two_groups(&c, node_ids.clone(), 0).await;

    for _ in 0..10 {
        let mut g = c.group(group_id_1);
        let src_group_epoch = c.must_group_epoch(group_id_1).await;
        let desc = MigrationDesc {
            shard_desc: Some(shard_desc.clone()),
            src_group_id: group_id_1,
            src_group_epoch,
            dest_group_id: group_id_2,
            dest_group_epoch: 1,
        };
        match g.setup_migration(&desc).await {
            Err(sekas_client::Error::EpochNotMatch(_)) => {
                continue;
            }
            Ok(_) => {}
            Err(e) => panic!("setup migration receive: {e:?}"),
        }

        let mut cloned_g = g.clone();
        let diff_desc = MigrationDesc { dest_group_id: 1231, ..desc.clone() };
        let handle = spawn(async move {
            // retry
            assert!(matches!(
                cloned_g.setup_migration(&diff_desc).await,
                Err(sekas_client::Error::EpochNotMatch(_))
            ));
        });

        ctx.wait_election_timeout().await;
        g.commit_migration(&desc).await.unwrap();
        handle.await.unwrap();
        break;
    }
}

#[sekas_macro::test]
async fn migration_receive_forward_request_after_shard_migrated() {
    let mut ctx = TestContext::new(fn_name!());
    ctx.disable_all_balance();
    ctx.disable_all_node_scheduler();
    let nodes = ctx.bootstrap_servers(2).await;
    let c = ClusterClient::new(nodes).await;
    let node_1_id = 0;
    let node_2_id = 1;
    let group_id_1 = 100000;
    let group_id_2 = 100001;
    let replica_1 = 1000000;
    let replica_2 = 2000000;
    let shard_id = 10000000;

    info!(
        "create group {} at node {} with replica {} and shard {}",
        group_id_1, node_1_id, replica_1, shard_id,
    );

    let shard_desc = ShardDesc::whole(shard_id, shard_id);
    let replica_desc_1 =
        ReplicaDesc { id: replica_1, node_id: node_1_id, role: ReplicaRole::Voter as i32 };
    let group_desc_1 = GroupDesc {
        id: group_id_1,
        shards: vec![shard_desc.clone()],
        replicas: vec![replica_desc_1.clone()],
        ..Default::default()
    };
    c.create_replica(node_1_id, replica_1, group_desc_1.clone()).await;

    info!("create group {} at node {} with replica {}", group_id_2, node_2_id, replica_2);
    let replica_desc_2 =
        ReplicaDesc { id: replica_2, node_id: node_2_id, role: ReplicaRole::Voter as i32 };
    let group_desc_2 = GroupDesc {
        id: group_id_2,
        shards: vec![],
        replicas: vec![replica_desc_2.clone()],
        ..Default::default()
    };
    c.create_replica(node_2_id, replica_2, group_desc_2.clone()).await;

    info!("issue accept shard {} request to group {}", shard_id, group_id_2);

    c.assert_group_leader(group_id_1).await;
    c.assert_group_leader(group_id_2).await;
    move_shard(&c, &shard_desc, group_id_2, group_id_1).await;

    let mut group_client = c.group(group_id_2);
    let req = ForwardRequest {
        group_id: group_id_2,
        shard_id,
        forward_data: vec![ValueSet {
            user_key: b"a".to_vec(),
            values: vec![Value { content: Some(b"b".to_vec()), version: 1 }],
        }],
        request: Some(GroupRequestUnion {
            request: Some(Request::Write(ShardWriteRequest {
                shard_id,
                puts: vec![PutRequest {
                    key: b"b".to_vec(),
                    value: b"value".to_vec(),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        }),
    };
    group_client.forward(&req).await.unwrap();
    let resp = group_client
        .request(&Request::Get(ShardGetRequest {
            shard_id,
            start_version: u64::MAX,
            key: b"a".to_vec(),
        }))
        .await
        .unwrap();
    let value = match resp {
        Response::Get(ShardGetResponse { value }) => value,
        _ => panic!("invalid response type, Get is required"),
    };
    // Ingest should failed because migration is finished.
    assert!(value.is_none());

    let resp = group_client
        .request(&Request::Get(ShardGetRequest {
            shard_id,
            start_version: u64::MAX,
            key: b"b".to_vec(),
        }))
        .await
        .unwrap();
    let value = match resp {
        Response::Get(ShardGetResponse { value }) => value,
        _ => panic!("invalid response type, Get is required"),
    };
    assert!(
        matches!(value, Some(Value { content: Some(v), version: _ }) if v == b"value".to_vec())
    );
}
