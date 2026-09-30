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

#![allow(clippy::result_large_err)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use log::{error, info};
use sekas_api::server::v1::*;
use sekas_client::{
    ClientOptions, ConnManager, GroupClient, NodeClient, RootClient, Router, RouterGroupState,
    SekasClient, StaticServiceDiscovery,
};
use sekas_server::Result;

pub async fn node_client_with_retry(addr: &str) -> NodeClient {
    for _ in 0..10000 {
        match NodeClient::connect(addr.to_string()).await {
            Ok(client) => return client,
            Err(_) => {
                sekas_runtime::time::sleep(Duration::from_millis(50)).await;
            }
        };
    }
    panic!("connect to {} timeout", addr);
}

#[allow(unused)]
pub struct ClusterClient {
    nodes: HashMap<u64, String>,
    router: Router,
    conn_manager: ConnManager,
    client: SekasClient,
}

#[allow(unused)]
impl ClusterClient {
    pub async fn new(nodes: HashMap<u64, String>) -> Self {
        let conn_manager = ConnManager::new();
        let discovery = Arc::new(StaticServiceDiscovery::new(nodes.values().cloned().collect()));
        let root_client = RootClient::new(discovery, conn_manager.clone());
        let router = Router::new(root_client.clone()).await;
        let client = SekasClient::build(
            ClientOptions::default(),
            router.clone(),
            root_client,
            conn_manager.clone(),
        );
        ClusterClient { nodes, router, conn_manager, client }
    }

    pub async fn create_replica(&self, node_id: u64, replica_id: u64, desc: GroupDesc) {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = node_client_with_retry(node_addr).await;
        client.create_replica(replica_id, desc).await.unwrap();
    }

    pub fn group(&self, group_id: u64) -> GroupClient {
        GroupClient::lazy(group_id, self.client.clone())
    }

    pub async fn app_client(&self) -> SekasClient {
        self.client.clone()
    }

    pub async fn app_client_with_options(&self, opts: ClientOptions) -> SekasClient {
        let addrs = self.nodes.values().cloned().collect::<Vec<_>>();
        SekasClient::new(opts, addrs).await.unwrap()
    }

    pub async fn group_members(&self, group_id: u64) -> Vec<(u64, i32)> {
        if let Ok(state) = self.router.find_group(group_id) {
            let mut current = state.replicas.iter().map(|(k, v)| (*k, v.role)).collect::<Vec<_>>();
            current.sort_unstable();
            current
        } else {
            vec![]
        }
    }

    pub async fn assert_group_members(&self, group_id: u64, mut replicas: Vec<u64>) {
        replicas.sort_unstable();
        for _ in 0..10000 {
            let members = self.group_members(group_id).await;
            let mut members = members
                .into_iter()
                .filter(|(_, v)| *v == ReplicaRole::Voter as i32)
                .map(|(k, _)| k)
                .collect::<Vec<u64>>();
            members.sort_unstable();
            if members == replicas {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have expected replicas {replicas:?}");
    }

    /// Loop until the exists expected size of voters in the target group.
    pub async fn assert_num_group_voters(&self, group_id: u64, size: usize) {
        for _ in 0..10000 {
            let members = self.group_members(group_id).await;
            if members.into_iter().filter(|(_, v)| *v == ReplicaRole::Voter as i32).count() == size
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have expected number of voters ({size})");
    }

    pub async fn assert_group_contains_member(&self, group_id: u64, replica_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id)
                && state.replicas.contains_key(&replica_id)
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is not contains replica {replica_id}");
    }

    pub async fn assert_group_not_contains_member(&self, group_id: u64, replica_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id)
                && !state.replicas.contains_key(&replica_id)
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is contains replica {replica_id}");
    }

    pub async fn assert_group_not_contains_node(&self, group_id: u64, node_id: u64) {
        for _ in 0..10000 {
            if let Ok(state) = self.router.find_group(group_id)
                && !state.replicas.iter().any(|(_, r)| r.node_id == node_id)
            {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is contains node {node_id}");
    }

    pub async fn get_group_leader(&self, group_id: u64) -> Option<u64> {
        self.router.find_group(group_id).ok().and_then(|s| s.leader_state).map(|s| s.0)
    }

    pub async fn get_group_leader_node_id(&self, group_id: u64) -> Option<u64> {
        if let Ok(state) = self.router.find_group(group_id) {
            for (_, replica) in state.replicas {
                if matches!(state.leader_state, Some(v) if v.0 == replica.id) {
                    return Some(replica.node_id);
                }
            }
        }
        None
    }

    pub async fn get_group_any_follower(&self, group_id: u64) -> Option<ReplicaDesc> {
        use rand::{Rng, thread_rng};

        if let Some(leader_id) = self.get_group_leader(group_id).await
            && let Ok(state) = self.router.find_group(group_id)
        {
            let replicas = state
                .replicas
                .into_iter()
                .filter(|(_, replica)| replica.id != leader_id)
                .map(|(_, replica)| replica)
                .collect::<Vec<_>>();
            if replicas.is_empty() {
                return None;
            }
            let index = thread_rng().gen_range(0..replicas.len());
            return Some(replicas[index].clone());
        }
        None
    }

    pub async fn must_group_any_follower(&self, group_id: u64) -> ReplicaDesc {
        for _ in 0..1000 {
            if let Some(replica) = self.get_group_any_follower(group_id).await {
                return replica;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have a follower");
    }

    pub async fn assert_group_leader(&self, group_id: u64) -> u64 {
        for _ in 0..10000 {
            if let Some(leader) = self.get_group_leader(group_id).await {
                return leader;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} does not have a leader");
    }

    pub async fn group_remove_node(&self, group_id: u64, node_id: u64) -> Result<()> {
        if let Ok(state) = self.router.find_group(group_id) {
            for (_, replica) in state.replicas {
                if replica.node_id == node_id {
                    let mut c = self.group(group_id);
                    c.remove_group_replica(replica.id).await?;
                }
            }
        }
        Ok(())
    }

    pub fn get_group_epoch(&self, group_id: u64) -> Option<u64> {
        self.router.find_group(group_id).ok().map(|s| s.epoch)
    }

    pub async fn must_group_epoch(&self, group_id: u64) -> u64 {
        for _ in 0..1000 {
            if let Some(epoch) = self.get_group_epoch(group_id) {
                return epoch;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no such group {group_id} exists");
    }

    pub async fn assert_large_group_epoch(&self, group_id: u64, former_epoch: u64) -> u64 {
        for _ in 0..1000 {
            if let Some(epoch) = self.get_group_epoch(group_id)
                && epoch > former_epoch
            {
                return epoch;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group epoch still less than or equals to {former_epoch}");
    }

    pub fn group_contains_shard(&self, group_id: u64, shard_id: u64) -> bool {
        if let Ok(state) = self.router.find_group_by_shard(shard_id)
            && state.id == group_id
        {
            return true;
        }
        false
    }

    pub async fn assert_group_contains_shard(&self, group_id: u64, shard_id: u64) {
        for _ in 0..10000 {
            if self.group_contains_shard(group_id, shard_id) {
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("group {group_id} is not contains shard {shard_id}");
    }

    pub async fn collect_moving_shard_state(
        &self,
        group_id: u64,
        node_id: u64,
    ) -> Result<CollectMovingShardStateResponse> {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = NodeClient::connect(node_addr.to_string()).await?;
        let resp = client
            .root_heartbeat(HeartbeatRequest {
                timestamp: 0,
                piggybacks: vec![PiggybackRequest {
                    info: Some(piggyback_request::Info::CollectMovingShardState(
                        CollectMovingShardStateRequest { group: group_id },
                    )),
                }],
            })
            .await?;
        for resp in &resp.piggybacks {
            match resp.info.as_ref().unwrap() {
                piggyback_response::Info::SyncRoot(_)
                | piggyback_response::Info::SyncGroupGcVersion(_)
                | piggyback_response::Info::CollectStats(_)
                | piggyback_response::Info::CollectScheduleState(_)
                | piggyback_response::Info::CollectGroupDetail(_) => {}
                piggyback_response::Info::CollectMovingShardState(resp) => {
                    return Ok(resp.clone());
                }
            }
        }
        panic!("collect_move_shard_state have't received response");
    }

    pub async fn collect_replica_state(
        &self,
        group_id: u64,
        node_id: u64,
    ) -> Result<Option<ReplicaState>> {
        let node_addr = self.nodes.get(&node_id).unwrap();
        let client = NodeClient::connect(node_addr.to_string()).await?;
        let resp = client
            .root_heartbeat(HeartbeatRequest {
                timestamp: 0,
                piggybacks: vec![PiggybackRequest {
                    info: Some(piggyback_request::Info::CollectGroupDetail(
                        CollectGroupDetailRequest { groups: vec![group_id] },
                    )),
                }],
            })
            .await
            .unwrap();
        for resp in &resp.piggybacks {
            match resp.info.as_ref().unwrap() {
                piggyback_response::Info::SyncRoot(_)
                | piggyback_response::Info::SyncGroupGcVersion(_)
                | piggyback_response::Info::CollectStats(_)
                | piggyback_response::Info::CollectScheduleState(_)
                | piggyback_response::Info::CollectMovingShardState(_) => {}
                piggyback_response::Info::CollectGroupDetail(resp) => {
                    for state in &resp.replica_states {
                        if state.group_id == group_id {
                            return Ok(Some(state.clone()));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn get_shard_desc(&self, table_id: u64, key: &[u8]) -> Option<ShardDesc> {
        self.router.find_shard(table_id, key).ok().map(|(_, shard)| shard)
    }

    pub async fn get_router_group_state(&self, group_id: u64) -> Option<RouterGroupState> {
        self.router.find_group(group_id).ok()
    }

    pub async fn find_router_group_state_by_key(
        &self,
        table_id: u64,
        key: &[u8],
    ) -> Option<RouterGroupState> {
        let (_, shard) = self.router.find_shard(table_id, key).ok()?;
        self.router.find_group_by_shard(shard.id).ok()
    }

    async fn group_for_key(&self, table_id: u64, key: &[u8]) -> (RouterGroupState, ShardDesc) {
        for _ in 0..1000 {
            if let Ok(route) = self.router.find_shard(table_id, key) {
                return route;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no group for table {table_id} key {key:?}");
    }

    async fn move_key_to_group(&self, key: (u64, &[u8]), target: u64) {
        for _ in 0..16 {
            let (source, shard) = self.group_for_key(key.0, key.1).await;
            if source.id == target {
                return;
            }
            if self.group(target).accept_shard(source.id, source.epoch, &shard).await.is_ok() {
                for _ in 0..1000 {
                    if self.group_contains_shard(target, shard.id) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("could not move table {} key {:?} to group {target}", key.0, key.1);
    }

    /// Co-locate the two keys' shards, keeping the first shard in place.
    /// The caller is responsible for disabling shard balancing.
    pub async fn ensure_same_group(&self, first: (u64, &[u8]), second: (u64, &[u8])) -> u64 {
        let (group, _) = self.group_for_key(first.0, first.1).await;
        self.move_key_to_group(second, group.id).await;
        let (current_first, _) = self.group_for_key(first.0, first.1).await;
        let (current_second, _) = self.group_for_key(second.0, second.1).await;
        assert_eq!(current_first.id, group.id);
        assert_eq!(current_second.id, group.id);
        group.id
    }

    /// Separate the two keys' shards, keeping the first shard in place.
    /// Requires an existing spare user group; does not change balancing
    /// settings.
    pub async fn ensure_different_group(
        &self,
        first: (u64, &[u8]),
        second: (u64, &[u8]),
    ) -> (u64, u64) {
        let (first_group, first_shard) = self.group_for_key(first.0, first.1).await;
        let (second_group, second_shard) = self.group_for_key(second.0, second.1).await;
        assert_ne!(first_shard.id, second_shard.id, "cannot separate keys in the same shard");
        if first_group.id != second_group.id {
            return (first_group.id, second_group.id);
        }
        for _ in 0..400 {
            let body = self.client.handle_statement("show groups").await.unwrap();
            let sekas_parser::ExecuteResult::Data(groups) = serde_json::from_slice(&body).unwrap()
            else {
                panic!("SHOW GROUPS did not return groups");
            };
            let mut ids =
                groups.rows.iter().map(|row| row.values[0].as_u64().unwrap()).collect::<Vec<_>>();
            ids.sort_unstable();
            for id in ids {
                if id == sekas_schema::ROOT_GROUP_ID || id == first_group.id {
                    continue;
                }
                if let Ok(target) = self.router.find_group(id)
                    && target.replicas.len() == second_group.replicas.len()
                    && target.leader_state.is_some()
                {
                    self.move_key_to_group(second, id).await;
                    let (current_first, _) = self.group_for_key(first.0, first.1).await;
                    let (current_second, _) = self.group_for_key(second.0, second.1).await;
                    assert_eq!(current_first.id, first_group.id);
                    assert_eq!(current_second.id, id);
                    return (current_first.id, current_second.id);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "no ready spare user group for shard {} in group {}",
            second_shard.id, second_group.id
        );
    }

    pub async fn assert_table_ready(&self, table_id: u64) {
        self.assert_table_ready_with_voters(table_id, 3).await;
    }

    pub async fn assert_table_ready_with_voters(&self, table_id: u64, required_voters: usize) {
        let mut ready_group: HashSet<u64> = HashSet::default();
        for i in 0..255u8 {
            for _ in 0..1000 {
                let state = match self.find_router_group_state_by_key(table_id, &[i]).await {
                    Some(state) => state,
                    None => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };
                if ready_group.insert(state.id) {
                    self.assert_num_group_voters(state.id, required_voters).await;
                    info!("table {table_id} is ready");
                    break;
                }
            }
        }
    }

    pub async fn assert_system_table_ready(&self, required_voters: usize) {
        let table_desc = sekas_schema::system::table::txn_desc();
        let mut ready_group: HashSet<u64> = HashSet::default();
        for i in 0..256u64 {
            for _ in 0..1000 {
                let key = i.to_be_bytes().to_vec();
                let state = match self.find_router_group_state_by_key(table_desc.id, &key).await {
                    Some(state) => state,
                    None => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                };
                if ready_group.insert(state.id) {
                    self.assert_num_group_voters(state.id, required_voters).await;
                    break;
                }
            }
        }
    }

    /// Some tests may shut down a server, if root happens to be on that server,
    /// and there is only one replica in root group, then the test will not
    /// continue because root group is lost.
    pub async fn assert_root_group_has_promoted(&self) {
        self.assert_num_group_voters(0, 3).await;
    }

    /// Transfer the leadership of the group to an random dest replica.
    pub async fn transfer_group_leader_randomly(&self, group_id: u64) -> Result<()> {
        let follower_desc = self.must_group_any_follower(group_id).await;
        let mut group_client = self.group(group_id);
        match group_client.transfer_leader(follower_desc.id).await {
            Ok(()) => {
                info!("transfer leader of group {} to {} success", group_id, follower_desc.id);
                Ok(())
            }
            Err(err) => {
                error!("transfer leader of group {} to {}: {}", group_id, follower_desc.id, err);
                Err(err.into())
            }
        }
    }
}
