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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use log::{info, trace, warn};
use sekas_api::server::v1::watch_response::delete_event::Event as DeleteEvent;
use sekas_api::server::v1::watch_response::update_event::Event as UpdateEvent;
use sekas_api::server::v1::*;
use sekas_api::Epoch;
use tokio::task::JoinHandle;
use tonic::Streaming;

use crate::rpc::RootClient;

#[derive(Debug, Clone)]
pub struct Router {
    core: Arc<RouterCore>,
}

#[derive(Debug)]
pub struct RouterCore {
    handle: JoinHandle<()>,
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Clone, Default)]
pub struct State {
    node_id_lookup: HashMap<u64, String /* ip:port */>,
    db_id_lookup: HashMap<u64, DatabaseDesc>,
    db_name_lookup: HashMap<String, u64>,
    co_id_lookup: HashMap<u64, TableDesc>,
    co_name_lookup: HashMap<(u64 /* db */, String), u64>,
    co_shards_lookup: HashMap<u64 /* co */, Vec<ShardDesc>>,
    shard_group_lookup: HashMap<u64 /* shard */, (u64, u64) /* (group, epoch) */>,
    group_id_lookup: HashMap<u64 /* group */, RouterGroupState>,

    cached_group_states: HashMap<u64, GroupState>,
}

#[derive(Debug, Clone, Default)]
pub struct RouterGroupState {
    pub id: u64,
    pub epoch: u64,
    pub leader_state: Option<(/* id */ u64, /* term */ u64)>,
    pub replicas: HashMap<u64, ReplicaDesc>,
}

impl Router {
    pub async fn new(root_client: RootClient) -> Self {
        let state = Arc::new(Mutex::new(State::default()));
        let state_clone = state.clone();
        let handle = tokio::spawn(async move {
            state_main(state_clone, root_client).await;
        });
        Router { core: Arc::new(RouterCore { handle, state }) }
    }

    // FIXME(walter) txn/get should retry if it meets shard not found.
    pub fn find_shard(
        &self,
        table_id: u64,
        user_key: &[u8],
    ) -> Result<(RouterGroupState, ShardDesc), crate::Error> {
        let state = self.core.state.lock().unwrap();
        let shards = state
            .co_shards_lookup
            .get(&table_id)
            .ok_or_else(|| crate::Error::NotFound(format!("shard (key={:?})", user_key)))?;
        for shard in shards {
            if sekas_schema::shard::belong_to(shard, user_key) {
                // FIXME(walter) there exist some shards, such merged shard, need to recycle.
                if let Some(group_state) = state.find_group_by_shard(shard.id) {
                    return Ok((group_state, shard.clone()));
                }
            }
        }
        Err(crate::Error::NotFound(format!("shard (key={:?})", user_key)))
    }

    pub fn find_group_by_shard(&self, shard: u64) -> Result<RouterGroupState, crate::Error> {
        let state = self.core.state.lock().unwrap();
        state
            .find_group_by_shard(shard)
            .ok_or_else(|| crate::Error::NotFound(format!("group (shard={shard:?})")))
    }

    pub fn find_group(&self, id: u64) -> Result<RouterGroupState, crate::Error> {
        let state = self.core.state.lock().unwrap();
        let group = state.group_id_lookup.get(&id).cloned();
        group.ok_or_else(|| crate::Error::NotFound(format!("group (id={:?})", id)))
    }

    pub fn find_node_addr(&self, id: u64) -> Result<String, crate::Error> {
        let state = self.core.state.lock().unwrap();
        let addr = state.node_id_lookup.get(&id).cloned();
        addr.ok_or_else(|| crate::Error::NotFound(format!("node_addr (node_id={:?})", id)))
    }

    pub fn total_nodes(&self) -> usize {
        self.core.state.lock().unwrap().node_id_lookup.len()
    }
}

impl Drop for RouterCore {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl State {
    fn find_group_by_shard(&self, shard_id: u64) -> Option<RouterGroupState> {
        let (group_id, epoch) = self.shard_group_lookup.get(&shard_id).cloned()?;
        let group_state = self.group_id_lookup.get(&group_id).cloned()?;
        if group_state.epoch > epoch {
            // This shard doesn't belongs to this group anymore.
            trace!(
                "shard {} doesn't belongs to group {}, group epoch {}, shard cached epoch {}",
                shard_id,
                group_id,
                Epoch(group_state.epoch),
                Epoch(epoch)
            );
            None
        } else {
            Some(group_state)
        }
    }

    fn apply_update_event(&mut self, event: UpdateEvent) {
        match event {
            UpdateEvent::Node(node_desc) => {
                self.node_id_lookup.insert(node_desc.id, node_desc.addr);
            }
            UpdateEvent::Group(group_desc) => {
                self.apply_group_descriptor(group_desc);
            }
            UpdateEvent::GroupState(group_state) => {
                trace!("update event; group state {group_state:?}");
                let id = group_state.group_id;
                if let Some(group) = self.group_id_lookup.get_mut(&id) {
                    group.leader_state = leader_state(&group_state);
                } else {
                    self.cached_group_states.insert(id, group_state);
                }
            }
            UpdateEvent::Database(db_desc) => {
                let desc = db_desc.clone();
                let (id, name) = (db_desc.id, db_desc.name);
                if let Some(old_desc) = self.db_id_lookup.insert(id, desc) {
                    if old_desc.name != name {
                        self.db_name_lookup.remove(&name);
                    }
                }
                self.db_name_lookup.insert(name, id);
            }
            UpdateEvent::Table(co_desc) => {
                let desc = co_desc.clone();
                let (id, name, db) = (co_desc.id, co_desc.name, co_desc.db);
                if let Some(old_desc) = self.co_id_lookup.insert(id, desc) {
                    if old_desc.name != name {
                        self.co_name_lookup.remove(&(db, old_desc.name));
                    }
                }
                self.co_name_lookup.insert((db, name), id);
            }
        }
    }

    fn apply_group_descriptor(&mut self, group_desc: GroupDesc) {
        trace!("update event; group {group_desc:?}");
        let (id, epoch) = (group_desc.id, group_desc.epoch);
        let (shards, replicas) = (group_desc.shards, group_desc.replicas);

        let replicas =
            replicas.into_iter().map(|d| (d.id, d)).collect::<HashMap<u64, ReplicaDesc>>();
        let mut group_state = RouterGroupState { id, epoch, leader_state: None, replicas };
        if let Some(old_state) = self.group_id_lookup.get(&id) {
            group_state.leader_state = old_state.leader_state;
        } else if let Some(cached_state) = self.cached_group_states.remove(&id) {
            group_state.leader_state = leader_state(&cached_state);
        }
        self.group_id_lookup.insert(id, group_state);

        for shard in shards {
            trace!(
                "apply group desc, update shard {} to {:?}, group {}, epoch {}",
                shard.id,
                shard,
                id,
                Epoch(epoch)
            );
            match self.shard_group_lookup.get_mut(&shard.id) {
                None => {
                    self.shard_group_lookup.insert(shard.id, (id, epoch));
                }
                Some((entry_id, entry_epoch)) => {
                    if *entry_epoch < epoch {
                        *entry_id = id;
                        *entry_epoch = epoch;
                    }
                }
            }

            let co_shards_lookup = &mut self.co_shards_lookup;
            match co_shards_lookup.get_mut(&shard.table_id) {
                None => {
                    co_shards_lookup.insert(shard.table_id, vec![shard]);
                }
                Some(shards) => {
                    shards.retain(|s| s.id != shard.id);
                    shards.push(shard);
                }
            }
        }
    }

    fn apply_delete_event(&mut self, event: DeleteEvent) {
        match event {
            DeleteEvent::Node(node) => {
                self.node_id_lookup.remove(&node);
            }
            DeleteEvent::Group(_) => todo!(),
            DeleteEvent::GroupState(_) => todo!(),
            DeleteEvent::Database(db) => {
                if let Some(desc) = self.db_id_lookup.remove(&db) {
                    self.db_name_lookup.remove(desc.name.as_str());
                }
            }
            DeleteEvent::Table(co) => {
                if let Some(desc) = self.co_id_lookup.remove(&co) {
                    self.co_name_lookup.remove(&(desc.db, desc.name));
                }
            }
        }
    }
}

async fn state_main(state: Arc<Mutex<State>>, root_client: RootClient) {
    info!("start watching events...");

    let mut interval = 1;
    loop {
        let cur_group_epochs = {
            let state = state.lock().unwrap();
            state.group_id_lookup.iter().map(|(id, s)| (*id, s.epoch)).collect()
        };
        let events = match root_client.watch(cur_group_epochs).await {
            Ok(events) => events,
            Err(e) => {
                warn!("watch events: {e:?}");
                tokio::time::sleep(Duration::from_millis(interval)).await;
                interval = std::cmp::min(interval * 2, 1000);
                continue;
            }
        };

        interval = 1;
        watch_events(state.as_ref(), events).await;
    }
}

async fn watch_events(state: &Mutex<State>, mut events: Streaming<WatchResponse>) {
    while let Some(event) = events.next().await {
        let (updates, deletes) = match event {
            Ok(resp) => (resp.updates, resp.deletes),
            Err(status) => {
                warn!("WatchEvent error: {}", status);
                continue;
            }
        };
        for update in updates {
            if let Some(event) = update.event {
                let mut state = state.lock().unwrap();
                state.apply_update_event(event);
            }
        }
        for delete in deletes {
            if let Some(event) = delete.event {
                let mut state = state.lock().unwrap();
                state.apply_delete_event(event);
            }
        }
    }
}

#[inline]
fn leader_state(group_state: &GroupState) -> Option<(u64, u64)> {
    if let Some(_leader_id) = group_state.leader_id {
        // FIXME: This is a temporary solution to bypass issue #1014.
        // group_state
        //     .replicas
        //     .iter()
        //     .find(|r| r.replica_id == leader_id)
        //     .map(|r| (leader_id, r.term))
        let mut candidates = group_state
            .replicas
            .iter()
            .filter(|r| r.role == RaftRole::Leader as i32)
            .map(|r| (r.replica_id, r.term))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(_, term)| *term);
        candidates.pop()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn shard(id: u64) -> ShardDesc {
        ShardDesc { id, table_id: 1, range: Some(RangePartition { start: vec![], end: vec![] }) }
    }

    fn descriptor(id: u64, epoch: u64) -> GroupDesc {
        GroupDesc { id, epoch, shards: vec![], replicas: vec![] }
    }

    #[test]
    fn update_shard_by_group_descriptor() {
        // Shard 1 migrated from group 1 to group 2.

        // case 1: group 2 leader report is lost.
        {
            let mut state = State::default();
            let mut desc = descriptor(1, 1);
            desc.shards.push(shard(1));
            state.apply_group_descriptor(desc);
            state.apply_group_descriptor(descriptor(2, 1));
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 1));

            // shard migrated to group 2.
            let group_1 = descriptor(1, 1 + (1 << 32));
            state.apply_group_descriptor(group_1);
            assert!(state.find_group_by_shard(1).is_none());
        }

        // case 2: group 2 report before group 1
        {
            let mut state = State::default();
            let mut desc = descriptor(1, 1);
            desc.shards.push(shard(1));
            state.apply_group_descriptor(desc);
            state.apply_group_descriptor(descriptor(2, 1));
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 1));

            // shard migrated to group 2.
            let mut group_2 = descriptor(2, 1 + (1 << 32));
            group_2.shards.push(shard(1));
            state.apply_group_descriptor(group_2);
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 2));

            let group_1 = descriptor(1, 1 + (1 << 32));
            state.apply_group_descriptor(group_1);
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 2));
        }

        // case 3: group 1 change configs before moving shard finished.
        {
            let mut state = State::default();
            let mut desc = descriptor(1, 1);
            desc.shards.push(shard(1));
            state.apply_group_descriptor(desc);
            state.apply_group_descriptor(descriptor(2, 1));
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 1));

            // shard migrated to group 2.
            let mut group_2 = descriptor(2, 1 + (1 << 32));
            group_2.shards.push(shard(1));
            state.apply_group_descriptor(group_2);
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 2));

            // group 1 change configs before moving shard finished.
            let group_1 = descriptor(1, 2);
            state.apply_group_descriptor(group_1);

            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 2));

            // group 1 finish moving shard.
            let group_1 = descriptor(1, 2 + (1 << 32));
            state.apply_group_descriptor(group_1);
            let find = state.find_group_by_shard(1);
            assert!(matches!(find, Some(RouterGroupState { id, .. }) if id == 2));
        }
    }
}
