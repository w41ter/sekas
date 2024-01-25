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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use futures::lock::Mutex;
use lazy_static::lazy_static;
use log::{debug, info, warn};
use prost::Message;
use sekas_api::server::v1::watch_response::{delete_event, update_event, DeleteEvent, UpdateEvent};
use sekas_api::server::v1::{DatabaseDesc, PutRequest, TableDesc, *};
use sekas_rock::time::timestamp_nanos;
use sekas_schema::system::col;

use super::store::RootStore;
use crate::constants::*;
use crate::engine::{GroupEngine, SnapshotMode};
use crate::serverpb::v1::BackgroundJob;
use crate::transport::TransportManager;
use crate::{Error, Result};

const META_CLUSTER_ID_KEY: &str = "cluster_id";
const META_TABLE_ID_KEY: &str = "table_id";
const META_DATABASE_ID_KEY: &str = "database_id";
const META_GROUP_ID_KEY: &str = "group_id";
const META_NODE_ID_KEY: &str = "node_id";
const META_REPLICA_ID_KEY: &str = "replica_id";
const META_SHARD_ID_KEY: &str = "shard_id";
const META_JOB_ID_KEY: &str = "job_id";
const META_TXN_ID_KEY: &str = "txn_id";

lazy_static! {
    pub static ref ID_GEN_LOCKS: HashMap<String, Mutex<()>> = HashMap::from([
        (META_CLUSTER_ID_KEY.to_owned(), Mutex::new(())),
        (META_TABLE_ID_KEY.to_owned(), Mutex::new(())),
        (META_DATABASE_ID_KEY.to_owned(), Mutex::new(())),
        (META_GROUP_ID_KEY.to_owned(), Mutex::new(())),
        (META_NODE_ID_KEY.to_owned(), Mutex::new(())),
        (META_REPLICA_ID_KEY.to_owned(), Mutex::new(())),
        (META_SHARD_ID_KEY.to_owned(), Mutex::new(())),
        (META_JOB_ID_KEY.to_owned(), Mutex::new(())),
    ]);
}

#[derive(Clone)]
pub struct Schema {
    store: Arc<RootStore>,
}

// public interface.
impl Schema {
    pub fn new(store: Arc<RootStore>) -> Self {
        Self { store }
    }

    pub async fn cluster_id(&self) -> Result<Option<Vec<u8>>> {
        self.get_meta(META_CLUSTER_ID_KEY.as_bytes()).await
    }

    pub async fn create_database(&self, desc: DatabaseDesc) -> Result<DatabaseDesc> {
        if self.get_database(&desc.name).await?.is_some() {
            warn!("create database but it already exists. database={}", desc.name);
            return Err(Error::AlreadyExists(format!("database {}", desc.name.to_owned())));
        }

        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_DATABASE_ID_KEY).await?;
        self.put_database(desc.clone()).await?;
        Ok(desc)
    }

    pub async fn get_database(&self, name: &str) -> Result<Option<DatabaseDesc>> {
        let val = self.get(col::DATABASE_ID, name.as_bytes()).await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = DatabaseDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("database desc: {}", name)))?;
        Ok(Some(desc))
    }

    pub async fn update_database(&self, _desc: DatabaseDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_database(&self, db: &DatabaseDesc) -> Result<u64> {
        self.delete(col::DATABASE_ID, db.name.as_bytes()).await?;
        Ok(db.id)
    }

    pub async fn list_database(&self) -> Result<Vec<DatabaseDesc>> {
        let values = self.list(col::DATABASE_ID).await?;
        let mut databases = Vec::new();
        for val in values {
            databases.push(
                DatabaseDesc::decode(&*val)
                    .map_err(|_| Error::InvalidData("database desc".into()))?,
            );
        }
        Ok(databases)
    }

    pub async fn prepare_create_table(&self, desc: TableDesc) -> Result<TableDesc> {
        if self.get_table(desc.db, &desc.name).await?.is_some() {
            return Err(Error::AlreadyExists(format!("table {}", desc.name.to_owned())));
        }
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_TABLE_ID_KEY).await?;
        Ok(desc)
    }

    pub async fn create_table(&self, desc: TableDesc) -> Result<TableDesc> {
        assert!(self.get_table(desc.db, &desc.name).await?.is_none());
        self.put_col(desc.clone()).await?;
        Ok(desc)
    }

    pub async fn get_table(&self, database: u64, table: &str) -> Result<Option<TableDesc>> {
        let val = self.get(col::TABLE_ID, &table_key(database, table)).await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = TableDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("table desc: {}, {}", database, table)))?;
        Ok(Some(desc))
    }

    pub async fn get_table_shards(&self, table_id: u64) -> Result<Vec<(u64, ShardDesc)>> {
        Ok(self
            .list_group()
            .await?
            .iter()
            .flat_map(|g| {
                g.shards.iter().filter(|s| s.table_id == table_id).map(|s| (g.id, s.to_owned()))
            })
            .collect::<Vec<_>>())
    }

    pub async fn update_table(&self, _desc: TableDesc) -> Result<()> {
        todo!()
    }

    pub async fn delete_table(&self, table: TableDesc) -> Result<()> {
        self.delete(col::TABLE_ID, &table_key(table.db, &table.name)).await
    }

    pub async fn list_table(&self) -> Result<Vec<TableDesc>> {
        let values = self.list(col::TABLE_ID).await?;
        let mut tables = Vec::new();
        for val in values {
            let c =
                TableDesc::decode(&*val).map_err(|_| Error::InvalidData("table desc".into()))?;
            tables.push(c);
        }
        Ok(tables)
    }

    pub async fn list_database_tables(&self, database: u64) -> Result<Vec<TableDesc>> {
        let tables = self.list_table().await?;
        Ok(tables.into_iter().filter(|c| c.db == database).collect::<Vec<_>>())
    }

    pub async fn add_node(&self, desc: NodeDesc) -> Result<NodeDesc> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_NODE_ID_KEY).await?;
        self.put_node(desc.clone()).await?;
        Ok(desc)
    }

    pub async fn get_node(&self, id: u64) -> Result<Option<NodeDesc>> {
        let val = self.get(col::NODE_ID, &id.to_le_bytes()).await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = NodeDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("node desc: {}", id)))?;
        Ok(Some(desc))
    }

    pub async fn delete_node(&self, id: u64) -> Result<()> {
        self.delete(col::NODE_ID, &id.to_le_bytes()).await
    }

    pub async fn update_node(&self, desc: NodeDesc) -> Result<()> {
        self.put_node(desc).await?;
        Ok(())
    }

    pub async fn list_node(&self) -> Result<Vec<NodeDesc>> {
        let values = self.list(col::NODE_ID).await?;
        let mut nodes = Vec::new();
        for val in values {
            nodes
                .push(NodeDesc::decode(&*val).map_err(|_| Error::InvalidData("node desc".into()))?);
        }
        Ok(nodes)
    }

    pub(crate) async fn list_node_raw(engine: GroupEngine) -> Result<Vec<NodeDesc>> {
        let shard_id = col::shard_id(col::NODE_ID);
        let mut snapshot = match engine.snapshot(shard_id, SnapshotMode::Prefix { key: &[] }) {
            Ok(snapshot) => snapshot,
            Err(Error::ShardNotFound(_)) => {
                // This replica of root group haven't initialized.
                return Ok(vec![]);
            }
            Err(e) => {
                warn!("root list nodes raw: {e:?}");
                return Err(e);
            }
        };
        let mut nodes = Vec::new();
        while let Some(mvcc_iter) = snapshot.next() {
            for entry in mvcc_iter? {
                let entry = entry?;
                if let Some(val) = entry.value() {
                    nodes.push(
                        NodeDesc::decode(val)
                            .map_err(|_| Error::InvalidData("node desc".into()))?,
                    );
                }
            }
        }

        Ok(nodes)
    }

    pub async fn update_group_replica(
        &self,
        group: Option<GroupDesc>,
        replica: Option<ReplicaState>,
    ) -> Result<()> {
        if let Some(replica) = replica {
            self.put_replica_state(replica).await?;
        }
        if let Some(group) = group {
            self.put_group(group).await?;
        }
        Ok(())
    }

    pub async fn remove_replica_state(&self, group_id: u64, replica_id: u64) -> Result<()> {
        let key = replica_key(group_id, replica_id);
        self.delete(col::REPLICA_STATE_ID, &key).await
    }

    pub async fn get_group(&self, id: u64) -> Result<Option<GroupDesc>> {
        let val = self.get(col::GROUP_ID, &id.to_le_bytes()).await?;
        if val.is_none() {
            return Ok(None);
        }
        let desc = GroupDesc::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData(format!("group desc: {}", id)))?;
        Ok(Some(desc))
    }

    pub async fn delete_group(&self, id: u64) -> Result<()> {
        // TODO: prefix delete replica_state
        self.delete(col::GROUP_ID, &id.to_le_bytes()).await
    }

    pub async fn list_group(&self) -> Result<Vec<GroupDesc>> {
        let values = self.list(col::GROUP_ID).await?;
        let mut groups = Vec::new();
        for val in values {
            groups.push(
                GroupDesc::decode(&*val).map_err(|_| Error::InvalidData("group desc".into()))?,
            );
        }
        Ok(groups)
    }

    pub async fn get_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Option<ReplicaState>> {
        let key = replica_key(group_id, replica_id);
        let val = self.get(col::REPLICA_STATE_ID, &key).await?;
        if val.is_none() {
            return Ok(None);
        }
        let state = ReplicaState::decode(&*val.unwrap()).map_err(|_| {
            Error::InvalidData(format!(
                "replica_state: group: {}, replica: {}",
                group_id, replica_id
            ))
        })?;
        Ok(Some(state))
    }

    pub async fn list_replica_state(&self) -> Result<Vec<ReplicaState>> {
        let values = self.list(col::REPLICA_STATE_ID).await?;
        let mut states = Vec::with_capacity(values.len());
        for val in values {
            let state = ReplicaState::decode(&*val)
                .map_err(|_| Error::InvalidData("replica state desc".into()))?;
            states.push(state.to_owned());
        }
        Ok(states)
    }

    pub async fn group_replica_states(&self, group_id: u64) -> Result<Vec<ReplicaState>> {
        let values =
            self.list_prefix(col::REPLICA_STATE_ID, group_id.to_le_bytes().as_slice()).await?;
        let mut states = Vec::with_capacity(values.len());
        for val in values {
            let state = ReplicaState::decode(&*val)
                .map_err(|_| Error::InvalidData("replica state desc".into()))?;
            states.push(state);
        }
        Ok(states)
    }

    pub async fn list_group_state(&self) -> Result<Vec<GroupState>> {
        let mut states: HashMap<u64, GroupState> = HashMap::new();
        for state in self.list_replica_state().await? {
            match states.entry(state.group_id) {
                Entry::Occupied(mut ent) => {
                    let group = ent.get_mut();
                    if state.role == RaftRole::Leader as i32 {
                        group.leader_id = Some(state.replica_id);
                    } else if group.leader_id == Some(state.replica_id) {
                        group.leader_id = None;
                    }
                    group.replicas.retain(|desc| desc.replica_id != state.replica_id);
                    group.replicas.push(state);
                }
                Entry::Vacant(ent) => {
                    let leader_id = if state.role == RaftRole::Leader as i32 {
                        Some(state.replica_id)
                    } else {
                        None
                    };
                    ent.insert(GroupState {
                        group_id: state.group_id,
                        leader_id,
                        replicas: vec![state],
                    });
                }
            }
        }
        Ok(states.into_values().collect())
    }

    pub async fn get_root_desc(&self) -> Result<RootDesc> {
        let group_desc =
            self.get_group(ROOT_GROUP_ID).await?.ok_or(Error::GroupNotFound(ROOT_GROUP_ID))?;
        let mut nodes = HashMap::new();
        for replica in &group_desc.replicas {
            let node = replica.node_id;
            if nodes.contains_key(&node) {
                continue;
            }
            let node = self
                .get_node(node)
                .await?
                .ok_or_else(|| Error::InvalidData(format!("node {} data not found", node)))?;
            nodes.insert(node.id, node);
        }
        Ok(RootDesc {
            epoch: group_desc.epoch,
            root_nodes: nodes.into_values().collect::<Vec<_>>(),
        })
    }

    pub async fn list_all_events(
        &self,
        cur_groups: HashMap<u64, u64>,
    ) -> Result<(Vec<UpdateEvent>, Vec<DeleteEvent>)> {
        let mut updates = Vec::new();
        let mut deletes = Vec::new();

        // list nodes.
        let nodes = self
            .list_node()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent { event: Some(update_event::Event::Node(desc)) })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&nodes);

        // list databases.
        let dbs = self
            .list_database()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent { event: Some(update_event::Event::Database(desc)) })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&dbs);

        // list tables.
        let tables = self
            .list_table()
            .await?
            .into_iter()
            .map(|desc| UpdateEvent { event: Some(update_event::Event::Table(desc)) })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&tables);

        // list groups.
        let groups = self
            .list_group()
            .await?
            .into_iter()
            .map(|desc| (desc.id, desc))
            .collect::<HashMap<u64, GroupDesc>>();

        let changed_groups = groups
            .iter()
            .filter(|(_, desc)| {
                if let Some(cur_epoch) = cur_groups.get(&desc.id) {
                    desc.epoch > *cur_epoch
                } else {
                    true
                }
            })
            .map(|(id, desc)| (id.to_owned(), desc.to_owned()))
            .collect::<HashMap<u64, GroupDesc>>();

        updates.extend_from_slice(
            &changed_groups
                .values()
                .map(|desc| UpdateEvent {
                    event: Some(update_event::Event::Group(desc.to_owned())),
                })
                .collect::<Vec<_>>(),
        );

        if !cur_groups.is_empty() {
            let deleted = cur_groups
                .keys()
                .filter(|group_id| !groups.contains_key(group_id))
                .collect::<Vec<_>>();
            let delete_desc = deleted
                .iter()
                .map(|id| DeleteEvent { event: Some(delete_event::Event::Group(**id)) })
                .collect::<Vec<_>>();
            let delete_state = deleted
                .iter()
                .map(|id| DeleteEvent { event: Some(delete_event::Event::GroupState(**id)) })
                .collect::<Vec<_>>();
            deletes.extend_from_slice(&delete_desc);
            deletes.extend_from_slice(&delete_state);
        }

        // list group_state.
        let group_states = self
            .list_group_state()
            .await?
            .into_iter()
            .filter(|desc| changed_groups.contains_key(&desc.group_id))
            .map(|desc| UpdateEvent { event: Some(update_event::Event::GroupState(desc)) })
            .collect::<Vec<UpdateEvent>>();
        updates.extend_from_slice(&group_states);

        Ok((updates, deletes))
    }

    pub async fn append_job(&self, desc: BackgroundJob) -> Result<BackgroundJob> {
        let mut desc = desc.to_owned();
        desc.id = self.next_id(META_JOB_ID_KEY).await?;
        self.put_job(desc.to_owned()).await?;
        Ok(desc)
    }

    pub async fn remove_job(&self, job: &BackgroundJob) -> Result<()> {
        // FIXME(walter) not atomic!!!
        self.put_job_history(job.to_owned()).await?;
        self.delete(col::JOB_ID, &job.id.to_le_bytes()).await?;
        Ok(())
    }

    pub async fn update_job(&self, desc: BackgroundJob) -> Result<bool> {
        if self.get(col::JOB_ID, &desc.id.to_be_bytes()).await?.is_none() {
            // TODO: replace this with storage put_condition operation.
            return Ok(false);
        }
        self.put_job(desc).await?;
        Ok(true)
    }

    pub async fn list_job(&self) -> Result<Vec<BackgroundJob>> {
        let values = self.list(col::JOB_ID).await?;
        let mut jobs = Vec::with_capacity(values.len());
        for val in values {
            let job = BackgroundJob::decode(&*val)
                .map_err(|_| Error::InvalidData("background job".into()))?;
            jobs.push(job.to_owned());
        }
        Ok(jobs)
    }

    pub async fn list_history_job(&self) -> Result<Vec<BackgroundJob>> {
        let values = self.list(col::JOB_HISTORY_ID).await?;
        let mut jobs = Vec::with_capacity(values.len());
        for val in values {
            let job = BackgroundJob::decode(&*val)
                .map_err(|_| Error::InvalidData("background job".into()))?;
            jobs.push(job.to_owned());
        }
        Ok(jobs)
    }

    pub async fn get_job_history(&self, id: &u64) -> Result<Option<BackgroundJob>> {
        let val = self.get(col::JOB_HISTORY_ID, &id.to_le_bytes()).await?;
        if val.is_none() {
            return Ok(None);
        }
        let job = BackgroundJob::decode(&*val.unwrap())
            .map_err(|_| Error::InvalidData("background job".into()))?;
        Ok(Some(job))
    }

    pub async fn max_txn_id(&self) -> Result<u64> {
        let txn_id = self
            .get_meta(META_TXN_ID_KEY.as_bytes())
            .await?
            .ok_or_else(|| Error::InvalidData("txn id".to_owned()))?;
        Ok(u64::from_le_bytes(
            txn_id.try_into().map_err(|_| Error::InvalidData("txn id".to_owned()))?,
        ))
    }

    pub async fn set_txn_id(&self, next_txn_id: u64) -> Result<()> {
        // TODO(walter) how about add a write condition here?
        self.put_meta(META_TXN_ID_KEY.as_bytes(), next_txn_id.to_le_bytes().to_vec()).await?;
        Ok(())
    }
}

pub struct ReplicaNodes(pub Vec<NodeDesc>);

impl From<ReplicaNodes> for Vec<NodeDesc> {
    fn from(r: ReplicaNodes) -> Self {
        r.0
    }
}

impl ReplicaNodes {
    pub fn move_first(&mut self, id: u64) {
        if let Some(idx) = self.0.iter().position(|n| n.id == id) {
            if idx != 0 {
                self.0.swap(0, idx)
            }
        }
    }
}

// bootstrap schema.
impl Schema {
    pub async fn try_bootstrap_root(
        &mut self,
        addr: &str,
        cfg_cpu_nums: u32,
        cluster_id: Vec<u8>,
    ) -> Result<()> {
        debug_assert_ne!(cfg_cpu_nums, 0);
        let _timer = super::metrics::BOOTSTRAP_DURATION_SECONDS.start_timer();

        if let Some(exist_cluster_id) = self.cluster_id().await? {
            if exist_cluster_id != cluster_id {
                warn!(
                    "cluster not match. exist_cluster={}, new_cluster={}",
                    String::from_utf8_lossy(&exist_cluster_id),
                    String::from_utf8_lossy(&cluster_id)
                );
                return Err(Error::ClusterNotMatch);
            }
            debug!(
                "cluster has been bootstrapped, cluster={}",
                String::from_utf8_lossy(&cluster_id)
            );
            return Ok(());
        }

        info!("start boostrap root. cluster={}", String::from_utf8_lossy(&cluster_id));

        self.put_database(sekas_schema::system::db::database_desc()).await?;
        let node_desc = NodeDesc {
            id: FIRST_NODE_ID,
            addr: addr.into(),
            capacity: Some(NodeCapacity {
                cpu_nums: cfg_cpu_nums as f64,
                replica_count: 1,
                leader_count: 0,
            }),
            status: NodeStatus::Active as i32,
        };
        self.put_node(node_desc).await?;

        // Put root group and replica state.
        self.put_group(sekas_schema::system::root_group()).await?;

        let replica_state = ReplicaState {
            replica_id: FIRST_REPLICA_ID,
            group_id: ROOT_GROUP_ID,
            term: 0,
            voted_for: FIRST_REPLICA_ID,
            role: RaftRole::Leader.into(),
            node_id: FIRST_NODE_ID,
        };
        self.put_replica_state(replica_state).await?;

        // Put user group and replica state.
        self.put_group(sekas_schema::system::init_group()).await?;

        let replica_state = ReplicaState {
            replica_id: INIT_USER_REPLICA_ID,
            group_id: FIRST_GROUP_ID,
            term: 0,
            voted_for: INIT_USER_REPLICA_ID,
            role: RaftRole::Leader.into(),
            node_id: FIRST_NODE_ID,
        };
        self.put_replica_state(replica_state).await?;

        let mut batch =
            ShardWriteRequest { shard_id: col::shard_id(col::TABLE_ID), ..Default::default() };
        for col in sekas_schema::system::tables() {
            batch.puts.push(PutRequest {
                key: table_key(col.db, &col.name),
                value: col.encode_to_vec(),
                ..Default::default()
            });
        }
        self.batch_write(batch).await?;

        // ATTN: init meta table will setup cluster id, so it must be the last step
        // of bootstrap root.
        self.init_meta_table(cluster_id.to_owned()).await?;

        info!("boostrap root successfully. cluster={}", String::from_utf8_lossy(&cluster_id));

        Ok(())
    }

    pub async fn next_group_id(&self) -> Result<u64> {
        self.next_id(META_GROUP_ID_KEY).await
    }

    pub async fn next_replica_id(&self) -> Result<u64> {
        self.next_id(META_REPLICA_ID_KEY).await
    }

    pub async fn next_shard_id(&self) -> Result<u64> {
        self.next_id(META_SHARD_ID_KEY).await
    }

    async fn init_meta_table(&self, cluster_id: Vec<u8>) -> Result<()> {
        let mut batch =
            ShardWriteRequest { shard_id: col::shard_id(col::META_ID), ..Default::default() };
        let mut put_meta =
            |key, value| batch.puts.push(PutRequest { key, value, ..Default::default() });
        put_meta(META_CLUSTER_ID_KEY.into(), cluster_id);
        put_meta(
            META_DATABASE_ID_KEY.into(),
            sekas_schema::FIRST_USER_DATABASE_ID.to_le_bytes().to_vec(),
        );
        put_meta(
            META_TABLE_ID_KEY.into(),
            sekas_schema::FIRST_USER_TABLE_ID.to_le_bytes().to_vec(),
        );
        put_meta(META_GROUP_ID_KEY.into(), (FIRST_GROUP_ID + 1).to_le_bytes().to_vec());
        put_meta(META_NODE_ID_KEY.into(), (FIRST_NODE_ID + 1).to_le_bytes().to_vec());
        put_meta(META_REPLICA_ID_KEY.into(), (INIT_USER_REPLICA_ID + 1).to_le_bytes().to_vec());
        put_meta(
            META_SHARD_ID_KEY.into(),
            sekas_schema::FIRST_USER_SHARD_ID.to_le_bytes().to_vec(),
        );
        put_meta(META_JOB_ID_KEY.into(), INITIAL_JOB_ID.to_le_bytes().to_vec());
        put_meta(META_TXN_ID_KEY.into(), timestamp_nanos().to_le_bytes().to_vec());
        self.batch_write(batch).await?;
        Ok(())
    }
}

// internal methods.
impl Schema {
    async fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(col::META_ID, key).await
    }

    async fn batch_write(&self, batch: ShardWriteRequest) -> Result<()> {
        self.store.batch_write(batch).await
    }

    #[inline]
    async fn get(&self, table_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let rs = self.store.get(col::shard_id(table_id), key).await;
        sekas_runtime::yield_now().await;
        rs
    }

    #[inline]
    async fn delete(&self, table_id: u64, key: &[u8]) -> Result<()> {
        self.store.delete(col::shard_id(table_id), key).await
    }

    #[inline]
    async fn put(&self, table_id: u64, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.store.put(col::shard_id(table_id), key.to_owned(), value).await
    }

    async fn list(&self, table_id: u64) -> Result<Vec<Vec<u8>>> {
        let rs = self.list_prefix(table_id, &[]).await;
        sekas_runtime::yield_now().await;
        rs
    }

    async fn list_prefix(&self, table_id: u64, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.store.list(col::shard_id(table_id), prefix).await
    }

    async fn next_id(&self, id_type: &str) -> Result<u64> {
        let _mutex = ID_GEN_LOCKS.get(id_type).expect("id gen lock not found").lock().await;
        let id = self
            .get_meta(id_type.as_bytes())
            .await?
            .ok_or_else(|| Error::InvalidData(format!("{} id", id_type)))?;
        let id = u64::from_le_bytes(
            id.try_into().map_err(|_| Error::InvalidData(format!("{} id", id_type)))?,
        );
        self.put_meta(id_type.as_bytes(), (id + 1).to_le_bytes().to_vec()).await?;
        Ok(id)
    }
}

/// A set of helper functions to simplify put logic.
impl Schema {
    #[inline]
    async fn put_database(&self, desc: DatabaseDesc) -> Result<()> {
        self.put(col::DATABASE_ID, desc.name.as_bytes(), desc.encode_to_vec()).await
    }

    #[inline]
    async fn put_group(&self, desc: GroupDesc) -> Result<()> {
        self.put(col::GROUP_ID, &desc.id.to_le_bytes(), desc.encode_to_vec()).await
    }

    #[inline]
    async fn put_replica_state(&self, state: ReplicaState) -> Result<()> {
        self.put(
            col::REPLICA_STATE_ID,
            &replica_key(state.group_id, state.replica_id),
            state.encode_to_vec(),
        )
        .await
    }

    #[inline]
    async fn put_meta(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.put(col::META_ID, key, value).await
    }

    #[inline]
    async fn put_node(&self, desc: NodeDesc) -> Result<()> {
        self.put(col::NODE_ID, &desc.id.to_le_bytes(), desc.encode_to_vec()).await
    }

    #[inline]
    async fn put_job(&self, desc: BackgroundJob) -> Result<()> {
        self.put(col::JOB_ID, &desc.id.to_le_bytes(), desc.encode_to_vec()).await
    }

    #[inline]
    async fn put_job_history(&self, desc: BackgroundJob) -> Result<()> {
        self.put(col::JOB_HISTORY_ID, &desc.id.to_le_bytes(), desc.encode_to_vec()).await
    }

    #[inline]
    async fn put_col(&self, col: TableDesc) -> Result<()> {
        self.put(col::TABLE_ID, &table_key(col.db, &col.name), col.encode_to_vec()).await
    }
}

#[derive(Clone)]
pub struct RemoteStore {
    transport_manager: TransportManager,
}

impl RemoteStore {
    pub(crate) fn new(transport_manager: TransportManager) -> Self {
        RemoteStore { transport_manager }
    }

    pub async fn list_replica_state(&self, group_id: u64) -> Result<Vec<ReplicaState>> {
        let shard_id = col::shard_id(col::REPLICA_STATE_ID);
        let prefix = group_key(group_id);

        let client = self.transport_manager.build_shard_client(ROOT_GROUP_ID, shard_id);
        let values = client.prefix_list(&prefix).await?;
        let mut states = vec![];
        for value in values {
            if let Ok(state) = ReplicaState::decode(value.as_slice()) {
                states.push(state);
            }
        }
        Ok(states)
    }

    pub async fn clear_replica_state(&self, group_id: u64, replica_id: u64) -> Result<()> {
        let shard_id = col::shard_id(col::REPLICA_STATE_ID);
        let key = replica_key(group_id, replica_id);
        let client = self.transport_manager.build_shard_client(ROOT_GROUP_ID, shard_id);
        client.delete(&key).await?;
        Ok(())
    }
}

#[inline]
fn table_key(database_id: u64, table_name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + table_name.len());
    buf.extend_from_slice(database_id.to_le_bytes().as_slice());
    buf.extend_from_slice(table_name.as_bytes());
    buf
}

#[inline]
fn group_key(group_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>());
    buf.extend_from_slice(group_id.to_le_bytes().as_slice());
    buf
}

#[inline]
fn replica_key(group_id: u64, replica_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() * 2);
    buf.extend_from_slice(group_id.to_le_bytes().as_slice());
    buf.extend_from_slice(replica_id.to_le_bytes().as_slice());
    buf
}
