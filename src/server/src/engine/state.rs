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

use std::sync::Arc;

use sekas_api::server::v1::*;

use crate::constants::STATE_REPLICA_ID;
use crate::serverpb::v1::*;
use crate::Result;

/// A structure supports saving and loading local states.
///
/// Local states:
/// - node ident
/// - root node descriptors
/// - replica states
///
/// NOTE: The group descriptors is stored in the corresponding GroupEngine,
/// which is to ensure that both the changes of group descriptor and data are
/// persisted to disk in atomic.
#[derive(Clone)]
pub struct StateEngine
where
    Self: Send + Sync,
{
    raw: Arc<raft_engine::Engine>,
}

impl StateEngine {
    pub fn new(raw: Arc<raft_engine::Engine>) -> Self {
        StateEngine { raw }
    }

    /// Read node ident from engine. `None` is returned if no such ident exists.
    pub async fn read_ident(&self) -> Result<Option<NodeIdent>> {
        Ok(self.raw.get_message::<NodeIdent>(STATE_REPLICA_ID, keys::node_ident())?)
    }

    /// Save node ident, return appropriate error if ident already exists.
    pub async fn save_ident(&self, ident: &NodeIdent) -> Result<()> {
        use raft_engine::LogBatch;

        let mut lb = LogBatch::default();
        lb.put_message(STATE_REPLICA_ID, keys::node_ident().to_owned(), ident)
            .expect("NodeIdent is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Save root desc.
    pub async fn save_root_desc(&self, root_desc: &RootDesc) -> Result<()> {
        use raft_engine::LogBatch;

        let mut lb = LogBatch::default();
        lb.put_message(STATE_REPLICA_ID, keys::root_desc().to_owned(), root_desc)
            .expect("RootDesc is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Load root desc. `None` is returned if there no any root node records
    /// exists.
    pub async fn load_root_desc(&self) -> Result<Option<RootDesc>> {
        Ok(self.raw.get_message::<RootDesc>(STATE_REPLICA_ID, keys::root_desc())?)
    }

    /// Save replica state.
    pub async fn save_replica_state(
        &self,
        group_id: u64,
        replica_id: u64,
        state: ReplicaLocalState,
    ) -> Result<()> {
        use raft_engine::LogBatch;

        let replica_meta = ReplicaMeta { group_id, replica_id, state: state.into() };

        let mut lb = LogBatch::default();
        let state_key = keys::replica_state(replica_id);
        lb.put_message(STATE_REPLICA_ID, state_key.to_vec(), &replica_meta)
            .expect("RootDesc is Serializable");
        self.raw.write(&mut lb, false)?;
        Ok(())
    }

    /// Fetch all replica states.
    pub async fn replica_states(&self) -> Result<Vec<(u64, u64, ReplicaLocalState)>> {
        let mut replica_states = Vec::default();
        let start_key = keys::replica_state_prefix();
        let end_key = keys::replica_state_end();
        self.raw.scan_messages(
            STATE_REPLICA_ID,
            Some(start_key),
            Some(end_key),
            false,
            |_, replica_meta: ReplicaMeta| {
                let replica_id = replica_meta.replica_id;
                let group_id = replica_meta.group_id;
                let local_state = ReplicaLocalState::from_i32(replica_meta.state)
                    .expect("invalid ReplicaLocalState value");
                replica_states.push((group_id, replica_id, local_state));
                true
            },
        )?;

        Ok(replica_states)
    }
}

mod keys {
    const IDENT_KEY: &[u8] = &[0x1];
    const ROOT_DESCRIPTOR_KEY: &[u8] = &[0x2];
    const REPLICA_STATE_PREFIX: &[u8] = &[0x3];
    const REPLICA_STATE_END: &[u8] = &[0x4];

    pub fn node_ident() -> &'static [u8] {
        IDENT_KEY
    }

    pub fn root_desc() -> &'static [u8] {
        ROOT_DESCRIPTOR_KEY
    }

    pub fn replica_state_prefix() -> &'static [u8] {
        REPLICA_STATE_PREFIX
    }

    pub fn replica_state_end() -> &'static [u8] {
        REPLICA_STATE_END
    }

    pub fn replica_state(replica_id: u64) -> [u8; 9] {
        let mut buf = [0; 9];
        buf[..1].copy_from_slice(REPLICA_STATE_PREFIX);
        buf[1..].copy_from_slice(&replica_id.to_le_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::open_raft_engine;
    use crate::runtime::ExecutorOwner;

    #[test]
    fn save_and_load_node_ident() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = StateEngine::new(Arc::new(open_raft_engine(dir.path()).unwrap()));

        executor.block_on(async move {
            // Read ident not exists.
            let ident = engine.read_ident().await.unwrap();
            assert!(ident.is_none());

            // Save ident
            let ident = NodeIdent { cluster_id: vec![1, 7, 9, 3, 9, 4], node_id: 123321 };
            engine.save_ident(&ident).await.unwrap();

            // Read ident again.
            let ident_read = engine.read_ident().await.unwrap();
            assert!(matches!(ident_read, Some(read) if read == ident));
        });
    }

    #[test]
    fn save_and_load_root_desc() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = StateEngine::new(Arc::new(open_raft_engine(dir.path()).unwrap()));

        executor.block_on(async move {
            // Load node desc not exists.
            let desc = engine.load_root_desc().await.unwrap();
            assert!(desc.is_none());

            // Save node desc.
            let desc = RootDesc {
                epoch: 123123,
                root_nodes: vec![NodeDesc {
                    id: 123123,
                    addr: "localhost:10011".into(),
                    capacity: None,
                    status: NodeStatus::Active.into(),
                }],
            };
            engine.save_root_desc(&desc).await.unwrap();

            // Load root desc again.
            let load_desc = engine.load_root_desc().await.unwrap();
            assert!(matches!(load_desc, Some(read) if read == desc));
        });
    }

    #[test]
    fn save_and_read_replica_states() {
        let executor_owner = ExecutorOwner::new(1);
        let executor = executor_owner.executor();
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = StateEngine::new(Arc::new(open_raft_engine(dir.path()).unwrap()));

        executor.block_on(async move {
            let expect_states = vec![
                (1, 1, ReplicaLocalState::Normal),
                (2, 2, ReplicaLocalState::Pending),
                (3, 3, ReplicaLocalState::Terminated),
                (3, 4, ReplicaLocalState::Tombstone),
            ];
            for (group_id, replica_id, state) in expect_states.clone() {
                engine.save_replica_state(group_id, replica_id, state).await.unwrap();
            }
            let read_states = engine.replica_states().await.unwrap();
            assert_eq!(expect_states, read_states);
        });
    }
}
