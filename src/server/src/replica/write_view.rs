// Copyright 2026-present The Sekas Authors.
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

use prost::Message;
use sekas_api::server::v1::{TxnIntent, Value};
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::pending::{
    CommitFence, PendingMutationEntry, PendingMutationKind, PendingMutationOverlay,
};
use crate::engine::{GroupEngine, SnapshotMode};
use crate::{Error, Result};

#[derive(Clone)]
pub(super) struct PendingWriteView {
    overlay: PendingMutationOverlay,
}

pub(super) struct WriteEvalContext<'a> {
    engine: &'a GroupEngine,
    overlay: PendingMutationOverlay,
    dependencies: CommitFence,
}

impl PendingWriteView {
    pub fn new(overlay: PendingMutationOverlay) -> Self {
        PendingWriteView { overlay }
    }

    pub fn context<'a>(&self, engine: &'a GroupEngine) -> WriteEvalContext<'a> {
        WriteEvalContext {
            engine,
            overlay: self.overlay.clone(),
            dependencies: CommitFence::none(),
        }
    }
}

impl WriteEvalContext<'_> {
    #[cfg(test)]
    pub(super) fn new(
        engine: &GroupEngine,
        overlay: PendingMutationOverlay,
    ) -> WriteEvalContext<'_> {
        WriteEvalContext { engine, overlay, dependencies: CommitFence::none() }
    }

    pub fn engine(&self) -> &GroupEngine {
        self.engine
    }

    pub fn take_dependencies(&mut self) -> CommitFence {
        std::mem::replace(&mut self.dependencies, CommitFence::none())
    }

    pub async fn latest_value(&mut self, shard_id: u64, key: &[u8]) -> Result<Option<Value>> {
        let committed = self.engine.get(shard_id, key).await?;
        let pending = self.overlay.entries(shard_id, key);
        Ok(self.merge_latest_value(committed, pending))
    }

    pub async fn wait_pending_intent(&self, shard_id: u64, key: &[u8]) -> Result<bool> {
        let pending = self.overlay.entries(shard_id, key);
        let Some(entry) =
            pending.iter().rev().find(|entry| entry.mutation.version == TXN_INTENT_VERSION)
        else {
            return Ok(false);
        };
        if matches!(entry.mutation.kind, PendingMutationKind::Delete) {
            return Ok(false);
        }
        entry.fence.wait().await?;
        Ok(true)
    }

    pub fn intent_and_next_value(
        &mut self,
        shard_id: u64,
        key: &[u8],
    ) -> Result<(Option<TxnIntent>, Option<Value>)> {
        let mut values = self.all_values(shard_id, key)?;
        let Some(value) = values.first().cloned() else {
            return Ok((None, None));
        };
        if value.version != TXN_INTENT_VERSION {
            return Ok((None, Some(value)));
        }

        values.remove(0);
        let content = value.content.ok_or_else(|| {
            Error::InvalidData(format!("intent value must exist, shard={shard_id}, key={key:?}"))
        })?;
        Ok((Some(TxnIntent::decode(content.as_slice())?), values.into_iter().next()))
    }

    pub async fn target_intent(
        &mut self,
        start_version: u64,
        shard_id: u64,
        key: &[u8],
    ) -> Result<Option<TxnIntent>> {
        let Some(value) = self.latest_value(shard_id, key).await? else {
            return Ok(None);
        };
        if value.version != TXN_INTENT_VERSION {
            return Ok(None);
        }

        let content = value.content.ok_or_else(|| {
            Error::InvalidData(format!("txn intent without value, shard {shard_id} key {key:?}"))
        })?;
        let intent = TxnIntent::decode(content.as_slice())?;
        if intent.start_version != start_version {
            return Ok(None);
        }
        Ok(Some(intent))
    }

    fn all_values(&mut self, shard_id: u64, key: &[u8]) -> Result<Vec<Value>> {
        let committed = self.committed_values(shard_id, key)?;
        let pending = self.overlay.entries(shard_id, key);
        Ok(self.merge_values(committed, pending))
    }

    fn committed_values(&self, shard_id: u64, key: &[u8]) -> Result<Vec<Value>> {
        let mut snapshot = self.engine.snapshot(shard_id, SnapshotMode::Key { key })?;
        let Some(iter) = snapshot.next() else {
            return Ok(Vec::new());
        };
        iter?.map(|entry| entry.map(Into::<Value>::into)).collect()
    }

    fn merge_latest_value(
        &mut self,
        committed: Option<Value>,
        pending: Vec<PendingMutationEntry>,
    ) -> Option<Value> {
        if pending.is_empty() {
            return committed;
        }
        self.join_pending_fences(&pending);
        self.merge_values(committed.into_iter().collect(), pending).into_iter().next()
    }

    fn merge_values(
        &mut self,
        committed: Vec<Value>,
        pending: Vec<PendingMutationEntry>,
    ) -> Vec<Value> {
        if pending.is_empty() {
            return committed;
        }

        self.join_pending_fences(&pending);
        let mut versions = std::collections::BTreeMap::<u64, Option<Value>>::new();
        for value in committed {
            versions.insert(value.version, Some(value));
        }
        for entry in pending {
            let value = match entry.mutation.kind {
                PendingMutationKind::Put(value) => {
                    Some(Value::with_value(value, entry.mutation.version))
                }
                PendingMutationKind::Tombstone => Some(Value::tombstone(entry.mutation.version)),
                PendingMutationKind::Delete => None,
            };
            versions.insert(entry.mutation.version, value);
        }

        versions.into_iter().rev().filter_map(|(_, value)| value).collect()
    }

    fn join_pending_fences(&mut self, entries: &[PendingMutationEntry]) {
        for entry in entries {
            self.dependencies.join(entry.fence.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::Value;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{WriteBatch, WriteStates, create_group_engine};
    use crate::replica::pending::PendingMutation;

    const SHARD_ID: u64 = 1;

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, SHARD_ID, key, value, *version).unwrap();
            } else {
                engine.tombstone(&mut wb, SHARD_ID, key, *version).unwrap();
            }
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    #[sekas_macro::test]
    async fn latest_value_reads_pending_mutation() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        commit_values(&engine, b"k", &[Value::with_value(b"committed".to_vec(), 10)]);

        let overlay = PendingMutationOverlay::default();
        overlay.insert_batch(
            &[PendingMutation::put(SHARD_ID, b"k".to_vec(), b"pending".to_vec(), 20)],
            CommitFence::none(),
        );
        let mut ctx = WriteEvalContext::new(&engine, overlay);

        let value = ctx.latest_value(SHARD_ID, b"k").await.unwrap().unwrap();
        assert_eq!(value.version, 20);
        assert_eq!(value.content.as_deref(), Some(&b"pending"[..]));
    }

    #[sekas_macro::test]
    async fn pending_delete_hides_same_version_intent() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        commit_values(&engine, b"k", &[Value::with_value(b"intent".to_vec(), TXN_INTENT_VERSION)]);

        let overlay = PendingMutationOverlay::default();
        overlay.insert_batch(
            &[PendingMutation::delete(SHARD_ID, b"k".to_vec(), TXN_INTENT_VERSION)],
            CommitFence::none(),
        );
        let mut ctx = WriteEvalContext::new(&engine, overlay);

        assert!(ctx.latest_value(SHARD_ID, b"k").await.unwrap().is_none());
    }
}
