// Copyright 2023-present The Sekas Authors.
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

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::{
    ShardKey, ShardWriteRequest, TxnIntent, TxnState, Value, WriteRequest,
};

use crate::{Error, Result};

pub trait LatchGuard {
    /// Resolve the state of the specified txn record and release the lock
    /// guard. Return the value if the txn is committed, otherwise [`None`] is
    /// returned.
    async fn resolve_txn(&mut self, txn_intent: TxnIntent) -> Result<Option<Value>>;

    /// Signal all intent waiters.
    fn signal_all(&self, txn_state: TxnState, commit_version: Option<u64>);
}

pub trait LatchManager {
    type Guard: LatchGuard;

    /// Resolve the state of the specified txn record. Return the value if the
    /// txn is committed, otherwise [`None`] is returned.
    ///
    /// - `start_version` the version of the executing txn.
    /// - `intent_version` the version of txn to resolved.
    async fn resolve_txn(
        &self,
        shard_id: u64,
        user_key: &[u8],
        start_version: u64,
        intent_version: u64,
    ) -> Result<Option<Value>>;

    /// Acquire row latch for the specified user key.
    async fn acquire(&self, shard_id: u64, user_key: &[u8]) -> Result<Self::Guard>;
}

pub struct DeferSignalLatchGuard<L: LatchGuard> {
    state: Option<(TxnState, Option<u64>)>,
    latches: HashMap<ShardKey, L>,
}

impl<L: LatchGuard> DeferSignalLatchGuard<L> {
    #[cfg(test)]
    pub fn empty() -> Self {
        DeferSignalLatchGuard { state: None, latches: HashMap::default() }
    }

    #[cfg(test)]
    pub fn with_single(shard_key: &ShardKey, latch: L) -> Self {
        let mut latches = HashMap::new();
        latches.insert(shard_key.clone(), latch);
        DeferSignalLatchGuard { state: None, latches }
    }

    pub async fn resolve_txn(
        &mut self,
        shard_id: u64,
        user_key: &[u8],
        txn_intent: TxnIntent,
    ) -> Result<Option<Value>> {
        let shard_key = ShardKey { shard_id, user_key: user_key.to_vec() };
        let latch = self.latches.get_mut(&shard_key).ok_or_else(|| {
            Error::InvalidData(format!(
                "resolve txn but not hold the latch, start version {}",
                txn_intent.start_version
            ))
        })?;
        latch.resolve_txn(txn_intent).await
        // TODO(walter) release the other latches!
    }

    #[inline]
    pub fn signal_all(&mut self, txn_state: TxnState, commit_version: Option<u64>) {
        self.state = Some((txn_state, commit_version));
    }
}

impl<L: LatchGuard> Drop for DeferSignalLatchGuard<L> {
    fn drop(&mut self) {
        if let Some((txn_state, commit_version)) = self.state.take() {
            for latch in self.latches.values() {
                latch.signal_all(txn_state, commit_version);
            }
        }
    }
}

pub async fn acquire_row_latches<T>(
    latch_mgr: &T,
    request: &Request,
) -> Result<Option<DeferSignalLatchGuard<T::Guard>>>
where
    T: LatchManager,
{
    let (shard_id, mut keys) = match request {
        Request::Write(req) => (req.shard_id, collect_shard_write_keys(req)?),
        Request::WriteIntent(req) => {
            let Some(write) = req.write.as_ref() else {
                return Ok(None);
            };
            match write {
                WriteRequest::Put(put) => (req.shard_id, vec![put.key.clone()]),
                WriteRequest::Delete(delete) => (req.shard_id, vec![delete.key.clone()]),
            }
        }
        Request::CommitIntent(req) => (req.shard_id, vec![req.user_key.clone()]),
        Request::ClearIntent(req) => (req.shard_id, vec![req.user_key.clone()]),
        Request::Scan(_)
        | Request::Get(_)
        | Request::CreateShard(_)
        | Request::ChangeReplicas(_)
        | Request::AcceptShard(_)
        | Request::Transfer(_)
        | Request::MoveReplicas(_)
        | Request::WatchKey(_) => return Ok(None),
    };

    if keys.is_empty() {
        return Ok(None);
    }

    // ATTN: Sort shard keys before acquiring any latch, to avoid deadlock.
    keys.sort_unstable();

    let mut latches = HashMap::with_capacity(keys.len());
    for user_key in keys {
        let latch = latch_mgr.acquire(shard_id, &user_key).await?;
        latches.insert(ShardKey { shard_id, user_key }, latch);
    }
    Ok(Some(DeferSignalLatchGuard { state: None, latches }))
}

fn collect_shard_write_keys(req: &ShardWriteRequest) -> Result<Vec<Vec<u8>>> {
    let mut keys = Vec::with_capacity(req.puts.len() + req.deletes.len());
    for put in &req.puts {
        keys.push(put.key.clone());
    }
    for delete in &req.deletes {
        keys.push(delete.key.clone());
    }
    Ok(keys)
}

pub mod remote {
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::time::Duration;

    use dashmap::DashMap;
    use futures::channel::oneshot;
    use log::{debug, trace};
    use prost::Message;
    use sekas_api::server::v1::{ShardKey, TxnIntent, TxnState, Value};
    use sekas_client::TxnStateTable;
    use sekas_rock::time::timestamp_millis;
    use sekas_schema::system::txn::TXN_INTENT_VERSION;

    use super::LatchGuard;
    use crate::engine::{GroupEngine, SnapshotMode, WriteBatch};
    use crate::raftgroup::RaftGroup;
    use crate::replica::eval::LatchManager;
    use crate::serverpb::v1::EvalResult;
    use crate::{Error, Result};

    #[derive(Default)]
    struct LatchBlock {
        hold: bool,
        shard_key: ShardKey,
        latch_waiters: VecDeque<oneshot::Sender<RemoteLatchGuard>>,
        intent_waiters: VecDeque<oneshot::Sender<(TxnState, u64)>>,
    }

    pub struct RemoteLatchGuard {
        hold: bool,
        shard_key: ShardKey,
        latch_mgr: RemoteLatchManager,
    }

    #[derive(Clone)]
    pub struct RemoteLatchManager {
        core: Arc<LatchManagerCore>,
    }

    pub struct LatchManagerCore {
        txn_table: TxnStateTable,
        group_engine: GroupEngine,
        raft_group: RaftGroup,
        latches: DashMap<ShardKey, LatchBlock>,
    }

    impl RemoteLatchManager {
        pub fn new(
            client: sekas_client::SekasClient,
            group_engine: GroupEngine,
            raft_group: RaftGroup,
        ) -> Self {
            RemoteLatchManager {
                core: Arc::new(LatchManagerCore {
                    txn_table: TxnStateTable::new(client, Some(Duration::from_secs(5))),
                    group_engine,
                    raft_group,
                    latches: DashMap::with_shard_amount(16),
                }),
            }
        }

        pub fn release(&self, shard_id: u64, user_key: &[u8]) {
            let shard_key = ShardKey { shard_id, user_key: user_key.to_owned() };
            log::debug!("release shard {} user key {:?}", shard_id, user_key);

            self.core.latches.remove_if_mut(&shard_key, |shard_key, latch_block| {
                log::debug!(
                    "transfer latch guard, shard {} user key {:?}",
                    shard_key.shard_id,
                    shard_key.user_key
                );
                self.transfer_latch_guard(latch_block)
            });
        }

        fn acquire_internal(
            &self,
            shard_id: u64,
            key: &[u8],
        ) -> Result<RemoteLatchGuard, oneshot::Receiver<RemoteLatchGuard>> {
            let mut entry = self.core.get_latch_mut(shard_id, key);
            let latch = entry.value_mut();
            if !latch.hold {
                latch.hold = true;
                log::debug!("acquire shard {} user key {:?}", shard_id, key);
                Ok(RemoteLatchGuard {
                    hold: true,
                    shard_key: ShardKey { shard_id, user_key: key.to_owned() },
                    latch_mgr: self.clone(),
                })
            } else {
                log::debug!(
                    "acquire shard {} user key {:?}, latch is hold, wait it release",
                    shard_id,
                    key
                );
                let (tx, rx) = oneshot::channel();
                latch.latch_waiters.push_back(tx);
                Err(rx)
            }
        }

        fn transfer_latch_guard(&self, latch_block: &mut LatchBlock) -> bool {
            let mut guard = RemoteLatchGuard {
                hold: true,
                shard_key: latch_block.shard_key.clone(),
                latch_mgr: self.clone(),
            };
            while let Some(sender) = latch_block.latch_waiters.pop_front() {
                log::debug!(
                    "find a waiter, try wake up it, shard {} user key {:?}",
                    latch_block.shard_key.shard_id,
                    latch_block.shard_key.user_key
                );
                guard = match sender.send(guard) {
                    Ok(()) => {
                        // The guard will wakes up the remaining waiters even the receiver is
                        // canceled.
                        log::debug!(
                            "acquire shard {} user key {:?}",
                            latch_block.shard_key.shard_id,
                            latch_block.shard_key.user_key
                        );
                        latch_block.hold = true;
                        return false;
                    }
                    Err(guard) => guard,
                };
            }
            log::debug!(
                "no waiters, shard {} user key {:?}",
                latch_block.shard_key.shard_id,
                latch_block.shard_key.user_key
            );

            guard.hold = false;
            latch_block.hold = false;

            // No more waiters, remove entry from map.
            latch_block.intent_waiters.is_empty()
        }

        async fn commit_intent(
            &self,
            shard_key: &ShardKey,
            txn_intent: &TxnIntent,
            commit_version: u64,
        ) -> Result<()> {
            // FIXME(walter) What happen if the target shard already migrated?
            let mut wb = WriteBatch::default();
            self.core.group_engine.delete(
                &mut wb,
                shard_key.shard_id,
                &shard_key.user_key,
                TXN_INTENT_VERSION,
            )?;
            if txn_intent.is_delete {
                self.core.group_engine.tombstone(
                    &mut wb,
                    shard_key.shard_id,
                    &shard_key.user_key,
                    commit_version,
                )?;
            } else if let Some(value) = txn_intent.value.as_ref() {
                self.core.group_engine.put(
                    &mut wb,
                    shard_key.shard_id,
                    &shard_key.user_key,
                    value,
                    commit_version,
                )?;
            }
            self.core.raft_group.propose(EvalResult::with_batch(wb.data().to_vec())).await
        }

        async fn clear_intent(&self, shard_key: &ShardKey) -> Result<()> {
            let mut wb = WriteBatch::default();
            self.core.group_engine.delete(
                &mut wb,
                shard_key.shard_id,
                &shard_key.user_key,
                TXN_INTENT_VERSION,
            )?;
            self.core.raft_group.propose(EvalResult::with_batch(wb.data().to_owned())).await
        }
    }

    impl super::LatchManager for RemoteLatchManager {
        type Guard = RemoteLatchGuard;

        /// Resolve the state of the specified txn record. Return the value if
        /// the txn is committed, otherwise [`None`] is returned.
        async fn resolve_txn(
            &self,
            shard_id: u64,
            user_key: &[u8],
            start_version: u64,
            intent_version: u64,
        ) -> Result<Option<Value>> {
            trace!("txn {start_version} try resolve txn {intent_version}, shard {shard_id} user key {user_key:?}");
            let mut latch_guard = self.acquire(shard_id, user_key).await?;
            // read the txn intent again with latch guard.
            let snapshot_mode = SnapshotMode::Key { key: user_key };
            let mut snapshot = self.core.group_engine.snapshot(shard_id, snapshot_mode)?;
            let Some(mvcc_iter) = snapshot.next() else { return Ok(None) };
            for entry in mvcc_iter? {
                let entry = entry?;
                if entry.version() == TXN_INTENT_VERSION {
                    let content = entry.value().ok_or_else(|| {
                        Error::InvalidData(format!(
                            "txn intent value is not exists, shard_id {shard_id} key {user_key:?}"
                        ))
                    })?;
                    let txn_intent = TxnIntent::decode(content)?;
                    if txn_intent.start_version == intent_version {
                        return latch_guard.resolve_txn(txn_intent).await;
                    }
                    // no such intent exists, just read the recent value.
                } else if entry.version() <= start_version {
                    return Ok(Some(entry.into()));
                }
            }
            Ok(None)
        }

        async fn acquire(&self, shard_id: u64, key: &[u8]) -> Result<RemoteLatchGuard> {
            match self.acquire_internal(shard_id, key) {
                Ok(latch) => Ok(latch),
                Err(rx) => Ok(rx.await.expect("Will not be dropped without send()")),
            }
        }
    }

    impl super::LatchGuard for RemoteLatchGuard {
        async fn resolve_txn(&mut self, txn_intent: TxnIntent) -> Result<Option<Value>> {
            let start_version = txn_intent.start_version;
            trace!("try resolve txn {start_version}, shard key {:?}", self.shard_key);
            loop {
                let txn_record =
                    self.latch_mgr.core.txn_table.get_txn_record(start_version).await?.ok_or_else(
                        || {
                            Error::InvalidData(format!(
                                "resolve txn {}, but txn record is not exists",
                                start_version
                            ))
                        },
                    )?;

                let mut delete_intent = false;
                let (actual_txn_state, commit_version) = if txn_record.state == TxnState::Running {
                    if txn_record.heartbeat + 500 < timestamp_millis() {
                        debug!("abort txn {} because it was expired", start_version);
                        match self.latch_mgr.core.txn_table.abort_txn(start_version).await {
                            Ok(()) => {
                                delete_intent = true;
                                (TxnState::Aborted, 0)
                            }
                            Err(sekas_client::Error::InvalidArgument(_)) => {
                                continue;
                            }
                            Err(err) => return Err(err.into()),
                        }
                    } else {
                        debug!("wait txn {} intent to commit or abort", start_version);
                        let (sender, receiver) = oneshot::channel();
                        {
                            let mut entry = self
                                .latch_mgr
                                .core
                                .get_latch_mut(self.shard_key.shard_id, &self.shard_key.user_key);
                            entry.intent_waiters.push_back(sender);
                            #[allow(clippy::explicit_auto_deref)]
                            self.latch_mgr.transfer_latch_guard(&mut *entry);
                        }
                        debug_assert!(self.hold, "resolve txn should hold the lock");
                        self.hold = false;
                        let (txn_state, commit_version) = receiver.await.expect("Do not cancel");
                        *self = self
                            .latch_mgr
                            .acquire(self.shard_key.shard_id, &self.shard_key.user_key)
                            .await?;
                        (txn_state, commit_version)
                    }
                } else {
                    delete_intent = true;
                    (txn_record.state, txn_record.commit_version.unwrap_or_default())
                };

                debug!("txn {} intent state {}, commit version {commit_version} delete intent {delete_intent}", start_version,
                    actual_txn_state.as_str_name());
                match actual_txn_state {
                    TxnState::Committed => {
                        if delete_intent {
                            self.latch_mgr
                                .commit_intent(&self.shard_key, &txn_intent, commit_version)
                                .await?;
                        }
                        if txn_intent.is_delete {
                            return Ok(Some(Value::tombstone(commit_version)));
                        } else {
                            return Ok(Some(Value {
                                content: txn_intent.value,
                                version: commit_version,
                            }));
                        }
                    }
                    TxnState::Aborted => {
                        if delete_intent {
                            self.latch_mgr.clear_intent(&self.shard_key).await?;
                        }
                        return Ok(None);
                    }
                    TxnState::Running => {
                        unreachable!("the txn state should be resolved")
                    }
                }
            }
        }

        fn signal_all(&self, txn_state: TxnState, commit_version: Option<u64>) {
            // FIXME(walter) what happen if the signal intent is not equals to wait intent.
            let commit_version = commit_version.unwrap_or_default();
            if let Some(mut latch_block) =
                self.latch_mgr.core.latches.get_mut(&self.shard_key.clone())
            {
                for sender in std::mem::take(&mut latch_block.intent_waiters) {
                    let _ = sender.send((txn_state, commit_version));
                }
            }
        }
    }

    impl Drop for RemoteLatchGuard {
        fn drop(&mut self) {
            if self.hold {
                self.latch_mgr.release(self.shard_key.shard_id, &self.shard_key.user_key);
            }
        }
    }

    impl LatchManagerCore {
        fn get_latch_mut(
            &self,
            shard_id: u64,
            user_key: &[u8],
        ) -> dashmap::mapref::one::RefMut<'_, ShardKey, LatchBlock> {
            let default_latch_block = || LatchBlock {
                shard_key: ShardKey { shard_id, user_key: user_key.to_owned() },
                ..Default::default()
            };

            let shard_key = ShardKey { shard_id, user_key: user_key.to_owned() };
            self.latches.entry(shard_key).or_insert_with(default_latch_block)
        }
    }

    #[cfg(test)]
    mod tests {
        use futures::channel::mpsc;
        use sekas_client::{ClientOptions, SekasClient};
        use sekas_rock::fn_name;
        use tempdir::TempDir;

        use super::*;
        use crate::engine::create_group_engine;

        #[sekas_macro::test]
        async fn acquire_and_release_latches() {
            let dir = TempDir::new(fn_name!()).unwrap();
            let client =
                SekasClient::new(ClientOptions::default(), vec!["127.0.0.1:5000".to_string()])
                    .await
                    .unwrap();
            let engine = create_group_engine(dir.path(), 1, 1, 1).await;
            let (sender, _receiver) = mpsc::channel(1024);
            let raft_group = RaftGroup::open(sender);
            let latch_mgr = RemoteLatchManager::new(client, engine, raft_group);

            let shard_id = 1;
            let user_key = vec![1u8, 2u8];

            // case 1: acquire and release normally.
            {
                let _latch_guard = latch_mgr.acquire(shard_id, &user_key).await.unwrap();
            }
            {
                let _latch_guard = latch_mgr.acquire(shard_id, &user_key).await.unwrap();
            }

            // case 2: transfer latch guard.
            let acquire_1 = latch_mgr.acquire(shard_id, &user_key).await.unwrap();
            let user_key_clone = user_key.clone();
            let latch_mgr_clone = latch_mgr.clone();
            let handle = sekas_runtime::spawn(async move {
                latch_mgr_clone.acquire(shard_id, &user_key_clone).await
            });
            sekas_runtime::time::sleep(Duration::from_secs(1)).await;
            drop(acquire_1);
            handle.await.unwrap().unwrap();

            // case 3: transfer latch should release latches.
            let _acquire_3 = latch_mgr.acquire(shard_id, &user_key).await.unwrap();
        }
    }
}

#[cfg(test)]
pub mod local {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use futures::channel::oneshot;
    use sekas_api::server::v1::{ShardKey, TxnIntent, TxnState, Value};

    use crate::replica::eval::LatchManager;

    #[derive(Default)]
    struct LatchBlock {
        hold: bool,
        shard_key: ShardKey,
        latch_waiters: VecDeque<oneshot::Sender<LocalLatchGuard>>,
        intent_waiters: VecDeque<oneshot::Sender<(TxnState, u64)>>,
    }

    /// A local latch manager.
    #[derive(Default, Clone)]
    pub struct LocalLatchManager {
        #[allow(clippy::type_complexity)]
        latches: Arc<Mutex<HashMap<ShardKey, LatchBlock>>>,
    }

    /// A guard of local latch.
    pub struct LocalLatchGuard {
        hold: bool,
        shard_key: ShardKey,
        latch_mgr: LocalLatchManager,
    }

    impl LocalLatchManager {
        fn release(&self, shard_key: &ShardKey) {
            let mut latches = self.latches.lock().unwrap();
            if let Some(latch_block) = latches.get_mut(shard_key) {
                if self.transfer_latch_guard(latch_block) {
                    latches.remove(shard_key);
                }
            }
        }

        fn transfer_latch_guard(&self, latch_block: &mut LatchBlock) -> bool {
            let mut guard = LocalLatchGuard {
                hold: true,
                shard_key: latch_block.shard_key.clone(),
                latch_mgr: self.clone(),
            };
            while let Some(sender) = latch_block.latch_waiters.pop_front() {
                guard = match sender.send(guard) {
                    Ok(()) => {
                        // The guard will wakes up the remaining waiters even the receiver is
                        // canceled.
                        latch_block.hold = true;
                        return false;
                    }
                    Err(guard) => guard,
                };
            }

            guard.hold = false;
            latch_block.hold = false;

            // No more waiters, remove entry from map.
            latch_block.intent_waiters.is_empty()
        }
    }

    impl super::LatchManager for LocalLatchManager {
        type Guard = LocalLatchGuard;

        async fn resolve_txn(
            &self,
            _shard_id: u64,
            _user_key: &[u8],
            _start_version: u64,
            _intent_version: u64,
        ) -> crate::Result<Option<Value>> {
            todo!()
        }

        async fn acquire(&self, shard_id: u64, user_key: &[u8]) -> crate::Result<Self::Guard> {
            let shard_key = ShardKey { shard_id, user_key: user_key.to_owned() };
            let receiver = {
                let mut latches = self.latches.lock().unwrap();
                let latch_block = latches.entry(shard_key.clone()).or_insert_with(|| LatchBlock {
                    shard_key: shard_key.clone(),
                    ..Default::default()
                });
                if !latch_block.hold {
                    latch_block.hold = true;
                    return Ok(LocalLatchGuard { hold: true, shard_key, latch_mgr: self.clone() });
                }
                let (sender, receiver) = oneshot::channel();
                latch_block.latch_waiters.push_back(sender);
                receiver
            };
            Ok(receiver.await.unwrap())
        }
    }

    impl super::LatchGuard for LocalLatchGuard {
        async fn resolve_txn(&mut self, txn_intent: TxnIntent) -> crate::Result<Option<Value>> {
            let (sender, receiver) = oneshot::channel();
            {
                let mut latches = self.latch_mgr.latches.lock().unwrap();
                let latch_block = latches.entry(self.shard_key.clone()).or_insert_with(|| {
                    LatchBlock { shard_key: self.shard_key.clone(), ..Default::default() }
                });
                latch_block.intent_waiters.push_back(sender);
                self.latch_mgr.transfer_latch_guard(latch_block);
            }

            let (txn_state, commit_version) = receiver.await.unwrap();
            *self =
                self.latch_mgr.acquire(self.shard_key.shard_id, &self.shard_key.user_key).await?;
            match txn_state {
                TxnState::Aborted => Ok(None),
                TxnState::Committed => {
                    if txn_intent.is_delete {
                        Ok(Some(Value::tombstone(commit_version)))
                    } else {
                        Ok(Some(Value { content: txn_intent.value, version: commit_version }))
                    }
                }
                _ => unreachable!(),
            }
        }

        fn signal_all(&self, txn_state: TxnState, commit_version: Option<u64>) {
            let mut latches = self.latch_mgr.latches.lock().unwrap();
            if let Some(latch_block) = latches.get_mut(&self.shard_key) {
                while let Some(sender) = latch_block.intent_waiters.pop_front() {
                    let _ = sender.send((txn_state, commit_version.unwrap_or_default()));
                }
            }
        }
    }

    impl Drop for LocalLatchGuard {
        fn drop(&mut self) {
            if self.hold {
                self.latch_mgr.release(&self.shard_key);
            }
        }
    }
}
