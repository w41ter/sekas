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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use sekas_api::server::v1::{ShardKey, Value};
use tokio::sync::Notify;

use super::local_txn::PendingLocalTxnGuard;
use crate::raftgroup::{ProposalReceiver, RaftGroup};
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub(super) struct PendingWrite {
    pub shard_key: ShardKey,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub(super) struct PendingValue {
    pub value: Value,
    pub fence: CommitFence,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CommitFence {
    watchers: Vec<ProposalWatcher>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProposalState {
    Applied,
    NotLeader,
}

#[derive(Clone, Debug)]
pub(super) struct ProposalWatcher {
    group_id: u64,
    core: Arc<ProposalWatcherCore>,
}

#[derive(Debug)]
struct ProposalWatcherCore {
    state: Mutex<Option<ProposalState>>,
    notify: Notify,
}

#[derive(Clone, Default)]
pub(super) struct PendingWriteOverlay {
    inner: Arc<Mutex<PendingWriteOverlayInner>>,
}

#[derive(Default)]
struct PendingWriteOverlayInner {
    // The outer map is keyed by shard/user key. The inner map is keyed by MVCC
    // version, which keeps intent (u64::MAX) and newer versions last.
    entries: HashMap<ShardKey, BTreeMap<u64, PendingEntry>>,
}

#[derive(Clone)]
struct PendingEntry {
    value: Value,
    fence: CommitFence,
}

impl PendingWrite {
    pub fn new(shard_id: u64, user_key: Vec<u8>, value: Value) -> Self {
        PendingWrite { shard_key: ShardKey { shard_id, user_key }, value }
    }
}

impl CommitFence {
    pub fn none() -> Self {
        CommitFence { watchers: Vec::new() }
    }

    pub fn from_watcher(watcher: ProposalWatcher) -> Self {
        CommitFence { watchers: vec![watcher] }
    }

    pub fn join(&mut self, other: CommitFence) {
        self.watchers.extend(other.watchers);
    }

    pub fn is_empty(&self) -> bool {
        self.watchers.is_empty()
    }

    pub async fn wait(&self) -> Result<()> {
        for watcher in &self.watchers {
            watcher.wait().await?;
        }
        Ok(())
    }
}

impl ProposalWatcher {
    pub fn new(group_id: u64) -> Self {
        ProposalWatcher {
            group_id,
            core: Arc::new(ProposalWatcherCore { state: Mutex::new(None), notify: Notify::new() }),
        }
    }

    pub fn drive(
        &self,
        dependencies: CommitFence,
        proposal_start_at: Instant,
        proposal: ProposalReceiver,
        overlay: PendingWriteOverlay,
        pending_writes: Vec<PendingWrite>,
        pending_local_txn: Option<PendingLocalTxnGuard>,
    ) {
        let watcher = self.clone();
        tokio::spawn(async move {
            let deps_applied = dependencies.wait().await.is_ok();
            let proposal_applied =
                matches!(RaftGroup::wait_proposal(proposal_start_at, proposal).await, Ok(()));
            let state = if deps_applied && proposal_applied {
                ProposalState::Applied
            } else {
                ProposalState::NotLeader
            };
            overlay.remove_batch(&pending_writes);
            if let Some(pending_local_txn) = pending_local_txn {
                match state {
                    ProposalState::Applied => pending_local_txn.finish().await,
                    ProposalState::NotLeader => pending_local_txn.abort().await,
                }
            }
            watcher.complete(state);
        });
    }

    pub async fn wait(&self) -> Result<()> {
        loop {
            if let Some(state) = *self.core.state.lock().unwrap() {
                return match state {
                    ProposalState::Applied => Ok(()),
                    ProposalState::NotLeader => Err(Error::NotLeader(self.group_id, 0, None)),
                };
            }
            self.core.notify.notified().await;
        }
    }

    pub fn complete(&self, state: ProposalState) {
        *self.core.state.lock().unwrap() = Some(state);
        self.core.notify.notify_waiters();
    }
}

impl PendingWriteOverlay {
    pub fn latest(&self, shard_id: u64, user_key: &[u8]) -> Option<PendingValue> {
        let shard_key = ShardKey { shard_id, user_key: user_key.to_vec() };
        let inner = self.inner.lock().unwrap();
        let entry = inner.entries.get(&shard_key)?.iter().next_back().map(|(_, v)| v)?;
        Some(PendingValue { value: entry.value.clone(), fence: entry.fence.clone() })
    }

    pub fn insert_batch(&self, writes: &[PendingWrite], fence: CommitFence) {
        let mut inner = self.inner.lock().unwrap();
        for write in writes {
            inner.entries.entry(write.shard_key.clone()).or_default().insert(
                write.value.version,
                PendingEntry { value: write.value.clone(), fence: fence.clone() },
            );
        }
    }

    pub fn remove_batch(&self, writes: &[PendingWrite]) {
        let mut inner = self.inner.lock().unwrap();
        for write in writes {
            let remove_key = if let Some(versions) = inner.entries.get_mut(&write.shard_key) {
                versions.remove(&write.value.version);
                versions.is_empty()
            } else {
                false
            };
            if remove_key {
                inner.entries.remove(&write.shard_key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sekas_macro::test]
    async fn commit_fence_waits_for_watcher() {
        let watcher = ProposalWatcher::new(1);
        let fence = CommitFence::from_watcher(watcher.clone());
        watcher.complete(ProposalState::Applied);
        fence.wait().await.unwrap();
    }

    #[test]
    fn overlay_returns_latest_version() {
        let overlay = PendingWriteOverlay::default();
        let first = PendingWrite::new(1, b"k".to_vec(), Value::with_value(b"v1".to_vec(), 10));
        let second = PendingWrite::new(1, b"k".to_vec(), Value::with_value(b"v2".to_vec(), 20));
        overlay.insert_batch(&[first], CommitFence::none());
        overlay.insert_batch(&[second.clone()], CommitFence::none());

        let latest = overlay.latest(1, b"k").unwrap();
        assert_eq!(latest.value.version, 20);
        assert_eq!(latest.value.content.as_deref(), Some(&b"v2"[..]));

        overlay.remove_batch(&[second]);
        let latest = overlay.latest(1, b"k").unwrap();
        assert_eq!(latest.value.version, 10);
    }

    #[test]
    fn overlay_removes_empty_key() {
        let overlay = PendingWriteOverlay::default();
        let write = PendingWrite::new(1, b"k".to_vec(), Value::with_value(b"v".to_vec(), 10));
        overlay.insert_batch(std::slice::from_ref(&write), CommitFence::none());
        overlay.remove_batch(&[write]);
        assert!(overlay.latest(1, b"k").is_none());
    }

    #[test]
    fn proposal_watcher_is_multi_waiter() {
        let watcher = ProposalWatcher::new(1);
        let _left = watcher.clone();
        let _right = watcher.clone();
        watcher.complete(ProposalState::Applied);
    }

    #[sekas_macro::test]
    async fn watcher_driver_removes_overlay_after_proposal() {
        let overlay = PendingWriteOverlay::default();
        let write = PendingWrite::new(1, b"k".to_vec(), Value::with_value(b"v".to_vec(), 10));
        let watcher = ProposalWatcher::new(1);
        let fence = CommitFence::from_watcher(watcher.clone());
        overlay.insert_batch(std::slice::from_ref(&write), fence.clone());

        let (sender, receiver) = futures::channel::oneshot::channel();
        watcher.drive(
            CommitFence::none(),
            Instant::now(),
            receiver,
            overlay.clone(),
            vec![write],
            None,
        );
        sender.send(Ok(())).unwrap();

        fence.wait().await.unwrap();
        assert!(overlay.latest(1, b"k").is_none());
    }
}
