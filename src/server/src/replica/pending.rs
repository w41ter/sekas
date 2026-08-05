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

use sekas_api::server::v1::{ReplicaDesc, ShardKey};
use tokio::sync::Notify;

use crate::error::BusyReason;
use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct PendingMutation {
    pub shard_key: ShardKey,
    pub version: u64,
    pub kind: PendingMutationKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PendingMutationKind {
    Put(Vec<u8>),
    Tombstone,
    Delete,
}

#[derive(Clone, Debug)]
pub(super) struct PendingMutationEntry {
    pub mutation: PendingMutation,
    pub fence: CommitFence,
}

#[derive(Clone, Debug, Default)]
pub(super) struct CommitFence {
    watchers: Vec<ProposalWatcher>,
}

#[derive(Clone, Debug)]
pub(super) enum ProposalOutcome {
    Applied,
    NotLeader(u64, u64, Option<ReplicaDesc>),
    GroupNotReady(u64),
    ServiceIsBusy(BusyReason),
    Canceled,
    Failed(Arc<Error>),
}

#[derive(Clone, Debug)]
pub(super) struct ProposalWatcher {
    core: Arc<ProposalWatcherCore>,
}

#[derive(Debug)]
struct ProposalWatcherCore {
    outcome: Mutex<Option<ProposalOutcome>>,
    notify: Notify,
}

#[derive(Clone, Default)]
pub(super) struct PendingMutationOverlay {
    inner: Arc<Mutex<PendingMutationOverlayInner>>,
}

#[derive(Default)]
struct PendingMutationOverlayInner {
    // The outer map is keyed by shard/user key. The inner map is keyed by MVCC
    // version, which keeps intent (u64::MAX) and newer versions last. The
    // value stack preserves proposal order for multiple pending mutations at
    // the same MVCC version.
    entries: HashMap<ShardKey, BTreeMap<u64, Vec<PendingEntry>>>,
}

#[derive(Clone)]
struct PendingEntry {
    mutation: PendingMutation,
    fence: CommitFence,
}

impl PendingMutation {
    pub fn put(shard_id: u64, user_key: Vec<u8>, value: Vec<u8>, version: u64) -> Self {
        PendingMutation {
            shard_key: ShardKey { shard_id, user_key },
            version,
            kind: PendingMutationKind::Put(value),
        }
    }

    pub fn tombstone(shard_id: u64, user_key: Vec<u8>, version: u64) -> Self {
        PendingMutation {
            shard_key: ShardKey { shard_id, user_key },
            version,
            kind: PendingMutationKind::Tombstone,
        }
    }

    pub fn delete(shard_id: u64, user_key: Vec<u8>, version: u64) -> Self {
        PendingMutation {
            shard_key: ShardKey { shard_id, user_key },
            version,
            kind: PendingMutationKind::Delete,
        }
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
    pub fn new() -> Self {
        ProposalWatcher {
            core: Arc::new(ProposalWatcherCore {
                outcome: Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    pub async fn wait(&self) -> Result<()> {
        loop {
            let notified = self.core.notify.notified();
            if let Some(outcome) = self.core.outcome.lock().unwrap().clone() {
                return outcome.into_result();
            }
            notified.await;
        }
    }

    pub fn complete(&self, outcome: ProposalOutcome) {
        *self.core.outcome.lock().unwrap() = Some(outcome);
        self.core.notify.notify_waiters();
    }

    pub fn complete_result(&self, result: Result<()>) -> Result<()> {
        let outcome = ProposalOutcome::from_result(result);
        self.complete(outcome.clone());
        outcome.into_result()
    }
}

impl ProposalOutcome {
    fn from_result(result: Result<()>) -> Self {
        match result {
            Ok(()) => ProposalOutcome::Applied,
            Err(err) => ProposalOutcome::from_error(err),
        }
    }

    fn from_error(err: Error) -> Self {
        match err {
            Error::NotLeader(group_id, term, leader) => {
                ProposalOutcome::NotLeader(group_id, term, leader.clone())
            }
            Error::GroupNotReady(group_id) => ProposalOutcome::GroupNotReady(group_id),
            Error::ServiceIsBusy(reason) => ProposalOutcome::ServiceIsBusy(reason),
            Error::Canceled => ProposalOutcome::Canceled,
            err => ProposalOutcome::Failed(Arc::new(err)),
        }
    }

    fn into_result(self) -> Result<()> {
        match self {
            ProposalOutcome::Applied => Ok(()),
            ProposalOutcome::NotLeader(group_id, term, leader) => {
                Err(Error::NotLeader(group_id, term, leader))
            }
            ProposalOutcome::GroupNotReady(group_id) => Err(Error::GroupNotReady(group_id)),
            ProposalOutcome::ServiceIsBusy(reason) => Err(Error::ServiceIsBusy(reason)),
            ProposalOutcome::Canceled => Err(Error::Canceled),
            ProposalOutcome::Failed(err) => Err(Error::Shared(err)),
        }
    }
}

impl PendingMutationOverlay {
    pub fn entries(&self, shard_id: u64, user_key: &[u8]) -> Vec<PendingMutationEntry> {
        let shard_key = ShardKey { shard_id, user_key: user_key.to_vec() };
        let inner = self.inner.lock().unwrap();
        inner
            .entries
            .get(&shard_key)
            .map(|versions| {
                versions
                    .values()
                    .flat_map(|entries| {
                        entries.iter().map(|entry| PendingMutationEntry {
                            mutation: entry.mutation.clone(),
                            fence: entry.fence.clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn insert_batch(&self, mutations: &[PendingMutation], fence: CommitFence) {
        if mutations.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().unwrap();
        for mutation in mutations {
            inner
                .entries
                .entry(mutation.shard_key.clone())
                .or_default()
                .entry(mutation.version)
                .or_default()
                .push(PendingEntry { mutation: mutation.clone(), fence: fence.clone() });
        }
    }

    pub fn remove_batch(&self, mutations: &[PendingMutation]) {
        if mutations.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().unwrap();
        for mutation in mutations {
            let remove_key = if let Some(versions) = inner.entries.get_mut(&mutation.shard_key) {
                if let Some(entries) = versions.get_mut(&mutation.version) {
                    if let Some(pos) = entries.iter().position(|entry| entry.mutation == *mutation)
                    {
                        entries.remove(pos);
                    }
                    if entries.is_empty() {
                        versions.remove(&mutation.version);
                    }
                }
                versions.is_empty()
            } else {
                false
            };
            if remove_key {
                inner.entries.remove(&mutation.shard_key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sekas_macro::test]
    async fn commit_fence_waits_for_watcher() {
        let watcher = ProposalWatcher::new();
        let fence = CommitFence::from_watcher(watcher.clone());
        watcher.complete(ProposalOutcome::Applied);
        fence.wait().await.unwrap();
    }

    #[test]
    fn overlay_returns_entries_in_version_order() {
        let overlay = PendingMutationOverlay::default();
        let first = PendingMutation::put(1, b"k".to_vec(), b"v1".to_vec(), 10);
        let second = PendingMutation::put(1, b"k".to_vec(), b"v2".to_vec(), 20);
        overlay.insert_batch(&[first], CommitFence::none());
        overlay.insert_batch(&[second.clone()], CommitFence::none());

        let entries = overlay.entries(1, b"k");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].mutation.version, 10);
        assert_eq!(entries[1].mutation.version, 20);

        overlay.remove_batch(&[second]);
        let entries = overlay.entries(1, b"k");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].mutation.version, 10);
    }

    #[test]
    fn overlay_removes_empty_key() {
        let overlay = PendingMutationOverlay::default();
        let mutation = PendingMutation::put(1, b"k".to_vec(), b"v".to_vec(), 10);
        overlay.insert_batch(std::slice::from_ref(&mutation), CommitFence::none());
        overlay.remove_batch(&[mutation]);
        assert!(overlay.entries(1, b"k").is_empty());
    }

    #[test]
    fn proposal_watcher_is_multi_waiter() {
        let watcher = ProposalWatcher::new();
        let _left = watcher.clone();
        let _right = watcher.clone();
        watcher.complete(ProposalOutcome::Applied);
    }

    #[sekas_macro::test]
    async fn commit_fence_preserves_typed_failure() {
        let watcher = ProposalWatcher::new();
        let fence = CommitFence::from_watcher(watcher.clone());
        watcher.complete_result(Err(Error::GroupNotReady(7))).unwrap_err();

        assert!(matches!(fence.wait().await, Err(Error::GroupNotReady(7))));
    }

    #[sekas_macro::test]
    async fn commit_fence_preserves_shared_failure_detail() {
        let watcher = ProposalWatcher::new();
        let fence = CommitFence::from_watcher(watcher.clone());
        watcher.complete_result(Err(Error::InvalidData("proposal failed".into()))).unwrap_err();

        let err = fence.wait().await.unwrap_err();
        assert_eq!(err.to_string(), "invalid proposal failed data");
    }

    #[test]
    fn overlay_remove_batch_clears_writes() {
        let overlay = PendingMutationOverlay::default();
        let mutation = PendingMutation::put(1, b"k".to_vec(), b"v".to_vec(), 10);
        let watcher = ProposalWatcher::new();
        let fence = CommitFence::from_watcher(watcher.clone());
        overlay.insert_batch(std::slice::from_ref(&mutation), fence.clone());

        overlay.remove_batch(&[mutation]);
        assert!(overlay.entries(1, b"k").is_empty());
    }

    #[test]
    fn overlay_preserves_same_version_order() {
        let overlay = PendingMutationOverlay::default();
        let put = PendingMutation::put(1, b"k".to_vec(), b"v".to_vec(), 10);
        let delete = PendingMutation::delete(1, b"k".to_vec(), 10);
        overlay.insert_batch(&[put.clone(), delete.clone()], CommitFence::none());

        let entries = overlay.entries(1, b"k");
        assert_eq!(entries[0].mutation, put);
        assert_eq!(entries[1].mutation, delete);
    }
}
