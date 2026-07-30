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

use std::sync::Arc;

use sekas_schema::system::txn::TXN_MAX_VERSION;
use tokio::sync::{Mutex, Notify};

#[derive(Clone, Default)]
pub struct LocalTxnManager {
    core: Arc<LocalTxnManagerCore>,
}

#[derive(Default)]
struct LocalTxnManagerCore {
    inner: Mutex<LocalTxnState>,
    notify: Notify,
}

#[derive(Default)]
struct LocalTxnState {
    next_pending_id: u64,
    max_served_read_version: u64,
    last_assigned_commit_version: u64,
    pending_commits: Vec<PendingLocalTxn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingLocalTxn {
    id: PendingLocalTxnId,
    commit_version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PendingLocalTxnId(u64);

pub struct PendingLocalTxnGuard {
    manager: LocalTxnManager,
    id: PendingLocalTxnId,
    commit_version: u64,
    finished: bool,
}

impl LocalTxnManager {
    pub async fn before_read(&self, read_version: u64) {
        if read_version == TXN_MAX_VERSION {
            return;
        }

        loop {
            let notified = {
                let mut state = self.core.inner.lock().await;
                state.max_served_read_version = state.max_served_read_version.max(read_version);
                if !state.has_pending_at_or_before(read_version) {
                    return;
                }
                self.core.notify.notified()
            };
            notified.await;
        }
    }

    pub async fn begin_commit(&self, candidate_commit_version: u64) -> PendingLocalTxnGuard {
        let mut state = self.core.inner.lock().await;
        let id = PendingLocalTxnId(state.next_pending_id);
        state.next_pending_id += 1;
        let commit_version = candidate_commit_version
            .max(state.max_served_read_version.saturating_add(1))
            .max(state.last_assigned_commit_version.saturating_add(1));
        state.last_assigned_commit_version = commit_version;
        state.pending_commits.push(PendingLocalTxn { id, commit_version });

        PendingLocalTxnGuard { manager: self.clone(), id, commit_version, finished: false }
    }

    async fn finish_pending(&self, id: PendingLocalTxnId) {
        let mut state = self.core.inner.lock().await;
        state.remove_pending(id);
        drop(state);
        self.core.notify.notify_waiters();
    }

    #[cfg(test)]
    pub async fn pending_count(&self) -> usize {
        self.core.inner.lock().await.pending_commits.len()
    }
}

impl LocalTxnState {
    fn has_pending_at_or_before(&self, read_version: u64) -> bool {
        self.pending_commits.iter().any(|pending| pending.commit_version <= read_version)
    }

    fn remove_pending(&mut self, id: PendingLocalTxnId) {
        self.pending_commits.retain(|pending| pending.id != id);
    }
}

impl PendingLocalTxnGuard {
    pub fn commit_version(&self) -> u64 {
        self.commit_version
    }

    pub async fn finish(mut self) {
        self.finished = true;
        self.manager.finish_pending(self.id).await;
    }

    pub async fn abort(mut self) {
        self.finished = true;
        self.manager.finish_pending(self.id).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[sekas_macro::test]
    async fn begin_commit_uses_candidate_without_served_reads() {
        let manager = LocalTxnManager::default();
        let pending = manager.begin_commit(10).await;
        assert_eq!(pending.commit_version(), 10);
        pending.finish().await;
    }

    #[sekas_macro::test]
    async fn begin_commit_raises_candidate_above_served_read_version() {
        let manager = LocalTxnManager::default();
        manager.before_read(100).await;

        let pending = manager.begin_commit(50).await;
        assert_eq!(pending.commit_version(), 101);
        pending.finish().await;
    }

    #[sekas_macro::test]
    async fn begin_commit_keeps_group_local_versions_monotonic() {
        let manager = LocalTxnManager::default();
        let first = manager.begin_commit(100).await;
        assert_eq!(first.commit_version(), 100);
        first.finish().await;

        let second = manager.begin_commit(50).await;
        assert_eq!(second.commit_version(), 101);
        second.finish().await;
    }

    #[sekas_macro::test]
    async fn read_waits_for_pending_commit_at_covered_version() {
        let manager = LocalTxnManager::default();
        let pending = manager.begin_commit(100).await;

        let cloned = manager.clone();
        let reader = tokio::spawn(async move {
            cloned.before_read(100).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!reader.is_finished());

        pending.finish().await;
        reader.await.unwrap();
    }

    #[sekas_macro::test]
    async fn read_ignores_pending_commit_above_read_version() {
        let manager = LocalTxnManager::default();
        let pending = manager.begin_commit(100).await;
        manager.before_read(99).await;
        pending.finish().await;
    }

    #[sekas_macro::test]
    async fn read_without_version_does_not_advance_served_read_version() {
        let manager = LocalTxnManager::default();
        manager.before_read(TXN_MAX_VERSION).await;

        let pending = manager.begin_commit(50).await;
        assert_eq!(pending.commit_version(), 50);
        pending.finish().await;
    }

    #[sekas_macro::test]
    async fn abort_removes_pending_commit() {
        let manager = LocalTxnManager::default();
        let pending = manager.begin_commit(100).await;
        assert_eq!(manager.pending_count().await, 1);
        pending.abort().await;
        assert_eq!(manager.pending_count().await, 0);
    }
}
