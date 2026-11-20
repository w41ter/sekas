// Copyright 2026 The Sekas Authors.
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

use std::sync::LazyLock;

use prometheus::{
    Histogram, HistogramTimer, HistogramVec, exponential_buckets, register_histogram_vec,
};
use sekas_api::server::v1::group_request_union::Request;

static LOCK_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "replica_group_lock_duration_seconds",
        "Time waiting for or holding metadata locks and row latches",
        &["lock", "phase"],
        exponential_buckets(0.000001, 2.0, 28).unwrap(),
    )
    .unwrap()
});

static WRITE_STAGE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec!(
        "replica_write_stage_duration_seconds",
        "Time spent in the stages of a replica write request",
        &["type", "stage"],
        exponential_buckets(0.00005, 1.8, 26).unwrap(),
    )
    .unwrap()
});

static METADATA_READ: LazyLock<LockMetrics> = LazyLock::new(|| LockMetrics::new("metadata_read"));
static METADATA_WRITE: LazyLock<LockMetrics> = LazyLock::new(|| LockMetrics::new("metadata_write"));
pub(super) static ROW_LATCH: LazyLock<LockMetrics> =
    LazyLock::new(|| LockMetrics::new("row_latch"));

pub(super) fn metadata_lock(exclusive: bool) -> &'static LockMetrics {
    if exclusive { &METADATA_WRITE } else { &METADATA_READ }
}

pub(super) struct LockMetrics {
    pub(super) wait: Histogram,
    pub(super) hold: Histogram,
}

impl LockMetrics {
    fn new(lock: &str) -> Self {
        Self {
            wait: LOCK_DURATION.with_label_values(&[lock, "wait"]),
            hold: LOCK_DURATION.with_label_values(&[lock, "hold"]),
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct WriteMetrics {
    request_type: Option<&'static str>,
}

pub(crate) enum WriteStage {
    Request,
    Evaluate,
    MvccRead,
    PendingIntent,
    ResolveIntent,
    PrepareProposal,
    RaftResult,
    CommitFence,
    OverlayCleanup,
    FinalizeLocalTxn,
    Forward,
}

impl WriteStage {
    fn label(&self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Evaluate => "evaluate",
            Self::MvccRead => "mvcc_read",
            Self::PendingIntent => "pending_intent",
            Self::ResolveIntent => "resolve_intent",
            Self::PrepareProposal => "proposal_prepare",
            Self::RaftResult => "raft_result",
            Self::CommitFence => "commit_fence",
            Self::OverlayCleanup => "overlay_cleanup",
            Self::FinalizeLocalTxn => "local_txn_finalize",
            Self::Forward => "forward",
        }
    }
}

impl WriteMetrics {
    pub(crate) fn for_request(request: &Request) -> Self {
        let request_type = match request {
            Request::Write(_) => "write",
            Request::LocalTxnWrite(_) => "local_txn_write",
            Request::WriteIntent(_) => "write_intent",
            Request::CommitIntent(_) => "commit_intent",
            Request::ClearIntent(_) => "clear_intent",
            _ => return Self::default(),
        };
        Self { request_type: Some(request_type) }
    }

    #[must_use]
    pub(crate) fn timer(&self, stage: WriteStage) -> Option<HistogramTimer> {
        let request_type = self.request_type?;
        Some(WRITE_STAGE_DURATION.with_label_values(&[request_type, stage.label()]).start_timer())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::pending::{CommitFence, ProposalOutcome, ProposalWatcher};

    #[sekas_macro::test]
    async fn commit_fence_timer_records_only_nonempty_waits() {
        let metrics = WriteMetrics { request_type: Some("test_commit_fence") };
        let histogram =
            WRITE_STAGE_DURATION.with_label_values(&["test_commit_fence", "commit_fence"]);
        CommitFence::none().wait_for_write(metrics).await.unwrap();
        assert_eq!(histogram.get_sample_count(), 0);

        let watcher = ProposalWatcher::new();
        let fence = CommitFence::from_watcher(watcher.clone());
        let wait = fence.wait_for_write(metrics);
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        watcher.complete(ProposalOutcome::Applied);
        wait.await.unwrap();
        assert_eq!(histogram.get_sample_count(), 1);
    }
}
