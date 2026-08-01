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

use sekas_api::server::v1::Value;
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::pending::{CommitFence, PendingWriteOverlay};
use crate::engine::GroupEngine;

#[derive(Clone)]
pub(super) struct PendingWriteView {
    overlay: PendingWriteOverlay,
}

#[derive(Clone, Debug)]
pub(super) struct WriteView {
    value: Option<Value>,
    fence: CommitFence,
}

impl PendingWriteView {
    pub fn new(_engine: GroupEngine, overlay: PendingWriteOverlay) -> Self {
        PendingWriteView { overlay }
    }

    pub fn read_latest(
        &self,
        shard_id: u64,
        user_key: &[u8],
        committed: Option<Value>,
    ) -> WriteView {
        let pending = self.overlay.latest(shard_id, user_key);
        let Some(pending) = pending else {
            return WriteView { value: committed, fence: CommitFence::none() };
        };

        if pending.value.version == TXN_INTENT_VERSION {
            return WriteView { value: Some(pending.value), fence: pending.fence };
        }

        let committed_is_newer = committed
            .as_ref()
            .map(|value| value.version > pending.value.version)
            .unwrap_or_default();
        if committed_is_newer {
            WriteView { value: committed, fence: CommitFence::none() }
        } else {
            WriteView { value: Some(pending.value), fence: pending.fence }
        }
    }
}

impl WriteView {
    pub fn fence(&self) -> CommitFence {
        self.fence.clone()
    }

    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}
