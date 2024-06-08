// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::eraftpb::HardState;

use crate::raft::{Raft, SoftState, StateRole};
use crate::storage::Storage;
use crate::ProgressTracker;

/// Represents the current status of the raft
#[derive(Default)]
pub struct Status<'a> {
    /// The ID of the current node.
    pub id: u64,
    /// The hardstate of the raft, representing voted state.
    pub hs: HardState,
    /// The softstate of the raft, representing proposed state.
    pub ss: SoftState,
    /// The index of the last entry to have been applied.
    pub applied: u64,
    /// The progress towards catching up and applying logs.
    pub progress: Option<&'a ProgressTracker>,
}

impl<'a> Status<'a> {
    /// Gets a copy of the current raft status.
    pub fn new<T: Storage>(raft: &'a Raft<T>) -> Status<'a> {
        let mut s = Status {
            id: raft.id,
            ..Default::default()
        };
        s.hs = raft.hard_state();
        s.ss = raft.soft_state();
        s.applied = raft.raft_log.applied;
        if s.ss.raft_state == StateRole::Leader {
            s.progress = Some(raft.prs());
        }
        s
    }
}
