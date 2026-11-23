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
use raft::prelude::Snapshot;

use super::{SNAP_DATA, SnapManager};
use crate::raftgroup::StateMachine;
use crate::raftgroup::applier::Applier;
use crate::raftgroup::metrics::*;
use crate::{Error, Result, record_latency};

pub fn prepare_snapshot<M: StateMachine>(
    replica_id: u64,
    snap_mgr: &SnapManager,
    applier: &mut Applier<M>,
    snapshot: &Snapshot,
) -> Result<String> {
    record_latency!(take_apply_snapshot_metrics());
    let snap_id = &snapshot.data;
    let snap_info = snap_mgr.lock_snap(replica_id, snap_id).ok_or_else(|| {
        Error::InvalidData(format!("replica {replica_id} cannot find snapshot {snap_id:?}"))
    })?;
    let expected = snapshot.get_metadata();
    if snap_info
        .meta
        .apply_state
        .is_none_or(|state| state.index != expected.index || state.term != expected.term)
    {
        return Err(Error::InvalidData(format!("replica {replica_id} snapshot position changed")));
    }
    let snap_dir = snap_info.base_dir.join(SNAP_DATA);
    applier.prepare_snapshot(&snap_dir, &snap_info.meta)
}
