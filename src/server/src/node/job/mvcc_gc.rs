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
use std::time::Duration;

use log::{debug, warn};
use sekas_runtime::JoinHandle;
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use crate::engine::mvcc_gc::min_allowed_version_from_retention;
use crate::engine::{GroupEngine, SnapshotMode};
use crate::node::NodeConfig;
use crate::replica::Replica;
use crate::{Error, Result};

pub(crate) fn setup(cfg: NodeConfig, replica: Arc<Replica>) -> Option<JoinHandle<()>> {
    if cfg.mvcc_gc_interval_ms == 0 || cfg.mvcc_gc_retention_ms == 0 || cfg.mvcc_gc_keys == 0 {
        return None;
    }

    Some(sekas_runtime::spawn(async move {
        let interval = Duration::from_millis(cfg.mvcc_gc_interval_ms);
        loop {
            sekas_runtime::time::sleep(interval).await;
            if let Err(err) = gc_replica(&cfg, &replica).await {
                warn!(
                    "group {} replica {} mvcc gc: {err}",
                    replica.replica_info().group_id,
                    replica.replica_info().replica_id
                );
            }
        }
    }))
}

async fn gc_replica(cfg: &NodeConfig, replica: &Arc<Replica>) -> Result<()> {
    match replica.on_leader("mvcc gc", true).await? {
        Some(_) => {}
        None => return Ok(()),
    }

    if replica.move_shard_state().is_some() {
        debug!(
            "group {} replica {} skip mvcc gc because shard is moving",
            replica.replica_info().group_id,
            replica.replica_info().replica_id
        );
        return Ok(());
    }

    let min_allowed_version = min_allowed_version_from_retention(cfg.mvcc_gc_retention_ms);
    let group_engine = replica.group_engine();
    for shard in replica.descriptor().shards {
        gc_shard(replica, &group_engine, shard.id, min_allowed_version, cfg.mvcc_gc_keys).await?;
    }
    Ok(())
}

async fn gc_shard(
    replica: &Replica,
    group_engine: &GroupEngine,
    shard_id: u64,
    min_allowed_version: u64,
    batch_limit: usize,
) -> Result<()> {
    let mut snapshot = group_engine.snapshot(shard_id, SnapshotMode::Start { start_key: None })?;
    let mut collector = GcKeyCollector::new(min_allowed_version, batch_limit);
    while let Some(mvcc_iter) = snapshot.next() {
        let mvcc_iter = mvcc_iter?;
        let user_key = mvcc_iter.user_key().to_vec();
        collector.reset_key(user_key);
        for entry in mvcc_iter {
            let version = entry?.version();
            if collector.push_version(version) {
                flush_gc_keys(replica, shard_id, collector.take_batch()).await?;
            }
        }
        sekas_runtime::yield_now().await;
    }
    flush_gc_keys(replica, shard_id, collector.take_batch()).await
}

async fn flush_gc_keys(
    replica: &Replica,
    shard_id: u64,
    gc_keys: Vec<(Vec<u8>, u64)>,
) -> Result<()> {
    if gc_keys.is_empty() {
        return Ok(());
    }
    match replica.delete_chunks(shard_id, &gc_keys).await {
        Ok(()) | Err(Error::ShardNotFound(_)) => Ok(()),
        Err(err) => Err(err),
    }
}

struct GcKeyCollector {
    min_allowed_version: u64,
    batch_limit: usize,
    current_key: Vec<u8>,
    floor_version_kept: bool,
    batch: Vec<(Vec<u8>, u64)>,
}

impl GcKeyCollector {
    fn new(min_allowed_version: u64, batch_limit: usize) -> Self {
        GcKeyCollector {
            min_allowed_version,
            batch_limit,
            current_key: vec![],
            floor_version_kept: false,
            batch: Vec::with_capacity(batch_limit),
        }
    }

    fn reset_key(&mut self, user_key: Vec<u8>) {
        self.current_key = user_key;
        self.floor_version_kept = false;
    }

    fn push_version(&mut self, version: u64) -> bool {
        if version == TXN_INTENT_VERSION || version >= self.min_allowed_version {
            return false;
        }
        if !self.floor_version_kept {
            self.floor_version_kept = true;
            return false;
        }
        self.batch.push((self.current_key.clone(), version));
        self.batch.len() >= self.batch_limit
    }

    fn take_batch(&mut self) -> Vec<(Vec<u8>, u64)> {
        std::mem::replace(&mut self.batch, Vec::with_capacity(self.batch_limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collector_keeps_floor_version() {
        let mut collector = GcKeyCollector::new(60, 16);
        collector.reset_key(b"k".to_vec());
        for version in [100, 80, 50, 20] {
            assert!(!collector.push_version(version));
        }
        assert_eq!(collector.take_batch(), vec![(b"k".to_vec(), 20)]);

        collector.reset_key(b"k".to_vec());
        for version in [50, 20] {
            assert!(!collector.push_version(version));
        }
        assert_eq!(collector.take_batch(), vec![(b"k".to_vec(), 20)]);

        collector.reset_key(b"k".to_vec());
        for version in [100, 80] {
            assert!(!collector.push_version(version));
        }
        assert!(collector.take_batch().is_empty());
    }

    #[test]
    fn collector_keeps_intent_and_tombstone_floor() {
        let mut collector = GcKeyCollector::new(60, 16);
        collector.reset_key(b"k".to_vec());
        for version in [TXN_INTENT_VERSION, 50, 20] {
            assert!(!collector.push_version(version));
        }
        assert_eq!(collector.take_batch(), vec![(b"k".to_vec(), 20)]);
    }

    #[test]
    fn collector_signals_when_batch_is_full() {
        let mut collector = GcKeyCollector::new(60, 2);
        collector.reset_key(b"k".to_vec());
        assert!(!collector.push_version(50));
        assert!(!collector.push_version(20));
        assert!(collector.push_version(10));
        assert_eq!(collector.take_batch(), vec![(b"k".to_vec(), 20), (b"k".to_vec(), 10)]);
    }
}
