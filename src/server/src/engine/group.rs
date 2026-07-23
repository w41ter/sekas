// Copyright 2023-present The Sekas Authors.
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

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use log::{info, warn};
use prost::Message;
use sekas_api::server::v1::*;
use sekas_rock::lexical;
use sekas_schema::shard;

use super::RawDb;
use crate::constants::{INITIAL_EPOCH, LOCAL_TABLE_ID};
use crate::serverpb::v1::*;
use crate::{EngineConfig, Error, Result};

#[derive(Default)]
pub struct WriteStates {
    pub apply_state: Option<ApplyState>,
    pub descriptor: Option<GroupDesc>,
    pub move_shard_state: Option<MoveShardState>,
    pub deleted_shards: Vec<ShardDesc>,
}

#[derive(Default)]
#[repr(transparent)]
pub struct WriteBatch {
    inner: rocksdb::WriteBatch,
}

/// A structure supports grouped data, metadata saving and retriving.
///
/// NOTE: Shard are managed by `GroupEngine` instead of a shard engine, because
/// shards from different tables in the same group needs to persist on disk
/// at the same time, to guarantee the accuracy of applied index.
#[derive(Clone)]
pub(crate) struct GroupEngine
where
    Self: Send,
{
    cfg: EngineConfig,
    name: String,
    raw_db: Arc<RawDb>,
    deleted_shards: super::DeletedShardRegistry,
    core: Arc<RwLock<GroupEngineCore>>,
}

#[derive(Default)]
struct GroupEngineCore {
    group_desc: GroupDesc,
    shard_descs: HashMap<u64, ShardDesc>,
    deleted_shards: HashMap<u64, ShardDesc>,
    move_shard_state: Option<MoveShardState>,
}

/// Traverse the data of the group engine, but don't care about the data format.
pub(crate) struct RawIterator<'a> {
    apply_state: ApplyState,
    descriptor: GroupDesc,
    db_iter: rocksdb::DBIterator<'a>,
}

#[derive(Debug)]
enum SnapshotRange {
    Target { target_key: Vec<u8> },
    Prefix { prefix: Vec<u8> },
    Range { start: Vec<u8>, end: Vec<u8> },
}

/// A snapshot of data, to traverse the data of a shard in the group engine,
/// analyze and return the data (including tombstone).
#[derive(Debug)]
pub(crate) struct Snapshot<'a> {
    table_id: u64,
    range: Option<SnapshotRange>,

    core: SnapshotCore<'a>,
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub(crate) struct SnapshotCore<'a> {
    #[derivative(Debug = "ignore")]
    db_iter: rocksdb::DBIterator<'a>,
    current_key: Option<Vec<u8>>,
    cached_entry: Option<MvccEntry>,
}

/// Traverse multi-version of a single key.
#[derive(Debug)]
pub(crate) struct MvccIterator<'a, 'b> {
    snapshot: &'b mut Snapshot<'a>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MvccEntry {
    key: Box<[u8]>,
    user_key: Vec<u8>,
    value: Box<[u8]>,
}

#[derive(Debug)]
pub(crate) enum SnapshotMode<'a> {
    Start { start_key: Option<&'a [u8]> },
    Key { key: &'a [u8] },
    Prefix { key: &'a [u8] },
}

struct ColumnFamilyDecorator<'a, 'b> {
    cf_handle: Arc<rocksdb::BoundColumnFamily<'b>>,
    wb: &'a mut rocksdb::WriteBatch,
}

struct SlowIoGuard {
    threshold: u64,
    start: Instant,
}

impl GroupEngine {
    /// Create a new instance of group engine.
    pub(crate) async fn create(
        cfg: &EngineConfig,
        raw_db: Arc<RawDb>,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Self> {
        let name = Self::cf_name(group_id, replica_id);
        info!("group {group_id} replica {replica_id} create group engine, cf name is {name}");
        debug_assert!(raw_db.cf_handle(&name).is_none());
        raw_db.create_cf(&name)?;

        let desc =
            GroupDesc { id: group_id, epoch: INITIAL_EPOCH, shards: vec![], replicas: vec![] };

        let cf_handle = raw_db.cf_handle(&name).expect("cf must exists because it just created");
        let engine = GroupEngine {
            cfg: cfg.clone(),
            deleted_shards: raw_db.deleted_shards_for_cf(&name),
            name,
            raw_db: raw_db.clone(),
            core: Arc::new(RwLock::new(GroupEngineCore {
                group_desc: desc.clone(),
                shard_descs: Default::default(),
                deleted_shards: Default::default(),
                move_shard_state: None,
            })),
        };

        // The group descriptor should be persisted into disk.
        let states = WriteStates {
            apply_state: Some(ApplyState { index: 0, term: 0 }),
            descriptor: Some(desc),
            ..Default::default()
        };
        engine.commit(WriteBatch::default(), states, true)?;

        // Flush mem tables so that subsequent `ReadTier::Persisted` can be executed.
        raw_db.flush_cf(&cf_handle)?;

        Ok(engine)
    }

    /// Open the exists instance of group engine.
    pub(crate) async fn open(
        cfg: &EngineConfig,
        raw_db: Arc<RawDb>,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Option<Self>> {
        let name = Self::cf_name(group_id, replica_id);
        let cf_handle = match raw_db.cf_handle(&name) {
            Some(cf_handle) => cf_handle,
            None => {
                return Ok(None);
            }
        };
        let deleted_shard_registry = raw_db.deleted_shards_for_cf(&name);

        let group_desc = internal::descriptor(&raw_db, &cf_handle)?;
        let move_shard_state = internal::move_shard_state(&raw_db, &cf_handle)?;
        let deleted_shards = internal::deleted_shards(&raw_db, &cf_handle)?;
        for shard in &deleted_shards {
            deleted_shard_registry.add(shard);
        }
        let mut shard_descs = internal::shard_descs(&group_desc);
        if let Some(shard_desc) = move_shard_state.as_ref().map(|m| m.get_shard_desc()) {
            shard_descs.entry(shard_desc.id).or_insert_with(|| shard_desc.clone());
        }
        for shard in &deleted_shards {
            shard_descs.entry(shard.id).or_insert_with(|| shard.clone());
        }
        let deleted_shards =
            deleted_shards.into_iter().map(|shard| (shard.id, shard)).collect::<HashMap<_, _>>();
        let core = GroupEngineCore { move_shard_state, group_desc, shard_descs, deleted_shards };

        Ok(Some(GroupEngine {
            cfg: cfg.clone(),
            name,
            raw_db: raw_db.clone(),
            deleted_shards: deleted_shard_registry,
            core: Arc::new(RwLock::new(core)),
        }))
    }

    /// Destory a group engine.
    pub(crate) async fn destory(group_id: u64, replica_id: u64, raw_db: Arc<RawDb>) -> Result<()> {
        let name = Self::cf_name(group_id, replica_id);
        raw_db.drop_cf(&name)?;
        info!("destory column family {}", name);
        Ok(())
    }

    /// Return the move shard state.
    #[inline]
    pub fn move_shard_state(&self) -> Option<MoveShardState> {
        self.core.read().unwrap().move_shard_state.clone()
    }

    pub fn deleted_shards(&self) -> Vec<ShardDesc> {
        self.core.read().unwrap().deleted_shards.values().cloned().collect()
    }

    pub fn mark_deleted_shard(&self, shard: ShardDesc) {
        {
            let mut core = self.core.write().unwrap();
            core.deleted_shards.insert(shard.id, shard.clone());
            core.shard_descs.entry(shard.id).or_insert_with(|| shard.clone());
        }
        self.deleted_shards.add(&shard);
    }

    pub fn clear_deleted_shard(&self, shard_id: u64) -> Result<()> {
        let cf_handle = self.cf_handle();
        let mut wb = rocksdb::WriteBatch::default();
        wb.delete_cf(&cf_handle, keys::deleted_shard(shard_id));
        self.raw_db.write_opt(wb, &rocksdb::WriteOptions::default())?;
        self.deleted_shards.remove(shard_id);
        let mut core = self.core.write().unwrap();
        core.deleted_shards.remove(&shard_id);
        if core.group_desc.shard(shard_id).is_none()
            && !core.move_shard_state.as_ref().is_some_and(|state| state.get_shard_id() == shard_id)
        {
            core.shard_descs.remove(&shard_id);
        }
        Ok(())
    }

    /// Return the group descriptor.
    #[inline]
    pub fn descriptor(&self) -> GroupDesc {
        self.core.read().unwrap().group_desc.clone()
    }

    /// Return the persisted apply state of raft.
    #[inline]
    pub fn flushed_apply_state(&self) -> Result<ApplyState> {
        internal::flushed_apply_state(&self.raw_db, &self.cf_handle())
    }

    /// Get the latest key value from the corresponding shard.
    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Value>> {
        let snapshot_mode = SnapshotMode::Key { key };
        let mut snapshot = self.snapshot(shard_id, snapshot_mode)?;
        if let Some(iter) = snapshot.next() {
            let mut iter = iter?;
            if let Some(entry) = iter.next() {
                let entry = entry?;
                return Ok(Some(entry.into()));
            }
        }
        Ok(None)
    }

    /// Get all versions.
    pub async fn get_all_versions(&self, shard_id: u64, key: &[u8]) -> Result<ValueSet> {
        let snapshot_mode = SnapshotMode::Key { key };
        let mut snapshot = self.snapshot(shard_id, snapshot_mode)?;
        let mut value_set = ValueSet { user_key: key.to_owned(), values: vec![] };
        if let Some(iter) = snapshot.next() {
            for entry in iter? {
                let entry = entry?;
                value_set.values.push(entry.into());
            }
        }
        Ok(value_set)
    }

    /// Put key value into the corresponding shard.
    pub fn put(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        value: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let table_id = desc.table_id;
        debug_assert_ne!(table_id, LOCAL_TABLE_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.put(keys::mvcc_key(table_id, key, version), values::data(value));

        Ok(())
    }

    /// Logically delete key from the corresponding shard.
    pub fn tombstone(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let table_id = desc.table_id;
        debug_assert_ne!(table_id, LOCAL_TABLE_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.put(keys::mvcc_key(table_id, key, version), values::tombstone());

        Ok(())
    }

    pub fn delete(
        &self,
        wb: &mut WriteBatch,
        shard_id: u64,
        key: &[u8],
        version: u64,
    ) -> Result<()> {
        let desc = self.shard_desc(shard_id)?;
        let table_id = desc.table_id;
        debug_assert_ne!(table_id, LOCAL_TABLE_ID);
        debug_assert!(shard::belong_to(&desc, key));

        wb.delete(keys::mvcc_key(table_id, key, version));

        Ok(())
    }

    #[inline]
    pub fn commit(&self, wb: WriteBatch, states: WriteStates, persisted: bool) -> Result<()> {
        self.group_commit(&[wb], states, persisted)
    }

    pub fn group_commit(
        &self,
        wbs: &[WriteBatch],
        states: WriteStates,
        persisted: bool,
    ) -> Result<()> {
        use rocksdb::WriteOptions;

        let cf_handle = self.cf_handle();
        let mut inner_wb = rocksdb::WriteBatch::default();
        let mut decorator =
            ColumnFamilyDecorator { cf_handle: cf_handle.clone(), wb: &mut inner_wb };
        for wb in wbs {
            wb.inner.iterate(&mut decorator);
        }
        states.write(&mut inner_wb, &cf_handle);

        let mut opts = WriteOptions::default();
        if persisted {
            opts.set_sync(true);
        } else {
            opts.disable_wal(true);
        }

        {
            let _slow_io_guard = self.cfg.engine_slow_io_threshold_ms.map(SlowIoGuard::new);
            self.raw_db.write_opt(inner_wb, &opts)?;
        }

        if states.descriptor.is_some() || states.move_shard_state.is_some() {
            self.apply_core_states(states.descriptor, states.move_shard_state);
        }
        for shard in states.deleted_shards {
            self.mark_deleted_shard(shard);
        }

        Ok(())
    }

    pub fn snapshot(&self, shard_id: u64, mode: SnapshotMode) -> Result<Snapshot<'_>> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};

        let desc = self.shard_desc(shard_id)?;
        let table_id = desc.table_id;
        debug_assert_ne!(table_id, LOCAL_TABLE_ID);

        let opts = ReadOptions::default();
        let key = match &mode {
            SnapshotMode::Start { start_key: Some(start_key) } => {
                debug_assert!(
                    shard::belong_to(&desc, start_key),
                    "shard desc {desc:?} start key {start_key:?}"
                );
                keys::raw(table_id, start_key)
            }
            SnapshotMode::Start { start_key: None } => {
                // An empty key is equivalent to range start key.
                keys::raw(table_id, &shard::start_key(&desc))
            }
            SnapshotMode::Key { key } => {
                debug_assert!(shard::belong_to(&desc, key), "shard desc {desc:?} key {key:?}");
                keys::raw(table_id, key)
            }
            SnapshotMode::Prefix { key } => {
                debug_assert!(shard::belong_to(&desc, key), "shard desc {desc:?} key {key:?}");
                keys::raw(table_id, key)
            }
        };
        let inner_mode = IteratorMode::From(&key, Direction::Forward);
        let iter = self.raw_db.iterator_cf_opt(&self.cf_handle(), opts, inner_mode);
        Ok(Snapshot::new(table_id, iter, mode, &desc))
    }

    pub fn raw_iter(&self) -> Result<RawIterator<'_>> {
        use rocksdb::{IteratorMode, ReadOptions};

        let opts = ReadOptions::default();
        let iter = self.raw_db.iterator_cf_opt(&self.cf_handle(), opts, IteratorMode::Start);
        RawIterator::new(iter)
    }

    /// Ingest data into group engine.
    pub fn ingest<P: AsRef<Path>>(&self, files: Vec<P>) -> Result<()> {
        use rocksdb::IngestExternalFileOptions;

        self.raw_db.drop_cf(&self.name)?;
        self.raw_db.create_cf(&self.name)?;

        let opts = IngestExternalFileOptions::default();
        let cf_handle = self.cf_handle();
        self.raw_db.ingest_external_file_cf_opts(&cf_handle, &opts, files)?;

        let group_desc = internal::descriptor(&self.raw_db, &cf_handle)?;
        let move_shard_state = internal::move_shard_state(&self.raw_db, &cf_handle)?;
        let deleted_shards = internal::deleted_shards(&self.raw_db, &cf_handle)?;
        for shard in &deleted_shards {
            self.deleted_shards.add(shard);
        }
        self.apply_core_states(Some(group_desc), move_shard_state);
        for shard in deleted_shards {
            self.mark_deleted_shard(shard);
        }

        Ok(())
    }

    pub fn apply_core_states(
        &self,
        descriptor: Option<GroupDesc>,
        move_shard_state: Option<MoveShardState>,
    ) {
        let mut core = self.core.write().unwrap();
        if let Some(desc) = descriptor {
            core.group_desc = desc;
        }

        // TODO(walter) remove shard desc if move shard task is aborted.
        if let Some(move_shard_state) = move_shard_state {
            if move_shard_state.step == MoveShardStep::Finished as i32
                || move_shard_state.step == MoveShardStep::Aborted as i32
            {
                core.move_shard_state = None;
            } else {
                core.move_shard_state = Some(move_shard_state);
            }
        }

        core.shard_descs = internal::shard_descs(&core.group_desc);
        if let Some(shard_desc) = core.move_shard_state.as_ref().map(|m| m.get_shard_desc().clone())
        {
            core.shard_descs.entry(shard_desc.id).or_insert(shard_desc);
        }
    }

    /// Get the approximates size of the target shard.
    pub fn get_approximate_size(&self, shard_id: u64) -> Result<u64> {
        let (start, end) = self.shard_raw_boundary(shard_id)?;
        self.raw_db.get_approximate_size(&self.cf_handle(), &start, &end)
    }

    /// Estimate the split keys (in user key) of the target shard.
    pub fn estimate_split_key(&self, shard_id: u64) -> Result<Option<Vec<u8>>> {
        let (start, end) = self.shard_raw_boundary(shard_id)?;
        let estimated_split_keys =
            self.raw_db.estimate_split_keys_in_range(&self.cf_handle(), &start, &end)?;
        Ok(estimate_user_split_key(&estimated_split_keys))
    }

    /// return the desc of the specified shard.
    #[inline]
    pub fn shard_desc(&self, shard_id: u64) -> Result<ShardDesc> {
        self.core
            .read()
            .expect("read lock")
            .shard_descs
            .get(&shard_id)
            .cloned()
            .ok_or(Error::ShardNotFound(shard_id))
    }

    #[inline]
    fn cf_handle(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.raw_db.cf_handle(&self.name).expect("column family handle")
    }

    #[inline]
    fn cf_name(group_id: u64, replica_id: u64) -> String {
        // Using the replica id avoids the problem of creating a new replica immediately
        // after deleting the replica.
        format!("{group_id}-{replica_id}")
    }

    /// Get the raw db boundary of the target shard.
    fn shard_raw_boundary(&self, shard_id: u64) -> Result<(Vec<u8>, Vec<u8>)> {
        let shard_desc = self.shard_desc(shard_id)?;
        let RangePartition { start, end } = shard_desc.range.ok_or_else(|| {
            Error::InvalidData(format!("the range field of shard {shard_id} is not set"))
        })?;
        let start = keys::raw(shard_desc.table_id, &start);
        let end = if end.is_empty() {
            lexical::lexical_next_boundary(&keys::raw(shard_desc.table_id, &end))
        } else {
            keys::raw(shard_desc.table_id, &end)
        };
        Ok((start, end))
    }
}

impl<'a> RawIterator<'a> {
    fn new(mut db_iter: rocksdb::DBIterator<'a>) -> Result<Self> {
        use rocksdb::IteratorMode;

        let apply_state = next_message(&mut db_iter, &keys::apply_state())?;
        let descriptor = next_message(&mut db_iter, &keys::descriptor())?;
        db_iter.set_mode(IteratorMode::Start);

        Ok(RawIterator { apply_state, descriptor, db_iter })
    }

    #[inline]
    pub fn apply_state(&self) -> &ApplyState {
        &self.apply_state
    }

    #[inline]
    pub fn descriptor(&self) -> &GroupDesc {
        &self.descriptor
    }
}

impl<'a> Iterator for RawIterator<'a> {
    /// Key value pairs.
    type Item = <rocksdb::DBIterator<'a> as Iterator>::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.db_iter.next()
    }
}

impl<'a> Snapshot<'a> {
    fn new<'b>(
        table_id: u64,
        db_iter: rocksdb::DBIterator<'a>,
        snapshot_mode: SnapshotMode<'b>,
        desc: &ShardDesc,
    ) -> Self {
        let range = match snapshot_mode {
            SnapshotMode::Key { key } => Some(SnapshotRange::Target { target_key: key.to_owned() }),
            SnapshotMode::Prefix { key } => Some(SnapshotRange::Prefix { prefix: key.to_owned() }),
            SnapshotMode::Start { start_key } => Some(SnapshotRange::Range {
                start: start_key.map(ToOwned::to_owned).unwrap_or_else(|| shard::start_key(desc)),
                end: shard::end_key(desc),
            }),
        };

        Snapshot {
            table_id,
            range,
            core: SnapshotCore { db_iter, current_key: None, cached_entry: None },
        }
    }

    pub fn next(&mut self) -> Option<Result<MvccIterator<'a, '_>>> {
        self.next_mvcc_iterator()
    }

    fn next_mvcc_iterator(&mut self) -> Option<Result<MvccIterator<'a, '_>>> {
        let core = &mut self.core;
        loop {
            if let Some(entry) = core.cached_entry.as_ref() {
                if let Some(range) = self.range.as_ref() {
                    if !range.is_valid_key(entry.user_key()) {
                        // The iterate target has been consumed.
                        return None;
                    }
                }

                // Skip iterated keys.
                // TODO(walter) support seek to next user key to skip old versions.
                if !core.is_current_key(entry.user_key()) {
                    core.current_key = Some(entry.user_key().to_owned());
                    return Some(Ok(MvccIterator { snapshot: self }));
                }
            }

            if let Err(err) = core.next_entry(self.table_id)? {
                return Some(Err(err));
            }
        }
    }

    fn next_mvcc_entry(&mut self) -> Option<Result<MvccEntry>> {
        let core = &mut self.core;
        loop {
            if let Some(entry) = core.cached_entry.take() {
                if core.is_current_key(entry.user_key()) {
                    return Some(Ok(entry));
                } else {
                    core.cached_entry = Some(entry);
                    return None;
                }
            }

            if let Err(err) = core.next_entry(self.table_id)? {
                return Some(Err(err));
            }
        }
    }
}

impl<'a> SnapshotCore<'a> {
    fn next_entry(&mut self, table_id: u64) -> Option<Result<()>> {
        let (key, value) = match self.db_iter.next()? {
            Ok(v) => v,
            Err(err) => return Some(Err(err.into())),
        };

        let prefix = &key[..core::mem::size_of::<u64>()];
        if prefix != table_id.to_le_bytes().as_slice() {
            return None;
        }

        self.cached_entry = Some(MvccEntry::new(key, value));
        Some(Ok(()))
    }

    #[inline]
    fn is_current_key(&self, target_key: &[u8]) -> bool {
        self.current_key.as_ref().map(|k| k == target_key).unwrap_or_default()
    }
}

impl<'a, 'b> MvccIterator<'a, 'b> {
    /// Return the user key of this mvcc iterator.
    pub fn user_key(&self) -> &[u8] {
        self.snapshot
            .core
            .current_key
            .as_ref()
            .expect("the current key always exists if MvccIterator is constructed")
    }
}

impl<'a, 'b> Iterator for MvccIterator<'a, 'b> {
    type Item = Result<MvccEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.snapshot.next_mvcc_entry()
    }
}

impl MvccEntry {
    pub fn new(key: Box<[u8]>, value: Box<[u8]>) -> Self {
        let user_key = keys::revert_mvcc_key(&key);
        MvccEntry { key, user_key, value }
    }

    #[inline]
    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    pub fn version(&self) -> u64 {
        const L: usize = core::mem::size_of::<u64>();
        let len = self.key.len();
        let bytes = &self.key[(len - L)..];
        let mut buf = [0u8; L];
        buf[..].copy_from_slice(bytes);
        !u64::from_be_bytes(buf)
    }

    /// Return value of this `MvccEntry`. `None` is returned if this entry is a
    /// tombstone.
    pub fn value(&self) -> Option<&[u8]> {
        if self.value[0] == values::TOMBSTONE {
            None
        } else {
            debug_assert_eq!(self.value[0], values::DATA);
            Some(&self.value[1..])
        }
    }

    #[allow(dead_code)]
    pub fn is_tombstone(&self) -> bool {
        self.value[0] == values::TOMBSTONE
    }

    #[allow(dead_code)]
    pub fn is_data(&self) -> bool {
        self.value[0] == values::DATA
    }
}

impl From<MvccEntry> for Value {
    fn from(entry: MvccEntry) -> Self {
        Value { content: entry.value().map(ToOwned::to_owned), version: entry.version() }
    }
}

impl SnapshotRange {
    #[inline]
    fn is_valid_key(&self, key: &[u8]) -> bool {
        match self {
            SnapshotRange::Target { target_key } if target_key == key => true,
            SnapshotRange::Prefix { prefix } if key.starts_with(prefix) => true,
            SnapshotRange::Range { start, end } if shard::in_range(start, end, key) => true,
            _ => false,
        }
    }
}

impl<'a> Default for SnapshotMode<'a> {
    fn default() -> Self {
        SnapshotMode::Start { start_key: None }
    }
}

pub(super) mod keys {
    const APPLY_STATE: &[u8] = b"APPLY_STATE";
    const DESCRIPTOR: &[u8] = b"DESCRIPTOR";
    const MIGRATE_STATE: &[u8] = b"MIGRATE_STATE";

    pub(crate) struct ParsedMvccKey<'a> {
        pub table_id: u64,
        pub encoded_user_key: &'a [u8],
        pub version: u64,
    }

    #[inline]
    pub fn raw(table_id: u64, key: &[u8]) -> Vec<u8> {
        if key.is_empty() {
            table_id.to_le_bytes().as_slice().to_owned()
        } else {
            mvcc_key(table_id, key, u64::MAX)
        }
    }

    /// Generate mvcc key with the memcomparable format.
    pub fn mvcc_key(table_id: u64, key: &[u8], version: u64) -> Vec<u8> {
        use std::io::{Cursor, Read};

        debug_assert!(!key.is_empty());
        let actual_len = (((key.len() - 1) / 8) + 1) * 9;
        let buf_len = 2 * core::mem::size_of::<u64>() + actual_len;
        let mut buf = Vec::with_capacity(buf_len);
        buf.extend_from_slice(table_id.to_le_bytes().as_slice());
        let mut cursor = Cursor::new(key);
        while !cursor_remaining_is_empty(&cursor) {
            let mut group = [0u8; 8];
            let mut size = cursor.read(&mut group[..]).unwrap() as u8;
            debug_assert_ne!(size, 0);
            if size == 8 && !cursor_remaining_is_empty(&cursor) {
                size += 1;
            }
            buf.extend_from_slice(group.as_slice());
            buf.push(b'0' + size);
        }
        buf.extend_from_slice((!version).to_be_bytes().as_slice());
        buf
    }

    /// Extracts user key from the mvcc key.
    pub fn may_revert_mvcc_key(key: &[u8]) -> Option<Vec<u8>> {
        parse_mvcc_key(key).map(|parsed| revert_encoded_user_key(parsed.encoded_user_key))
    }

    pub fn is_mvcc_key(key: &[u8]) -> bool {
        table_id(key).map(|table_id| table_id != super::LOCAL_TABLE_ID).unwrap_or_default()
    }

    pub fn parse_mvcc_key(key: &[u8]) -> Option<ParsedMvccKey<'_>> {
        let table_id = table_id(key)?;
        if table_id == super::LOCAL_TABLE_ID {
            return None;
        }

        const L: usize = core::mem::size_of::<u64>();
        let (_, encoded_user_key, version_bytes) = split_mvcc_key(key)?;
        let mut version_buf = [0u8; L];
        version_buf.copy_from_slice(version_bytes);
        let version = !u64::from_be_bytes(version_buf);

        Some(ParsedMvccKey { table_id, encoded_user_key, version })
    }

    fn table_id(key: &[u8]) -> Option<u64> {
        const L: usize = core::mem::size_of::<u64>();
        if key.len() < L {
            return None;
        }
        let mut buf = [0u8; L];
        buf.copy_from_slice(&key[..L]);
        Some(u64::from_le_bytes(buf))
    }

    fn split_mvcc_key(key: &[u8]) -> Option<(&[u8], &[u8], &[u8])> {
        const L: usize = core::mem::size_of::<u64>();
        let len = key.len();
        if len <= 2 * L || !(len - 2 * L).is_multiple_of(9) {
            return None;
        }

        let encoded_user_key = &key[L..(len - L)];
        validate_encoded_user_key(encoded_user_key)?;

        Some((&key[..L], encoded_user_key, &key[(len - L)..]))
    }

    /// Extracts user key from the mvcc key.
    pub fn revert_mvcc_key(key: &[u8]) -> Vec<u8> {
        const L: usize = core::mem::size_of::<u64>();
        let len = key.len();
        debug_assert!(len > 2 * L);
        let encoded_user_key = &key[L..(len - L)];

        debug_assert_eq!(encoded_user_key.len() % 9, 0);
        revert_encoded_user_key(encoded_user_key)
    }

    pub fn revert_encoded_user_key(encoded_user_key: &[u8]) -> Vec<u8> {
        use std::io::{Cursor, Read};

        let num_groups = encoded_user_key.len() / 9;
        let mut buf = Vec::with_capacity(num_groups * 8);
        let mut cursor = Cursor::new(encoded_user_key);
        while !cursor_remaining_is_empty(&cursor) {
            let mut group = [0u8; 9];
            let _ = cursor.read(&mut group[..]).unwrap();
            let num_element = std::cmp::min((group[8] - b'0') as usize, 8);
            buf.extend_from_slice(&group[..num_element]);
        }
        buf
    }

    fn validate_encoded_user_key(encoded_user_key: &[u8]) -> Option<()> {
        if encoded_user_key.is_empty() || !encoded_user_key.len().is_multiple_of(9) {
            return None;
        }
        for group in encoded_user_key.as_chunks::<9>().0 {
            let num_element = group[8].checked_sub(b'0')? as usize;
            if num_element == 0 || num_element > 9 {
                return None;
            }
        }
        Some(())
    }

    fn cursor_remaining_is_empty(cursor: &std::io::Cursor<&[u8]>) -> bool {
        cursor.position() as usize >= cursor.get_ref().len()
    }

    #[inline]
    pub fn apply_state() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + APPLY_STATE.len());
        buf.extend_from_slice(super::LOCAL_TABLE_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(APPLY_STATE);
        buf
    }

    #[inline]
    pub fn descriptor() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + DESCRIPTOR.len());
        buf.extend_from_slice(super::LOCAL_TABLE_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(DESCRIPTOR);
        buf
    }

    #[inline]
    pub fn move_shard_state() -> Vec<u8> {
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() + MIGRATE_STATE.len());
        buf.extend_from_slice(super::LOCAL_TABLE_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(MIGRATE_STATE);
        buf
    }

    #[inline]
    pub fn deleted_shard(shard_id: u64) -> Vec<u8> {
        const DELETED_SHARD: &[u8] = b"DELETED_SHARD";
        let mut buf = Vec::with_capacity(core::mem::size_of::<u64>() * 2 + DELETED_SHARD.len());
        buf.extend_from_slice(super::LOCAL_TABLE_ID.to_le_bytes().as_slice());
        buf.extend_from_slice(DELETED_SHARD);
        buf.extend_from_slice(shard_id.to_le_bytes().as_slice());
        buf
    }
}

mod values {
    pub(super) const DATA: u8 = 0;
    pub(super) const TOMBSTONE: u8 = 1;

    #[inline]
    pub fn tombstone() -> &'static [u8] {
        &[TOMBSTONE]
    }

    pub fn data(v: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(v.len() + 1);
        buf.push(DATA);
        buf.extend_from_slice(v);
        buf
    }
}

impl<'a, 'b> rocksdb::WriteBatchIterator for ColumnFamilyDecorator<'a, 'b> {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.wb.put_cf(&self.cf_handle, key, value);
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.wb.delete_cf(&self.cf_handle, key);
    }
}

impl WriteBatch {
    #[inline]
    pub fn new(content: &[u8]) -> Self {
        WriteBatch { inner: rocksdb::WriteBatch::from_data(content) }
    }
}

impl Deref for WriteBatch {
    type Target = rocksdb::WriteBatch;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WriteBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl WriteStates {
    fn write(&self, wb: &mut rocksdb::WriteBatch, cf_handle: &impl rocksdb::AsColumnFamilyRef) {
        if let Some(apply_state) = &self.apply_state {
            wb.put_cf(cf_handle, keys::apply_state(), apply_state.encode_to_vec());
        }
        if let Some(desc) = &self.descriptor {
            wb.put_cf(cf_handle, keys::descriptor(), desc.encode_to_vec());
        }
        if let Some(move_shard_state) = &self.move_shard_state {
            // Moving shard in abort or finish steps are not persisted.
            if move_shard_state.step != MoveShardStep::Finished as i32
                && move_shard_state.step != MoveShardStep::Aborted as i32
            {
                wb.put_cf(cf_handle, keys::move_shard_state(), move_shard_state.encode_to_vec());
            } else {
                wb.delete_cf(cf_handle, keys::move_shard_state());
            }
        }
        for shard in &self.deleted_shards {
            wb.put_cf(cf_handle, keys::deleted_shard(shard.id), shard.encode_to_vec());
        }
    }
}

impl SlowIoGuard {
    fn new(threshold: u64) -> Self {
        use rocksdb::perf::*;

        set_perf_stats(PerfStatsLevel::EnableTime);
        SlowIoGuard { threshold, start: Instant::now() }
    }
}

impl Drop for SlowIoGuard {
    fn drop(&mut self) {
        use rocksdb::perf::*;

        let mut perf_ctx = PerfContext::default();
        if self.start.elapsed() >= Duration::from_millis(self.threshold) {
            warn!("rocksdb slow io: {}", perf_ctx.report(true));
        }

        perf_ctx.reset();
        set_perf_stats(PerfStatsLevel::Disable);
    }
}

mod internal {
    use super::*;

    pub(super) fn descriptor(
        db: &RawDb,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<GroupDesc> {
        let value = db
            .get_pinned_cf(cf_handle, keys::descriptor())?
            .expect("group descriptor will persisted when creating group");
        Ok(GroupDesc::decode(value.as_ref())?)
    }

    pub(super) fn move_shard_state(
        db: &RawDb,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<Option<MoveShardState>> {
        if let Some(v) = db.get_pinned_cf(cf_handle, keys::move_shard_state())? {
            Ok(Some(MoveShardState::decode(v.as_ref())?))
        } else {
            Ok(None)
        }
    }

    pub(super) fn deleted_shards(
        db: &RawDb,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<Vec<ShardDesc>> {
        use rocksdb::{Direction, IteratorMode, ReadOptions};

        const DELETED_SHARD: &[u8] = b"DELETED_SHARD";
        let mut prefix = Vec::with_capacity(core::mem::size_of::<u64>() + DELETED_SHARD.len());
        prefix.extend_from_slice(super::LOCAL_TABLE_ID.to_le_bytes().as_slice());
        prefix.extend_from_slice(DELETED_SHARD);

        let opts = ReadOptions::default();
        let iter =
            db.iterator_cf_opt(cf_handle, opts, IteratorMode::From(&prefix, Direction::Forward));
        let mut shards = vec![];
        for entry in iter {
            let (key, value) = entry?;
            if !key.starts_with(&prefix) {
                break;
            }
            shards.push(ShardDesc::decode(value.as_ref())?);
        }
        Ok(shards)
    }

    pub(super) fn flushed_apply_state(
        db: &RawDb,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
    ) -> Result<ApplyState> {
        use rocksdb::{ReadOptions, ReadTier};
        let mut opt = ReadOptions::default();
        opt.set_read_tier(ReadTier::Persisted);
        let value = db
            .get_pinned_cf_opt(cf_handle, keys::apply_state(), &opt)?
            .expect("apply state will persisted when creating group");
        Ok(ApplyState::decode(value.as_ref())?)
    }

    #[inline]
    pub(super) fn shard_descs(group_desc: &GroupDesc) -> HashMap<u64, ShardDesc> {
        group_desc.shards.iter().map(|shard| (shard.id, shard.clone())).collect::<HashMap<_, _>>()
    }
}

fn next_message<T: prost::Message + Default>(
    db_iter: &mut rocksdb::DBIterator<'_>,
    key: &[u8],
) -> Result<T> {
    use rocksdb::{Direction, IteratorMode};

    db_iter.set_mode(IteratorMode::From(key, Direction::Forward));
    match db_iter.next() {
        Some(Ok((_, value))) => Ok(T::decode(&*value).expect("should encoded with T")),
        Some(Err(err)) => Err(err.into()),
        None => Err(Error::InvalidData("no such key exists".into())),
    }
}

fn estimate_user_split_key(estimated_split_keys: &[Vec<u8>]) -> Option<Vec<u8>> {
    if estimated_split_keys.is_empty() {
        return None;
    }

    let split_point = estimated_split_keys.len() / 2;
    let right = keys::may_revert_mvcc_key(&estimated_split_keys[split_point])?;
    for key in estimated_split_keys[..split_point].iter().rev() {
        let left = keys::may_revert_mvcc_key(key)?;
        if left < right {
            return Some(shortest_separator_prefix(&left, &right));
        }
    }

    Some(right)
}

fn shortest_separator_prefix(left: &[u8], right: &[u8]) -> Vec<u8> {
    debug_assert!(left < right);

    let mut pos = 0;
    while pos < left.len() && pos < right.len() && left[pos] == right[pos] {
        pos += 1;
    }

    right[..=pos].to_vec()
}

#[cfg(test)]
mod tests {
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;

    async fn create_engine(group_id: u64, shard_id: u64, path: &Path) -> GroupEngine {
        create_engine_with_range(group_id, shard_id, vec![], vec![], path).await
    }

    async fn create_engine_with_range(
        group_id: u64,
        shard_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
        path: &Path,
    ) -> GroupEngine {
        use crate::bootstrap::open_engine_with_default_config;

        let db_dir = path.join("db");
        let db = open_engine_with_default_config(db_dir).unwrap();
        let db = Arc::new(db);
        let group_engine =
            GroupEngine::create(&EngineConfig::default(), db.clone(), group_id, shard_id)
                .await
                .unwrap();

        let wb = WriteBatch::default();
        let states = WriteStates {
            descriptor: Some(GroupDesc {
                id: group_id,
                shards: vec![ShardDesc::with_range(shard_id, 1, start, end)],
                ..Default::default()
            }),
            ..Default::default()
        };

        group_engine.commit(wb, states, false).unwrap();

        group_engine
    }

    #[test]
    fn memory_comparable_format() {
        struct Less {
            left: &'static [u8],
            left_version: u64,
            right: &'static [u8],
            right_version: u64,
        }

        let tests = [
            // 1. compare version
            Less { left: b"1", left_version: 1, right: b"1", right_version: 0 },
            Less { left: b"1", left_version: 256, right: b"1", right_version: 255 },
            Less { left: b"12345678", left_version: 256, right: b"12345678", right_version: 255 },
            Less { left: b"123456789", left_version: 256, right: b"123456789", right_version: 255 },
            // 2. different length
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"123456789",
                right_version: 0,
            },
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"12345678\x00",
                right_version: 0,
            },
            Less {
                left: b"12345678",
                left_version: u64::MAX,
                right: b"12345678\x00\x00\x00\x00\x00\x00\x00\x00",
                right_version: 0,
            },
            Less {
                left: b"12345678\x00\x00\x00",
                left_version: 0,
                right: b"12345678\x00\x00\x00\x00",
                right_version: 0,
            },
        ];
        for (idx, t) in tests.iter().enumerate() {
            let left = keys::mvcc_key(0, t.left, t.left_version);
            let right = keys::mvcc_key(0, t.right, t.right_version);
            assert!(left < right, "index {}, left {:?}, right {:?}", idx, left, right);
        }
    }

    #[sekas_macro::test]
    async fn create_and_drop_engine() {
        let dir = TempDir::new(fn_name!()).unwrap();

        let group_id = 1;
        let replica_id = 1;

        // 1. create engine
        let raw_db = {
            let group_engine = create_engine(group_id, replica_id, dir.path()).await;
            group_engine.raw_db.clone()
        };

        // 2. open engine
        let engine =
            GroupEngine::open(&EngineConfig::default(), raw_db.clone(), group_id, replica_id)
                .await
                .unwrap();
        assert!(engine.is_some());

        // 3. drop engine
        GroupEngine::destory(group_id, replica_id, raw_db.clone()).await.unwrap();

        let engine =
            GroupEngine::open(&EngineConfig::default(), raw_db.clone(), group_id, replica_id)
                .await
                .unwrap();
        assert!(engine.is_none());
    }

    #[sekas_macro::test]
    async fn deleted_shard_compaction_filter_is_scoped_to_group_cf() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let db_dir = dir.path().join("db");
        let raw_db = Arc::new(crate::bootstrap::open_engine_with_default_config(db_dir).unwrap());
        let source =
            GroupEngine::create(&EngineConfig::default(), raw_db.clone(), 11, 1001).await.unwrap();
        let target =
            GroupEngine::create(&EngineConfig::default(), raw_db.clone(), 12, 1002).await.unwrap();
        for engine in [&source, &target] {
            engine
                .commit(
                    WriteBatch::default(),
                    WriteStates {
                        descriptor: Some(GroupDesc {
                            id: engine.descriptor().id,
                            shards: vec![ShardDesc::whole(101, 1)],
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    false,
                )
                .unwrap();
        }

        let mut wb = WriteBatch::default();
        source.put(&mut wb, 101, b"k", b"source", 100).unwrap();
        source.commit(wb, WriteStates::default(), false).unwrap();
        let mut wb = WriteBatch::default();
        target.put(&mut wb, 101, b"k", b"target", 100).unwrap();
        target.commit(wb, WriteStates::default(), false).unwrap();

        source.mark_deleted_shard(ShardDesc::whole(101, 1));
        source.raw_db.flush_cf(&source.cf_handle()).unwrap();
        target.raw_db.flush_cf(&target.cf_handle()).unwrap();
        source.raw_db.db.compact_range_cf(&source.cf_handle(), None::<&[u8]>, None::<&[u8]>);
        target.raw_db.db.compact_range_cf(&target.cf_handle(), None::<&[u8]>, None::<&[u8]>);

        assert!(source.get(101, b"k").await.unwrap().is_none());
        let value = target.get(101, b"k").await.unwrap().expect("target value must survive");
        assert_eq!(value.content.as_deref(), Some(b"target".as_slice()));
    }

    #[sekas_macro::test]
    async fn mvcc_iterator() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        let mut snapshot = group_engine.snapshot(1, SnapshotMode::default()).unwrap();
        {
            // key 123456
            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 256);

            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 5);

            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 1);

            assert!(mvcc_iter.next().is_none());
        }

        {
            // key 123456789
            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456789");
            assert_eq!(entry.version(), 0);

            assert!(mvcc_iter.next().is_none());
        }
    }

    #[sekas_macro::test]
    async fn user_key_iterator() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        let mut snapshot = group_engine.snapshot(1, SnapshotMode::default()).unwrap();
        {
            // key 123456
            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456");
            assert_eq!(entry.version(), 256);
        }

        {
            // key 123456789, user_data_iter should skip the iterated keys.
            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let entry = mvcc_iter.next().unwrap().unwrap();
            assert_eq!(entry.user_key(), b"123456789");
            assert_eq!(entry.version(), 0);

            assert!(mvcc_iter.next().is_none());
        }
    }

    #[sekas_macro::test]
    async fn iterate_target_key() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        {
            // Target key `123456`
            let snapshot_mode = SnapshotMode::Key { key: b"123456" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            assert!(snapshot.next().is_some());
            assert!(snapshot.next().is_none());
        }

        {
            // Target key `123456789`
            let snapshot_mode = SnapshotMode::Key { key: b"123456789" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            assert!(snapshot.next().is_some());
            assert!(snapshot.next().is_none());
        }

        {
            // Target to an not existed key
            let snapshot_mode = SnapshotMode::Key { key: b"???" };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            assert!(snapshot.next().is_none());
        }
    }

    #[sekas_macro::test]
    async fn iterate_with_prefix() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123455", version: 1 },
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456789", version: 0 },
            Payload { key: b"123457789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        {
            // Scan with prefix.
            let prefix = b"123456";
            let snapshot_mode = SnapshotMode::Prefix { key: prefix };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let first_key = mvcc_iter.next();
            assert!(matches!(first_key, Some(Ok(entry)) if entry.user_key() == prefix));
            assert!(mvcc_iter.next().is_some());

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            assert!(matches!(mvcc_iter.next(), Some(Ok(entry)) if entry.user_key == b"123456789"));
            assert!(mvcc_iter.next().is_none());
        }

        {
            // Scan with non-exists prefix
            let prefix = b"1234577890";
            let snapshot_mode = SnapshotMode::Prefix { key: prefix };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            assert!(snapshot.next().is_none());
        }

        {
            // Scan with empty prefix should returns all.
            let prefix = b"";
            let snapshot_mode = SnapshotMode::Prefix { key: prefix };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let first_key = mvcc_iter.next();
            assert!(matches!(first_key, Some(Ok(entry)) if entry.user_key() == b"123455"));

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            assert!(matches!(mvcc_iter.next(), Some(Ok(entry)) if entry.user_key == b"123456"));
            assert!(mvcc_iter.next().is_some());

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            assert!(matches!(mvcc_iter.next(), Some(Ok(entry)) if entry.user_key == b"123456789"));

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            assert!(matches!(mvcc_iter.next(), Some(Ok(entry)) if entry.user_key == b"123457789"));
        }
    }

    #[sekas_macro::test]
    async fn iterate_from_start_point() {
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123455", version: 1 },
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456789", version: 0 },
            Payload { key: b"123457789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        {
            // Scan with prefix.
            let prefix = b"123456";
            let snapshot_mode = SnapshotMode::Start { start_key: Some(prefix) };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            let first_key = mvcc_iter.next();
            assert!(matches!(first_key, Some(Ok(entry)) if entry.user_key() == prefix));
            assert!(mvcc_iter.next().is_some());

            let mut mvcc_iter = snapshot.next().unwrap().unwrap();
            assert!(matches!(mvcc_iter.next(), Some(Ok(entry)) if entry.user_key == b"123456789"));
            assert!(mvcc_iter.next().is_none());
        }

        {
            // Scan with non-exists key
            let prefix = b"1234577890";
            let snapshot_mode = SnapshotMode::Start { start_key: Some(prefix) };
            let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
            assert!(snapshot.next().is_none());
        }
    }

    #[sekas_macro::test]
    async fn iterate_in_range() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        group_engine.put(&mut wb, 1, b"a", b"", 123).unwrap();
        group_engine.tombstone(&mut wb, 1, b"a", 124).unwrap();
        group_engine.put(&mut wb, 1, b"b", b"123", 123).unwrap();
        group_engine.put(&mut wb, 1, b"b", b"124", 124).unwrap();
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        // Add new shard
        let wb = WriteBatch::default();
        let states = WriteStates {
            descriptor: Some(GroupDesc {
                id: 1,
                shards: vec![
                    ShardDesc::with_range(1, 1, vec![], vec![b'b']),
                    ShardDesc::with_range(2, 1, vec![b'b'], vec![]),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        group_engine.commit(wb, states, false).unwrap();

        // Iterate shard 1
        let snapshot_mode = SnapshotMode::default();
        let mut snapshot = group_engine.snapshot(1, snapshot_mode).unwrap();
        let mut mvcc_key_iter = snapshot.next().unwrap().unwrap();
        let entry = mvcc_key_iter.next().unwrap().unwrap();
        assert_eq!(entry.user_key(), b"a");
        assert!(snapshot.next().is_none());

        // Iterate shard 2
        let snapshot_mode = SnapshotMode::default();
        let mut snapshot = group_engine.snapshot(2, snapshot_mode).unwrap();
        let mut mvcc_key_iter = snapshot.next().unwrap().unwrap();
        let entry = mvcc_key_iter.next().unwrap().unwrap();
        assert_eq!(entry.user_key(), b"b");
        assert!(snapshot.next().is_none());
    }

    #[sekas_macro::test]
    async fn raw_iterate_all() {
        #[derive(Debug)]
        struct Payload {
            key: &'static [u8],
            version: u64,
        }

        let payloads = vec![
            Payload { key: b"123455", version: 1 },
            Payload { key: b"123456", version: 256 },
            Payload { key: b"123456", version: 5 },
            Payload { key: b"123456", version: 1 },
            Payload { key: b"123456789", version: 0 },
            Payload { key: b"123457789", version: 0 },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;
        let mut wb = WriteBatch::default();
        for payload in &payloads {
            group_engine.put(&mut wb, 1, payload.key, b"", payload.version).unwrap();
        }
        group_engine.commit(wb, WriteStates::default(), false).unwrap();

        let mut iter = group_engine.raw_iter().unwrap();

        // First is the local table datum.
        let (k, _) = iter.next().unwrap().unwrap();
        assert!(k[..] == keys::apply_state());
        let (k, _) = iter.next().unwrap().unwrap();
        assert!(k[..] == keys::descriptor());

        // The the user payloads.
        for payload in &payloads {
            let (k, _) = iter.next().unwrap().unwrap();
            let table_id = group_engine.shard_desc(1).unwrap().table_id;
            let expect_key = keys::mvcc_key(table_id, payload.key, payload.version);
            assert!(
                k[..] == expect_key,
                "expect {expect_key:?}, but got {k:?}, payload {payload:?}",
            );
        }
        assert!(iter.next().is_none());
    }

    #[sekas_macro::test]
    async fn get_latest_version() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let group_engine = create_engine(1, 1, dir.path()).await;

        {
            // Only return the last visible version.
            let mut wb = WriteBatch::default();
            group_engine.put(&mut wb, 1, b"a12345678", b"", 123).unwrap();
            group_engine.tombstone(&mut wb, 1, b"a12345678", 124).unwrap();
            group_engine.put(&mut wb, 1, b"b12345678", b"123", 123).unwrap();
            group_engine.put(&mut wb, 1, b"b12345678", b"124", 124).unwrap();
            group_engine.commit(wb, WriteStates::default(), false).unwrap();

            let v = group_engine.get(1, b"a12345678").await.unwrap();
            assert!(matches!(v, Some(value) if value.content.is_none()));

            let v = group_engine.get(1, b"b12345678").await.unwrap();
            assert!(matches!(v, Some(value) if value.content.is_some()));

            let v = group_engine.get(1, b"c").await.unwrap();
            assert!(v.is_none());
        }

        {
            // Put with old version is not visible.
            let mut wb = WriteBatch::default();
            group_engine.put(&mut wb, 1, b"a12345678", b"123", 122).unwrap();
            group_engine.delete(&mut wb, 1, b"b12345678", 122).unwrap();
            group_engine.commit(wb, WriteStates::default(), false).unwrap();

            let v = group_engine.get(1, b"a12345678").await.unwrap();
            assert!(matches!(v, Some(value) if value.content.is_none()));

            let v = group_engine.get(1, b"b12345678").await.unwrap();
            assert!(matches!(v, Some(value) if value.version == 124 && value.content.is_some()));
        }
    }

    #[sekas_macro::test]
    async fn cf_id_irrelevant_write_batch() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine_1 = create_engine(1, 1, dir.path().join("1").as_path()).await;
        let engine_2 = create_engine(1, 1, dir.path().join("2").as_path()).await;

        // Put in engine 1, commit in engine 2.
        let mut wb = WriteBatch::default();
        engine_1.put(&mut wb, 1, b"a", b"", 123).unwrap();
        engine_1.put(&mut wb, 1, b"b", b"123", 123).unwrap();

        engine_2.commit(wb, WriteStates::default(), false).unwrap();
    }

    #[sekas_macro::test]
    async fn commit_with_write_states() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_engine(1, 1, dir.path().join("1").as_path()).await;

        {
            // with apply state.
            let states = WriteStates {
                apply_state: Some(ApplyState { index: 10, term: 10 }),
                ..Default::default()
            };
            engine.commit(WriteBatch::default(), states, false).unwrap();
        }

        {
            // with move shard state
            let move_shard_state = MoveShardState {
                move_shard: Some(MoveShardDesc {
                    shard_desc: Some(ShardDesc::whole(1, 1)),
                    src_group_id: 1,
                    src_group_epoch: 1,
                    dest_group_id: 2,
                    dest_group_epoch: 2,
                }),
                last_moved_key: None,
                step: MoveShardStep::Prepare.into(),
            };
            let states = WriteStates {
                move_shard_state: Some(move_shard_state.clone()),
                ..Default::default()
            };
            engine.commit(WriteBatch::default(), states, false).unwrap();

            let read_state = engine.move_shard_state();
            assert!(matches!(read_state, Some(state) if state == move_shard_state));
        }
    }

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, 1, key, value, *version).unwrap();
            } else {
                engine.tombstone(&mut wb, 1, key, *version).unwrap();
            }
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    #[sekas_macro::test]
    async fn test_read_shard_all_versions() {
        let cases = vec![
            // empty values.
            vec![],
            // a tombstone.
            vec![Value { version: 1, content: None }],
            // a write.
            vec![Value { version: 1, content: Some(vec![b'1']) }],
            // a write overwrite a tombstone.
            vec![
                Value { version: 2, content: Some(vec![b'1']) },
                Value { version: 1, content: None },
            ],
            // a tombstone overwrite a write.
            vec![
                Value { version: 2, content: None },
                Value { version: 1, content: Some(vec![b'1']) },
            ],
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_engine(1, 1, dir.path().join("1").as_path()).await;
        for (idx, case) in cases.into_iter().enumerate() {
            let key = idx.to_string();
            commit_values(&engine, key.as_bytes(), &case);

            let value_set = engine.get_all_versions(1, key.as_bytes()).await.unwrap();
            assert_eq!(value_set.values, case, "idx = {idx}");
        }
    }

    #[test]
    fn parse_mvcc_key_rejects_local_metadata_key() {
        assert!(!keys::is_mvcc_key(&keys::descriptor()));
        assert!(keys::parse_mvcc_key(&keys::descriptor()).is_none());
    }

    #[test]
    fn is_mvcc_key_is_based_on_table_id() {
        let non_local_prefix_only = 1_u64.to_le_bytes();
        assert!(keys::is_mvcc_key(&non_local_prefix_only));
        assert!(keys::parse_mvcc_key(&non_local_prefix_only).is_none());
    }

    #[test]
    fn parse_mvcc_key_supports_long_user_key() {
        let key = b"123456789";
        let mvcc_key = keys::mvcc_key(1, key, 123);
        assert!(keys::is_mvcc_key(&mvcc_key));
        let parsed = keys::parse_mvcc_key(&mvcc_key).unwrap();
        assert_eq!(parsed.table_id, 1);
        assert_eq!(keys::revert_encoded_user_key(parsed.encoded_user_key), key);
        assert_eq!(parsed.version, 123);
    }

    #[test]
    fn estimate_user_split_key_returns_short_separator_prefix() {
        let estimated_split_keys = vec![
            keys::mvcc_key(1, b"user/00001234", 20),
            keys::mvcc_key(1, b"user/00001234", 10),
            keys::mvcc_key(1, b"user/00005678", 20),
            keys::mvcc_key(1, b"user/00009999", 20),
        ];

        let split_key = estimate_user_split_key(&estimated_split_keys).unwrap();
        assert_eq!(split_key, b"user/00005");
        assert!(b"user/00001234".as_slice() < split_key.as_slice());
        assert!(split_key.as_slice() <= b"user/00005678".as_slice());
    }

    #[test]
    fn estimate_user_split_key_keeps_raw_sample_midpoint() {
        let estimated_split_keys = vec![
            keys::mvcc_key(1, b"user/00001000", 20),
            keys::mvcc_key(1, b"user/00001000", 10),
            keys::mvcc_key(1, b"user/00001000", 5),
            keys::mvcc_key(1, b"user/00002000", 20),
            keys::mvcc_key(1, b"user/00009000", 20),
        ];

        let split_key = estimate_user_split_key(&estimated_split_keys).unwrap();
        assert_eq!(split_key, b"user/00001000");
    }

    #[test]
    fn estimate_user_split_key_falls_back_to_single_user_key() {
        let estimated_split_keys =
            vec![keys::mvcc_key(1, b"user/00001234", 20), keys::mvcc_key(1, b"user/00001234", 10)];

        let split_key = estimate_user_split_key(&estimated_split_keys).unwrap();
        assert_eq!(split_key, b"user/00001234");
    }

    #[test]
    fn shortest_separator_prefix_handles_prefix_relation() {
        let split_key = shortest_separator_prefix(b"abc", b"abcd");
        assert_eq!(split_key, b"abcd");
        assert!(b"abc".as_slice() < split_key.as_slice());
        assert!(split_key.as_slice() <= b"abcd".as_slice());
    }

    #[test]
    fn shortest_separator_prefix_handles_binary_keys() {
        let split_key = shortest_separator_prefix(b"a\xff\xff", b"b\x00");
        assert_eq!(split_key, b"b");
        assert!(b"a\xff\xff".as_slice() < split_key.as_slice());
        assert!(split_key.as_slice() <= b"b\x00".as_slice());
    }

    #[sekas_macro::test]
    async fn estimate_split_key_of_all_range() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let (group_id, shard_id) = (1, 1);
        let engine = create_engine(group_id, shard_id, dir.path().join("1").as_path()).await;

        let split_key = engine.estimate_split_key(shard_id).unwrap();
        assert!(split_key.is_none());

        let mut wb = WriteBatch::default();
        let n = 5000;
        for i in 0..n {
            engine
                .put(
                    &mut wb,
                    shard_id,
                    format!("key-{i:03}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                    i,
                )
                .unwrap();
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
        engine.raw_db.flush_cf(&engine.cf_handle()).unwrap();

        let split_key = engine.estimate_split_key(shard_id).unwrap();
        assert!(split_key.is_some());
    }

    #[sekas_macro::test]
    async fn estimate_split_key_uses_short_separator_prefix() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let (group_id, shard_id) = (1, 1);
        let engine = create_engine(group_id, shard_id, dir.path().join("1").as_path()).await;

        let mut wb = WriteBatch::default();
        let n = 5000;
        for i in 0..n {
            engine
                .put(
                    &mut wb,
                    shard_id,
                    format!("user/0000{i:04}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                    i,
                )
                .unwrap();
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
        engine.raw_db.flush_cf(&engine.cf_handle()).unwrap();

        let split_key = engine.estimate_split_key(shard_id).unwrap().unwrap();
        assert_eq!(split_key, b"user/00003");
        assert!(split_key.len() < b"user/00002048".len());
    }

    #[sekas_macro::test]
    async fn estimate_split_key_in_range() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let (group_id, shard_id) = (1, 1);
        let engine = create_engine_with_range(
            group_id,
            shard_id,
            b"a".to_vec(),
            b"b".to_vec(),
            dir.path().join("1").as_path(),
        )
        .await;

        let split_key = engine.estimate_split_key(shard_id).unwrap();
        assert!(split_key.is_none());

        let mut wb = WriteBatch::default();
        let n = 5000;
        for i in 0..n {
            engine
                .put(
                    &mut wb,
                    shard_id,
                    format!("a-key-{i:03}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                    i,
                )
                .unwrap();
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
        engine.raw_db.flush_cf(&engine.cf_handle()).unwrap();

        let split_key = engine.estimate_split_key(shard_id).unwrap();
        assert!(split_key.is_some());
    }
}
