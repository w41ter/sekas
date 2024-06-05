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

mod group;
mod options;
mod properties;
mod state;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::info;
use sekas_rock::fs::create_dir_all_if_not_exists;

pub(crate) use self::group::{
    GroupEngine, MvccEntry, MvccIterator, RawIterator, Snapshot, SnapshotMode, WriteBatch,
    WriteStates,
};
pub(crate) use self::state::StateEngine;
use crate::{DbConfig, Result};

// The disk layouts.
const LAYOUT_DATA: &str = "db";
const LAYOUT_LOG: &str = "log";
const LAYOUT_SNAP: &str = "snap";

type DbResult<T, E = rocksdb::Error> = Result<T, E>;

pub(crate) struct RawDb {
    pub options: rocksdb::Options,
    pub db: rocksdb::DB,
}

impl RawDb {
    #[inline]
    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(name)
    }

    #[inline]
    pub fn create_cf<N: AsRef<str>>(&self, name: N) -> DbResult<()> {
        self.db.create_cf(name, &self.options)
    }

    #[inline]
    pub fn drop_cf(&self, name: &str) -> DbResult<()> {
        self.db.drop_cf(name)
    }

    #[inline]
    pub fn flush_cf(&self, cf: &impl rocksdb::AsColumnFamilyRef) -> DbResult<()> {
        self.db.flush_cf(cf)
    }

    #[inline]
    pub fn write_opt(
        &self,
        batch: rocksdb::WriteBatch,
        writeopts: &rocksdb::WriteOptions,
    ) -> DbResult<()> {
        self.db.write_opt(batch, writeopts)
    }

    #[inline]
    pub fn get_pinned_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
    ) -> DbResult<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf(cf, key)
    }

    #[inline]
    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        key: K,
        readopts: &rocksdb::ReadOptions,
    ) -> DbResult<Option<rocksdb::DBPinnableSlice>> {
        self.db.get_pinned_cf_opt(cf, key, readopts)
    }

    #[inline]
    pub fn iterator_cf_opt<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl rocksdb::AsColumnFamilyRef,
        readopts: rocksdb::ReadOptions,
        mode: rocksdb::IteratorMode,
    ) -> rocksdb::DBIteratorWithThreadMode<'b, rocksdb::DB> {
        self.db.iterator_cf_opt(cf_handle, readopts, mode)
    }

    #[inline]
    pub fn ingest_external_file_cf_opts<P: AsRef<Path>>(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        opts: &rocksdb::IngestExternalFileOptions,
        paths: Vec<P>,
    ) -> DbResult<()> {
        self.db.ingest_external_file_cf_opts(cf, opts, paths)
    }

    /// Estimate the split keys in the target range.
    #[inline]
    pub fn estimate_split_keys_in_range(
        &self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        start: &[u8],
        end: &[u8],
    ) -> DbResult<Vec<Vec<u8>>, crate::Error> {
        use properties::{EstimatedSplitKeys, PROPERTY_SPLIT_KEYS};
        use prost::Message;

        let collection = if end.is_empty() {
            self.db.get_properties_of_all_range(cf)?
        } else {
            self.db.get_properties_of_tables_in_range(cf, &[(start, end)])?
        };
        let mut split_keys = BTreeSet::default();
        for table in collection.tables {
            let properties = table.user_collected_properties();
            if let Some(value) = properties.get(PROPERTY_SPLIT_KEYS) {
                let table_split_keys = EstimatedSplitKeys::decode(&**value).map_err(|err| {
                    crate::Error::InvalidData(format!("deserialize EstimatedSplitKeys: {err}"))
                })?;
                for key in table_split_keys.keys {
                    if start < key.as_slice() && (key.as_slice() < end || end.is_empty()) {
                        split_keys.insert(key);
                    }
                }
            }
        }
        Ok(split_keys.into_iter().collect::<Vec<_>>())
    }
}

#[derive(Clone)]
pub(crate) struct Engines {
    log_path: PathBuf,
    _db_path: PathBuf,
    log: Arc<raft_engine::Engine>,
    db: Arc<RawDb>,
    state: StateEngine,
}

impl Engines {
    pub(crate) fn open(root_dir: &Path, db_cfg: &DbConfig) -> Result<Self> {
        let db_path = root_dir.join(LAYOUT_DATA);
        let log_path = root_dir.join(LAYOUT_LOG);
        let db = Arc::new(open_raw_db(db_cfg, &db_path)?);
        let log = Arc::new(open_raft_engine(&log_path)?);
        let state = StateEngine::new(log.clone());
        Ok(Engines { log_path, _db_path: db_path, log, db, state })
    }

    #[inline]
    pub(crate) fn log(&self) -> Arc<raft_engine::Engine> {
        self.log.clone()
    }

    #[inline]
    pub(crate) fn db(&self) -> Arc<RawDb> {
        self.db.clone()
    }

    #[inline]
    pub(crate) fn state(&self) -> StateEngine {
        self.state.clone()
    }

    #[inline]
    pub(crate) fn snap_dir(&self) -> PathBuf {
        self.log_path.join(LAYOUT_SNAP)
    }
}

pub(crate) fn open_raw_db<P: AsRef<Path>>(cfg: &DbConfig, path: P) -> Result<RawDb> {
    use rocksdb::DB;

    std::fs::create_dir_all(&path)?;
    let options = options::to_rocksdb_options(cfg);

    // List column families and open database with column families.
    match DB::list_cf(&options, &path) {
        Ok(cfs) => {
            info!("open local db {} with {} column families", path.as_ref().display(), cfs.len());
            let db = DB::open_cf_with_opts(
                &options,
                path,
                cfs.into_iter().map(|name| (name, options.clone())),
            )?;
            Ok(RawDb { db, options })
        }
        Err(e) => {
            if e.as_ref().ends_with("CURRENT: No such file or directory") {
                info!("create new local db: {}", path.as_ref().display());
                let db = DB::open(&options, &path)?;
                Ok(RawDb { db, options })
            } else {
                Err(e.into())
            }
        }
    }
}

pub(crate) fn open_raft_engine(log_path: &Path) -> Result<raft_engine::Engine> {
    use raft_engine::{Config, Engine};
    let engine_dir = log_path.join("engine");
    let snap_dir = log_path.join("snap");
    create_dir_all_if_not_exists(&engine_dir)?;
    create_dir_all_if_not_exists(&snap_dir)?;
    let engine_cfg = Config {
        dir: engine_dir.to_str().unwrap().to_owned(),
        enable_log_recycle: false,
        ..Default::default()
    };
    Ok(Engine::open(engine_cfg)?)
}

/// A helper function to create [`GroupEngine`].
#[cfg(test)]
pub async fn create_group_engine(
    dir: &Path,
    group_id: u64,
    shard_id: u64,
    replica_id: u64,
) -> GroupEngine {
    use sekas_api::server::v1::*;

    use crate::EngineConfig;

    const TABLE_ID: u64 = 1;
    let db = Arc::new(open_raw_db(&DbConfig::default(), dir).unwrap());

    let group_engine =
        GroupEngine::create(&EngineConfig::default(), db.clone(), group_id, replica_id)
            .await
            .unwrap();
    let wb = WriteBatch::default();
    let states = WriteStates {
        descriptor: Some(GroupDesc {
            id: group_id,
            shards: vec![ShardDesc::whole(shard_id, TABLE_ID)],
            ..Default::default()
        }),
        ..Default::default()
    };
    group_engine.commit(wb, states, false).unwrap();
    group_engine
}

#[cfg(test)]
mod tests {
    use raft_engine::LogBatch;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn reopen_raw_db_with_families() {
        let dir = TempDir::new(fn_name!()).unwrap();

        {
            // Create a lots column families.
            let db = open_raw_db(&DbConfig::default(), dir.path()).unwrap();
            db.create_cf("cf1").unwrap();
            db.create_cf("cf2").unwrap();
            db.create_cf("cf3").unwrap();
            db.drop_cf("cf3").unwrap();
        }

        {
            // Reopen db with columns.
            let db = open_raw_db(&DbConfig::default(), dir.path()).unwrap();
            assert!(db.cf_handle("cf1").is_some());
            assert!(db.cf_handle("cf2").is_some());
            assert!(db.cf_handle("cf3").is_none());
        }
    }

    #[test]
    fn estimate_split_keys() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let db = open_raw_db(&DbConfig::default(), dir.path()).unwrap();
        db.create_cf("cf1").unwrap();
        let cf_handle = db.cf_handle("cf1").unwrap();
        let mut wb = rocksdb::WriteBatch::default();
        let n = 5000;
        for i in 0..n {
            wb.put_cf(
                &cf_handle,
                format!("key-{i:03}").as_bytes(),
                format!("value-{i}").as_bytes(),
            );
        }
        let mut opt = rocksdb::WriteOptions::default();
        opt.set_sync(false);
        db.write_opt(wb, &opt).unwrap();
        db.flush_cf(&cf_handle).unwrap();

        // A sub range.
        let split_keys = db
            .estimate_split_keys_in_range(
                &cf_handle,
                format!("key-{:03}", 0).as_bytes(),
                format!("key-{:03}", n - 100).as_bytes(),
            )
            .unwrap();
        assert!(!split_keys.is_empty());

        // A inf range
        let split_keys = db.estimate_split_keys_in_range(&cf_handle, &[], &[]).unwrap();
        assert!(!split_keys.is_empty());

        // An empty range
        let split_keys = db
            .estimate_split_keys_in_range(&cf_handle, "key".as_bytes(), "key-0000".as_bytes())
            .unwrap();
        assert!(split_keys.is_empty());
    }

    #[test]
    fn reopen_raft_engine() {
        let dir = TempDir::new(fn_name!()).unwrap();

        {
            let engine = open_raft_engine(dir.path()).unwrap();
            let mut batch = LogBatch::default();
            batch.put(1, vec![1, 2, 3], vec![4, 5, 6]);
            engine.write(&mut batch, true).unwrap();
        }

        {
            let engine = open_raft_engine(dir.path()).unwrap();
            let result = engine.get(1, &[1, 2, 3]);
            assert!(matches!(result, Some(x) if x == vec![4, 5, 6]));
            let result = engine.get(1, &[4, 5, 6]);
            assert!(result.is_none());
        }
    }
}
