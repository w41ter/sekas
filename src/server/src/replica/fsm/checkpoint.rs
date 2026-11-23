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
use std::path::Path;

use log::debug;
use sekas_api::server::v1::GroupDesc;

use crate::engine::{GroupEngine, PreparedGroupSnapshot, RawIterator};
use crate::raftgroup::SnapshotBuilder;
use crate::serverpb::v1::{ApplyState, SnapshotMeta};
use crate::{Error, ReplicaConfig, Result};

pub struct GroupSnapshotBuilder {
    cfg: ReplicaConfig,
    engine: GroupEngine,
}

impl GroupSnapshotBuilder {
    pub(crate) fn new(cfg: ReplicaConfig, engine: GroupEngine) -> Self {
        GroupSnapshotBuilder { cfg, engine }
    }
}

#[crate::async_trait]
impl SnapshotBuilder for GroupSnapshotBuilder {
    async fn checkpoint(&self, base_dir: &Path) -> Result<(ApplyState, GroupDesc)> {
        std::fs::create_dir_all(base_dir)?;
        let mut iter = self.engine.raw_iter()?;
        for i in 0.. {
            if write_partial_to_file(&self.cfg, &mut iter, base_dir, i).await?.is_none() {
                break;
            }
        }

        let apply_state = *iter.apply_state();
        let descriptor = iter.descriptor().clone();
        Ok((apply_state, descriptor))
    }
}

/// Write partial of the iterator's data to the file, return `None` if all data
/// is written.
async fn write_partial_to_file(
    cfg: &ReplicaConfig,
    iter: &mut RawIterator<'_>,
    base_dir: &Path,
    file_no: usize,
) -> Result<Option<()>> {
    use rocksdb::{Options, SstFileWriter};

    let opts = Options::default();
    let file = base_dir.join(format!("{}.sst", file_no));
    let mut writer: Option<SstFileWriter> = None;
    let mut index = 0;
    for item in iter.by_ref() {
        let (key, value) = item?;
        if writer.is_none() {
            debug!("create sst file: {}", file.display());
            let raw_writer = SstFileWriter::create(&opts);
            raw_writer.open(&file)?;
            writer = Some(raw_writer);
        }

        let writer = writer.as_mut().unwrap();
        writer.put(key, value)?;
        if writer.file_size() >= cfg.snap_file_size {
            break;
        }

        index += 1;
        if index % 1024 == 0 {
            sekas_runtime::yield_now().await;
        }
    }

    if let Some(mut writer) = writer {
        writer.finish()?;
        Ok(Some(()))
    } else {
        Ok(None)
    }
}

pub(crate) fn prepare_snapshot(
    engine: &GroupEngine,
    snap_dir: &Path,
    meta: &SnapshotMeta,
) -> Result<PreparedGroupSnapshot> {
    let (Some(state), Some(desc)) = (meta.apply_state, meta.group_desc.as_ref()) else {
        return Err(Error::InvalidData(format!(
            "incomplete snapshot metadata in {}",
            snap_dir.display()
        )));
    };
    if meta.files.is_empty() {
        return Err(Error::InvalidData(format!("no snapshot SST files in {}", snap_dir.display())));
    }

    let mut files = Vec::with_capacity(meta.files.len());
    for file in &meta.files {
        let relative = Path::new(&file.name);
        if relative.parent() != Some(Path::new("DATA"))
            || relative.extension().is_none_or(|ext| ext != "sst")
        {
            return Err(Error::InvalidData(format!("invalid snapshot SST file {}", file.name)));
        }
        let path = snap_dir.join(relative.file_name().unwrap());
        let metadata = path.metadata()?;
        if !metadata.is_file() || metadata.len() != file.size {
            return Err(Error::InvalidData(format!(
                "incomplete snapshot SST file {}",
                path.display()
            )));
        }
        files.push(path);
    }
    files.sort_unstable();
    engine.prepare_snapshot(files, state, desc)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sekas_api::server::v1::ShardDesc;

    use super::*;
    use crate::EngineConfig;
    use crate::engine::{WriteBatch, WriteStates};
    use crate::testutil::TempDir;

    async fn create_engine(dir: &Path, group_id: u64, shard_id: u64) -> GroupEngine {
        use crate::bootstrap::open_engine_with_default_config;

        let db = open_engine_with_default_config(dir).unwrap();
        let db = Arc::new(db);

        let group_engine =
            GroupEngine::create(&EngineConfig::default(), db.clone(), group_id, 1).await.unwrap();
        let wb = WriteBatch::default();
        let states = WriteStates {
            descriptor: Some(GroupDesc {
                id: group_id,
                shards: vec![ShardDesc::whole(shard_id, 1)],
                ..Default::default()
            }),
            ..Default::default()
        };
        group_engine.commit(wb, states, false).unwrap();

        group_engine
    }

    fn put_data(engine: &GroupEngine, shard_id: u64, prefix: &str, num: usize) {
        let mut wb = WriteBatch::default();
        for i in 0..num {
            let key = format!("{prefix}-{i}");
            let value = vec![0; 1024];
            engine.put(&mut wb, shard_id, key.as_bytes(), &value, 0).unwrap();
        }
        engine.commit(wb, WriteStates::default(), false).unwrap();
    }

    // This test aims to fix bug:
    //
    // `RocksDb(Error { message: "IO error: No such file or directory: While open a
    // file for appending: /tmp/xxx/snap/DATA/0.sst: No such file or directory"
    // })`
    #[sekas_macro::test]
    async fn checkpoint_create_dir() {
        let tmp_dir = TempDir::new("checkpoint_create_dir").unwrap().into_path();
        let db_dir = tmp_dir.join("db");
        let snap_dir = tmp_dir.join("snap");
        let engine = create_engine(&db_dir, 1, 1).await;

        // insert 128KB data.
        put_data(&engine, 1, "key", 128);

        let cfg = ReplicaConfig { snap_file_size: 64 * 1024, ..Default::default() };
        std::fs::create_dir_all(&snap_dir).unwrap();
        let data = snap_dir.join("DATA");
        let builder = GroupSnapshotBuilder::new(cfg, engine);
        builder.checkpoint(&data).await.unwrap();
    }

    // This tests aims to fix bugs:
    //
    // `RocksDb(Error { message: "Invalid argument: external_files[0] is empty" })`
    // `RocksDb(Error { message: "Corruption: file is too short (0 bytes) to be an
    // sstable: /tmp/xxx/snap/DATA/0.sst" })`
    #[sekas_macro::test]
    async fn apply_empty_snapshot() {
        let tmp_dir = TempDir::new("apply_empty_snapshot").unwrap().into_path();
        let db_dir = tmp_dir.join("db");
        let snap_dir = tmp_dir.join("snap");
        let engine = create_engine(&db_dir, 1, 1).await;

        let cfg = ReplicaConfig { snap_file_size: 64 * 1024, ..Default::default() };
        std::fs::create_dir_all(&snap_dir).unwrap();
        let data = snap_dir.join("DATA");
        let builder = GroupSnapshotBuilder::new(cfg, engine.clone());
        let (state, desc) = builder.checkpoint(&data).await.unwrap();
        let meta = SnapshotMeta {
            apply_state: Some(state),
            group_desc: Some(desc),
            files: vec![crate::serverpb::v1::SnapshotFile {
                name: "DATA/0.sst".into(),
                size: data.join("0.sst").metadata().unwrap().len(),
                ..Default::default()
            }],
        };
        let prepared = prepare_snapshot(&engine, &data, &meta).unwrap();
        let old_name = engine.column_family();
        let mut reader = engine.raw_iter().unwrap();
        engine.activate_snapshot(prepared);
        assert_ne!(engine.column_family(), old_name);
        assert_eq!(reader.descriptor().id, 1);
        assert!(reader.next().unwrap().is_ok());
        drop(reader);
    }
}
