// Copyright 2023-present The Sekas Authors.
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
use sekas_api::server::v1::{MoveShardDesc, ValueSet};

use crate::Result;
use crate::engine::{GroupEngine, WriteBatch};

/// Stage source history and report whether this is a key's first import.
pub fn ingest_value_set(
    engine: &GroupEngine,
    desc: &MoveShardDesc,
    value_set: &ValueSet,
    wb: &mut WriteBatch,
) -> Result<bool> {
    if engine.pull_covers_key(&value_set.user_key)
        || engine.is_migrated_key(desc.dest_group_epoch, &value_set.user_key)?
    {
        return Ok(false);
    }

    let shard_id = desc.get_shard_id();
    for value in &value_set.values {
        if let Some(content) = value.content.as_ref() {
            engine.put(wb, shard_id, &value_set.user_key, content, value.version)?;
        } else {
            engine.tombstone(wb, shard_id, &value_set.user_key, value.version)?;
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::{ShardDesc, Value};
    use sekas_rock::fn_name;

    use super::*;
    use crate::engine::{WriteStates, create_group_engine};
    use crate::testutil::TempDir;

    const SHARD_ID: u64 = 1;

    #[sekas_macro::test]
    async fn cmd_ingest_value_set_skips_marked_key() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let desc =
            MoveShardDesc { shard_desc: Some(ShardDesc::whole(SHARD_ID, 1)), ..Default::default() };

        let value_set = ValueSet { user_key: vec![1, 2, 3, 4], values: vec![Value::tombstone(1)] };
        let mut wb = WriteBatch::default();
        let ingested = ingest_value_set(&engine, &desc, &value_set, &mut wb).unwrap();
        assert!(ingested);
        wb.mark_migrated_key(desc.dest_group_epoch, &value_set.user_key);
        let wb = WriteBatch::new(wb.data());
        engine.commit(wb, WriteStates::default(), false).unwrap();

        // Ignore a key that has already been imported.
        let value_set = ValueSet { user_key: vec![1, 2, 3, 4], values: vec![Value::tombstone(1)] };
        let mut wb = WriteBatch::default();
        let ingested = ingest_value_set(&engine, &desc, &value_set, &mut wb).unwrap();
        assert!(!ingested);
    }
}
