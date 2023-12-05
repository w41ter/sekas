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
use sekas_api::server::v1::ValueSet;

use crate::engine::{GroupEngine, WriteBatch};
use crate::serverpb::v1::{EvalResult, WriteBatchRep};
use crate::Result;

pub async fn ingest_value_set(
    engine: &GroupEngine,
    shard_id: u64,
    value_set: &ValueSet,
) -> Result<Option<EvalResult>> {
    // TODO(walter) assert row lock is hold.
    if value_set.values.is_empty() {
        return Ok(None);
    }

    if engine.get(shard_id, &value_set.user_key).await?.is_some() {
        return Ok(None);
    };

    let mut wb = WriteBatch::default();
    for value in &value_set.values {
        if let Some(content) = value.content.as_ref() {
            engine.put(&mut wb, shard_id, &value_set.user_key, content, value.version)?;
        } else {
            engine.tombstone(&mut wb, shard_id, &value_set.user_key, value.version)?;
        }
    }

    let eval_result = EvalResult {
        batch: Some(WriteBatchRep { data: wb.data().to_vec() }),
        ..Default::default()
    };
    Ok(Some(eval_result))
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::Value;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{create_group_engine, WriteStates};

    const SHARD_ID: u64 = 1;

    #[sekas_macro::test]
    async fn cmd_ingest_value_set_when_no_former_value_exists() {
        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;

        let value_set = ValueSet { user_key: vec![1, 2, 3, 4], values: vec![Value::tombstone(1)] };
        let result = ingest_value_set(&engine, SHARD_ID, &value_set).await.unwrap();
        assert!(result.is_some());
        let eval_result = result.unwrap();
        let wb = WriteBatch::new(&eval_result.batch.unwrap().data);
        engine.commit(wb, WriteStates::default(), false).unwrap();

        // Ignore if the user key already exists.
        let value_set = ValueSet { user_key: vec![1, 2, 3, 4], values: vec![Value::tombstone(1)] };
        let result = ingest_value_set(&engine, SHARD_ID, &value_set).await.unwrap();
        assert!(result.is_none());
    }
}
