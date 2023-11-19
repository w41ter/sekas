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

use log::trace;
use prost::Message;
use sekas_api::server::v1::*;
use sekas_schema::system::txn::TXN_INTENT_VERSION;

use super::LatchManager;
use crate::engine::{GroupEngine, SnapshotMode};
use crate::node::migrate::ForwardCtx;
use crate::node::replica::ExecCtx;
use crate::{Error, Result};

/// Get the value of the specified key.
pub(crate) async fn get<T: LatchManager>(
    exec_ctx: &ExecCtx,
    engine: &GroupEngine,
    latch_mgr: &T,
    req: &ShardGetRequest,
) -> Result<Option<Value>> {
    if let Some(desc) = exec_ctx.migration_desc.as_ref() {
        let shard_id = desc.shard_desc.as_ref().unwrap().id;
        if shard_id == req.shard_id {
            let payloads = read_shard_all_versions(engine, req.shard_id, &req.key).await?;
            let forward_ctx = ForwardCtx { shard_id, dest_group_id: desc.dest_group_id, payloads };
            return Err(Error::Forward(forward_ctx));
        }
    }

    trace!("read key {:?} at shard {} with version {}", req.key, req.shard_id, req.start_version);
    read_key(engine, latch_mgr, req.shard_id, &req.key, req.start_version).await
}

async fn read_key<T: LatchManager>(
    engine: &GroupEngine,
    latch_mgr: &T,
    shard_id: u64,
    key: &[u8],
    start_version: u64,
) -> Result<Option<Value>> {
    let snapshot_mode = SnapshotMode::Key { key };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    if let Some(iter) = snapshot.next() {
        for entry in iter? {
            let entry = entry?;
            trace!("read key entry with version: {}", entry.version());
            if entry.version() == TXN_INTENT_VERSION {
                // maybe we need to wait intent.
                let Some(value) = entry.value() else {
                    return Err(Error::InvalidData(format!(
                        "the intent value of key: {key:?} not exists?"
                    )));
                };
                let intent = TxnIntent::decode(value)?;
                if intent.start_version <= start_version {
                    if let Some(value) = latch_mgr
                        .resolve_txn(shard_id, key, start_version, intent.start_version)
                        .await?
                    {
                        if value.version <= start_version {
                            return Ok(Some(value));
                        }
                    }
                }
            } else if entry.version() <= start_version {
                // This entry is safe for reading.
                return Ok(Some(entry.into()));
            }
        }
    }
    Ok(None)
}

async fn read_shard_all_versions(
    engine: &GroupEngine,
    shard_id: u64,
    key: &[u8],
) -> Result<ValueSet> {
    let snapshot_mode = SnapshotMode::Key { key };
    let mut snapshot = engine.snapshot(shard_id, snapshot_mode)?;
    let mut value_set = ValueSet { user_key: key.to_owned(), values: vec![] };
    if let Some(iter) = snapshot.next() {
        for entry in iter? {
            let entry = entry?;
            value_set.values.push(entry.into());
        }
    }
    Ok(value_set)
}

#[cfg(test)]
mod tests {
    use sekas_api::server::v1::Value;
    use sekas_rock::fn_name;
    use tempdir::TempDir;

    use super::*;
    use crate::engine::{create_group_engine, WriteBatch, WriteStates};
    use crate::node::replica::eval;

    #[derive(Default)]
    struct LatchGuard {}

    impl eval::LatchGuard for LatchGuard {
        fn signal_all(&self, txn_state: TxnState, commit_version: Option<u64>) {
            todo!()
        }

        async fn resolve_txn(&mut self, _txn_intent: TxnIntent) -> Result<Option<Value>> {
            todo!()
        }
    }

    #[derive(Default)]
    struct LatchManager {}

    impl eval::LatchManager for LatchManager {
        type Guard = LatchGuard;

        async fn acquire(&self, _shard_id: u64, _key: &[u8]) -> Result<Self::Guard> {
            Ok(LatchGuard {})
        }

        async fn resolve_txn(
            &self,
            _shard_id: u64,
            _user_key: &[u8],
            _start_version: u64,
            _intent_version: u64,
        ) -> Result<Option<Value>> {
            todo!()
        }
    }

    fn commit_values(engine: &GroupEngine, key: &[u8], values: &[Value]) {
        let mut wb = WriteBatch::default();
        for Value { version, content } in values {
            if let Some(value) = content {
                engine.put(&mut wb, 1, key, &value, *version).unwrap();
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
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let mut idx = 0;
        for case in cases {
            let key = idx.to_string();
            commit_values(&engine, key.as_bytes(), &case);

            let value_set = read_shard_all_versions(&engine, 1, key.as_bytes()).await.unwrap();
            assert_eq!(value_set.values, case, "idx = {idx}");

            idx += 1;
        }
    }

    #[sekas_macro::test]
    async fn read_key_without_intent() {
        // read_key should return the first value, including tombstone.
        struct TestCase {
            values: Vec<Value>,
            expect: Option<Value>,
        }
        let cases = vec![
            // empty values.
            TestCase { values: vec![], expect: None },
            // a tombstone.
            TestCase { values: vec![Value::tombstone(1)], expect: Some(Value::tombstone(1)) },
            // a write.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 1)],
                expect: Some(Value::with_value(vec![b'1'], 1)),
            },
            // a write overwrite a tombstone.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 2), Value::tombstone(1)],
                expect: Some(Value::with_value(vec![b'1'], 2)),
            },
            // a tombstone overwrite a write.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 1), Value::tombstone(2)],
                expect: Some(Value::tombstone(2)),
            },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LatchManager::default();
        let mut idx = 0;
        for TestCase { values, expect } in cases {
            let key = idx.to_string();
            commit_values(&engine, key.as_bytes(), &values);

            let got = read_key(&engine, &latch_mgr, 1, key.as_bytes(), 3).await.unwrap();
            assert_eq!(got, expect, "idx = {idx}");

            idx += 1;
        }
    }

    #[sekas_macro::test]
    async fn read_key_with_version_but_without_intent() {
        // read_key should return the first value in the target version, including
        // tombstone.
        struct TestCase {
            values: Vec<Value>,
            expect: Option<Value>,
        }
        let txn_version = 10;
        let cases = vec![
            // empty values.
            TestCase { values: vec![], expect: None },
            // a visible tombstone.
            TestCase { values: vec![Value::tombstone(10)], expect: Some(Value::tombstone(10)) },
            // a non-visible tombstone.
            TestCase { values: vec![Value::tombstone(11)], expect: None },
            // a visible write.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 1)],
                expect: Some(Value::with_value(vec![b'1'], 1)),
            },
            // a non-visible write.
            TestCase { values: vec![Value::with_value(vec![b'1'], 11)], expect: None },
            // a write overwrite a visible tombstone.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 11), Value::tombstone(1)],
                expect: Some(Value::tombstone(1)),
            },
            // a tombstone overwrite a visible write.
            TestCase {
                values: vec![Value::with_value(vec![b'1'], 1), Value::tombstone(20)],
                expect: Some(Value::with_value(vec![b'1'], 1)),
            },
        ];

        let dir = TempDir::new(fn_name!()).unwrap();
        let engine = create_group_engine(dir.path(), 1, 1, 1).await;
        let latch_mgr = LatchManager::default();
        let mut idx = 0;
        for TestCase { values, expect } in cases {
            let key = idx.to_string();
            commit_values(&engine, key.as_bytes(), &values);

            let got = read_key(&engine, &latch_mgr, 1, key.as_bytes(), txn_version).await.unwrap();
            assert_eq!(got, expect, "idx = {idx}");

            idx += 1;
        }
    }
}
