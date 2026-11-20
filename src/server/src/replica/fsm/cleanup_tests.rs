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

use super::*;
use crate::EngineConfig;
use crate::bootstrap::open_engine_with_default_config;
use crate::testutil::TempDir;

struct NoopObserver;

impl StateMachineObserver for NoopObserver {
    fn on_descriptor_updated(&mut self, _: GroupDesc) {}
    fn on_term_updated(&mut self, _: u64) {}
    fn on_move_shard_state_updated(&mut self, _: Option<MoveShardState>) {}
}

#[derive(Clone, Copy, Debug)]
enum Cleanup {
    Abort,
    SourceAbort,
    MoveOut,
    AddDelete,
    SplitLeftDelete,
    SplitRightDelete,
    MergeDelete,
}

fn proposal(op: Box<SyncOp>) -> EvalResult {
    EvalResult { op: Some(op), ..Default::default() }
}

fn apply(fsm: &mut GroupStateMachine, index: u64, result: EvalResult, flush_each: bool) {
    fsm.apply(index, 2, ApplyEntry::Proposal { eval_result: result }).unwrap();
    if flush_each {
        fsm.finish_plug().unwrap();
        fsm.start_plug().unwrap();
    }
}

#[sekas_macro::test]
async fn shard_cleanup_observes_plug_state() {
    let dir = TempDir::new("shard-cleanup").unwrap();
    let db = Arc::new(open_engine_with_default_config(dir.path().join("db")).unwrap());
    // Log payloads are already encoded. Use a separate engine to build them,
    // independent of the receiving engine's committed shard metadata.
    let encoder =
        GroupEngine::create(&EngineConfig::default(), db.clone(), 100, 100).await.unwrap();
    encoder
        .commit(
            WriteBatch::default(),
            WriteStates {
                descriptor: Some(GroupDesc {
                    id: 100,
                    shards: vec![ShardDesc::whole(1, 1), ShardDesc::whole(2, 2)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            true,
        )
        .unwrap();

    let mut replica_id = 21;
    for cleanup in [
        Cleanup::Abort,
        Cleanup::SourceAbort,
        Cleanup::MoveOut,
        Cleanup::AddDelete,
        Cleanup::SplitLeftDelete,
        Cleanup::SplitRightDelete,
        Cleanup::MergeDelete,
    ] {
        for threshold in [0, 128] {
            for flush_each in [false, true] {
                replica_id += 1;
                let group_id =
                    if matches!(cleanup, Cleanup::MoveOut | Cleanup::SourceAbort) { 1 } else { 3 };
                let engine =
                    GroupEngine::create(&EngineConfig::default(), db.clone(), group_id, replica_id)
                        .await
                        .unwrap();
                let target = ShardDesc::with_range(1024, 1, b"b".to_vec(), b"d".to_vec());
                let moving = MoveShardDesc {
                    shard_desc: Some(target.clone()),
                    src_group_id: 1,
                    src_group_epoch: 6,
                    dest_group_id: 3,
                    dest_group_epoch: 8,
                };
                let mut desc = GroupDesc {
                    id: group_id,
                    shards: vec![
                        ShardDesc::with_range(1025, 1, vec![], b"b".to_vec()),
                        ShardDesc::with_range(1026, 1, b"d".to_vec(), vec![]),
                        ShardDesc::whole(1027, 2),
                    ],
                    ..Default::default()
                };
                let (prefix, cleanup_op, cleared) = match cleanup {
                    Cleanup::Abort => (
                        vec![proposal(SyncOp::move_shard(MoveShardEvent::Setup, moving.clone()))],
                        SyncOp::move_shard(MoveShardEvent::Abort, moving),
                        Some(target.clone()),
                    ),
                    Cleanup::SourceAbort | Cleanup::MoveOut => {
                        desc.shards.push(target.clone());
                        let mut prefix = vec![proposal(SyncOp::move_shard(
                            MoveShardEvent::Setup,
                            moving.clone(),
                        ))];
                        let (event, cleared) = if matches!(cleanup, Cleanup::MoveOut) {
                            prefix.push(proposal(SyncOp::move_shard(
                                MoveShardEvent::Commit,
                                moving.clone(),
                            )));
                            (MoveShardEvent::Apply, Some(target.clone()))
                        } else {
                            (MoveShardEvent::Abort, None)
                        };
                        (prefix, SyncOp::move_shard(event, moving), cleared)
                    }
                    Cleanup::AddDelete => (
                        vec![proposal(Box::new(SyncOp {
                            add_shard: Some(AddShard { shard: Some(target.clone()) }),
                            ..Default::default()
                        }))],
                        SyncOp::delete_shard(1024),
                        Some(target.clone()),
                    ),
                    Cleanup::SplitLeftDelete | Cleanup::SplitRightDelete => {
                        desc.shards.push(ShardDesc::with_range(
                            1024,
                            1,
                            b"b".to_vec(),
                            b"f".to_vec(),
                        ));
                        desc.shard_mut(1026).unwrap().range.as_mut().unwrap().start = b"f".to_vec();
                        let cleared = if matches!(cleanup, Cleanup::SplitLeftDelete) {
                            target.clone()
                        } else {
                            ShardDesc::with_range(1028, 1, b"d".to_vec(), b"f".to_vec())
                        };
                        (
                            vec![proposal(Box::new(SyncOp {
                                split_shard: Some(SplitShard {
                                    old_shard_id: 1024,
                                    new_shard_id: 1028,
                                    split_key: b"d".to_vec(),
                                }),
                                ..Default::default()
                            }))],
                            SyncOp::delete_shard(cleared.id),
                            Some(cleared),
                        )
                    }
                    Cleanup::MergeDelete => {
                        desc.shards.push(ShardDesc::with_range(
                            1024,
                            1,
                            b"b".to_vec(),
                            b"c".to_vec(),
                        ));
                        desc.shards.push(ShardDesc::with_range(
                            1028,
                            1,
                            b"c".to_vec(),
                            b"d".to_vec(),
                        ));
                        (
                            vec![proposal(Box::new(SyncOp {
                                merge_shard: Some(MergeShard {
                                    left_shard_id: 1024,
                                    right_shard_id: 1028,
                                }),
                                ..Default::default()
                            }))],
                            SyncOp::delete_shard(1024),
                            Some(target.clone()),
                        )
                    }
                };
                let mut disk = WriteBatch::default();
                for key in [b"a".as_slice(), b"aa", b"b", b"c", b"d", b"e", b"f", b"z"] {
                    if desc.shards.iter().any(|shard| {
                        shard.table_id == 1 && sekas_schema::shard::belong_to(shard, key)
                    }) {
                        encoder.put(&mut disk, 1, key, b"disk", 1).unwrap();
                    }
                }
                encoder.put(&mut disk, 2, b"b", b"other-table", 1).unwrap();
                engine
                    .commit(
                        disk,
                        WriteStates { descriptor: Some(desc), ..Default::default() },
                        true,
                    )
                    .unwrap();

                let info = Arc::new(ReplicaInfo::new(
                    &ReplicaDesc { id: replica_id, ..Default::default() },
                    group_id,
                    ReplicaLocalState::Normal,
                ));
                let (_, receiver) = mpsc::channel();
                let mut fsm = GroupStateMachine::new(
                    ReplicaConfig { delete_shard_range_threshold: threshold, ..Default::default() },
                    info,
                    engine.clone(),
                    Box::new(NoopObserver),
                    WatchHub::new(receiver),
                );
                fsm.start_plug().unwrap();
                // An earlier cleanup's range-only batch must survive the filter
                // and must not be mistaken for an empty batch.
                let (start, end) = GroupEngine::shard_raw_range(&ShardDesc::with_range(
                    0,
                    1,
                    b"z".to_vec(),
                    b"zz".to_vec(),
                ))
                .unwrap();
                let mut previous_delete = WriteBatch::default();
                previous_delete.delete_range(start, end);
                fsm.plugged_write_batches.push(previous_delete);
                let mut index = 0;
                for entry in prefix {
                    index += 1;
                    apply(&mut fsm, index, entry, flush_each);
                }
                if !flush_each
                    && matches!(
                        cleanup,
                        Cleanup::Abort | Cleanup::AddDelete | Cleanup::SplitRightDelete
                    )
                {
                    let id = cleared.as_ref().unwrap().id;
                    assert!(matches!(engine.shard_desc(id), Err(Error::ShardNotFound(_))));
                }
                let mut pending = WriteBatch::default();
                for key in [b"aa".as_slice(), b"b", b"c", b"d", b"e", b"f"] {
                    encoder.put(&mut pending, 1, key, b"plug", 2).unwrap();
                }
                encoder.put(&mut pending, 1, b"b", b"intent", u64::MAX).unwrap();
                encoder.tombstone(&mut pending, 1, b"c", 3).unwrap();
                encoder.delete(&mut pending, 1, b"a", 1).unwrap();
                let mut entries = vec![
                    EvalResult {
                        batch: Some(WriteBatchRep { data: pending.data().to_vec() }),
                        op: if matches!(cleanup, Cleanup::Abort | Cleanup::SourceAbort) {
                            Some(SyncOp::ingest(b"c".to_vec()))
                        } else {
                            None
                        },
                    },
                    proposal(cleanup_op),
                ];
                if let Some(shard) = &cleared {
                    // A later recreation/write to this range must survive.
                    entries.push(proposal(Box::new(SyncOp {
                        add_shard: Some(AddShard { shard: Some(shard.clone()) }),
                        ..Default::default()
                    })));
                    let mut later = WriteBatch::default();
                    encoder
                        .put(&mut later, 1, &shard.range.as_ref().unwrap().start, b"later", 7)
                        .unwrap();
                    entries.push(EvalResult {
                        batch: Some(WriteBatchRep { data: later.data().to_vec() }),
                        ..Default::default()
                    });
                }
                for entry in entries {
                    index += 1;
                    apply(&mut fsm, index, entry, flush_each);
                }
                if !flush_each {
                    fsm.finish_plug().unwrap();
                }
                // finish_plug disables WAL; flushed_apply_state reads only the
                // persisted tier, so flush before checking the durable index.
                db.flush_cf(&db.cf_handle(&format!("{group_id}-{replica_id}")).unwrap()).unwrap();
                assert_eq!(engine.flushed_apply_state().unwrap(), ApplyState { index, term: 2 });
                assert!(engine.move_shard_state().is_none());
                let final_desc = engine.descriptor();
                for key in [b"aa".as_slice(), b"b", b"c", b"d", b"e", b"f"] {
                    let shard = final_desc
                        .shards
                        .iter()
                        .find(|shard| {
                            shard.table_id == 1 && sekas_schema::shard::belong_to(shard, key)
                        })
                        .unwrap();
                    let mut expected = if cleared
                        .as_ref()
                        .is_some_and(|shard| sekas_schema::shard::belong_to(shard, key))
                    {
                        if key == cleared.as_ref().unwrap().range.as_ref().unwrap().start {
                            vec![Value::with_value(b"later".to_vec(), 7)]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![
                            Value::with_value(b"plug".to_vec(), 2),
                            Value::with_value(b"disk".to_vec(), 1),
                        ]
                    };
                    if !cleared
                        .as_ref()
                        .is_some_and(|shard| sekas_schema::shard::belong_to(shard, key))
                    {
                        if key == b"b" {
                            expected.insert(0, Value::with_value(b"intent".to_vec(), u64::MAX));
                        }
                        if key == b"c" {
                            expected.insert(0, Value::tombstone(3));
                        }
                    }
                    assert_eq!(
                        engine.get_all_versions(shard.id, key).await.unwrap().values,
                        expected,
                        "{cleanup:?}, threshold={threshold}, flush_each={flush_each}, key={key:?}"
                    );
                }
                assert!(engine.get(1025, b"a").await.unwrap().is_none());
                assert!(engine.get(1026, b"z").await.unwrap().is_none());
                assert_eq!(
                    engine.get(1027, b"b").await.unwrap(),
                    Some(Value::with_value(b"other-table".to_vec(), 1))
                );
            }
        }
    }
}
