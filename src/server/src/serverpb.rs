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

#![allow(clippy::all)]

pub mod v1 {
    use sekas_api::server::v1::{MoveShardDesc, ShardDesc};

    tonic::include_proto!("serverpb.v1");

    pub type ApplyState = EntryId;
    pub type MoveShardEvent = move_shard::Event;

    impl SyncOp {
        #[inline]
        pub fn add_shard(shard: ShardDesc) -> Box<Self> {
            Box::new(SyncOp {
                add_shard: Some(AddShard { shard: Some(shard) }),
                ..Default::default()
            })
        }

        #[inline]
        pub fn purge_replica(orphan_replica_id: u64) -> Box<Self> {
            Box::new(SyncOp {
                purge_replica: Some(PurgeOrphanReplica { replica_id: orphan_replica_id }),
                ..Default::default()
            })
        }

        #[inline]
        pub fn move_shard(event: MoveShardEvent, desc: MoveShardDesc) -> Box<Self> {
            Box::new(SyncOp {
                move_shard: Some(MoveShard {
                    event: event as i32,
                    desc: Some(desc),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
        #[inline]
        pub fn ingest(key: Vec<u8>) -> Box<Self> {
            Box::new(SyncOp {
                move_shard: Some(MoveShard {
                    event: MoveShardEvent::Ingest as i32,
                    last_ingested_key: key,
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
    }

    impl MoveShardState {
        #[inline]
        pub fn get_shard_desc(&self) -> &ShardDesc {
            self.get_move_shard_desc().get_shard_desc()
        }

        #[inline]
        pub fn get_move_shard_desc(&self) -> &MoveShardDesc {
            self.move_shard.as_ref().expect("MoveShardState::move_shard is not None")
        }

        #[inline]
        pub fn get_shard_id(&self) -> u64 {
            self.get_shard_desc().id
        }
    }

    impl From<&raft::eraftpb::Entry> for EntryId {
        fn from(e: &raft::eraftpb::Entry) -> Self {
            EntryId { index: e.index, term: e.term }
        }
    }

    impl EvalResult {
        pub fn with_batch(data: Vec<u8>) -> Self {
            EvalResult { batch: Some(WriteBatchRep { data }), ..Default::default() }
        }
    }
}
