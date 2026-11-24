// Copyright 2024-present The Sekas Authors.
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

use sekas_api::server::v1::{MoveShardDesc, ShardDesc};

/// Root's durable placement intent. Groups expose business state for recovery.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PlacementJob {
    #[prost(oneof = "Action", tags = "1, 2, 3, 4, 5")]
    pub action: Option<Action>,
    #[prost(string, tag = "7")]
    pub reason: String,
    #[prost(string, optional, tag = "8")]
    pub policy: Option<String>,
    #[prost(uint64, tag = "9")]
    pub created_at: u64,
    /// A terminal result removes the job from the active set in the same write.
    #[prost(message, optional, tag = "10")]
    pub outcome: Option<Outcome>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Action {
    #[prost(message, tag = "1")]
    MoveShard(MoveShard),
    #[prost(message, tag = "2")]
    TransferLeader(TransferLeader),
    #[prost(message, tag = "3")]
    MoveReplica(MoveReplica),
    #[prost(message, tag = "4")]
    SplitShard(SplitShard),
    #[prost(message, tag = "5")]
    MergeShards(MergeShards),
}

/// Once saved, the request may already have reached the group. Retries preserve
/// these admission conditions until the effect or the fence has been observed.
#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct GroupVersion {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    #[prost(uint64, tag = "2")]
    pub term: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Outcome {
    #[prost(enumeration = "OutcomeKind", tag = "1")]
    pub kind: i32,
    #[prost(string, tag = "2")]
    pub detail: String,
    #[prost(uint64, tag = "3")]
    pub finished_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
pub enum OutcomeKind {
    Succeeded = 0,
    Cancelled = 1,
    Failed = 2,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MoveShard {
    #[prost(message, required, tag = "1")]
    pub desc: MoveShardDesc,
    #[prost(uint64, tag = "2")]
    pub target_term: u64,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct TransferLeader {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(uint64, tag = "2")]
    pub source: u64,
    #[prost(uint64, tag = "3")]
    pub target: u64,
    #[prost(uint64, optional, tag = "4")]
    pub draining_node: Option<u64>,
    #[prost(message, required, tag = "5")]
    pub version: GroupVersion,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct MoveReplica {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(uint64, tag = "2")]
    pub outgoing: u64,
    #[prost(uint64, tag = "3")]
    pub target_node: u64,
    #[prost(message, optional, tag = "4")]
    pub learner: Option<LearnerAdmission>,
}

#[derive(Clone, Copy, PartialEq, prost::Message)]
pub struct LearnerAdmission {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Group epoch at which the fixed ID may be added to the configuration.
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SplitShard {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(uint64, tag = "2")]
    pub epoch: u64,
    #[prost(message, required, tag = "3")]
    pub shard: ShardDesc,
    #[prost(uint64, optional, tag = "4")]
    pub target_bytes: Option<u64>,
    #[prost(message, optional, tag = "5")]
    pub prepared: Option<PreparedSplit>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct PreparedSplit {
    #[prost(uint64, tag = "1")]
    pub new_shard: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub key: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub term: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct MergeShards {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(message, required, tag = "2")]
    pub left: ShardDesc,
    #[prost(message, required, tag = "3")]
    pub right: ShardDesc,
    #[prost(message, required, tag = "4")]
    pub version: GroupVersion,
}
