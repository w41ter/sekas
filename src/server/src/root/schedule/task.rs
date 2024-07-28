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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReconcileTask {
    #[prost(uint64, tag = "128")]
    pub created_at: u64,
    #[prost(uint64, tag = "129")]
    pub fire_at: u64,
    #[prost(oneof = "reconcile_task::Task", tags = "1, 2, 3, 4, 5")]
    pub task: ::core::option::Option<reconcile_task::Task>,
}

/// Nested message and enum types in `ReconcileTask`.
pub mod reconcile_task {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Task {
        #[prost(message, tag = "1")]
        ReallocateReplica(super::ReallocateReplicaTask),
        #[prost(message, tag = "2")]
        MigrateShard(super::MigrateShardTask),
        #[prost(message, tag = "3")]
        TransferGroupLeader(super::TransferGroupLeaderTask),
        #[prost(message, tag = "4")]
        ShedLeader(super::ShedLeaderTask),
        #[prost(message, tag = "5")]
        ShedRoot(super::ShedRootLeaderTask),
        #[prost(message, tag = "6")]
        SplitShard(super::SplitShardTask),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReallocateReplicaTask {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(uint64, tag = "2")]
    pub src_node: u64,
    #[prost(uint64, tag = "3")]
    pub src_replica: u64,
    #[prost(message, optional, tag = "4")]
    pub dest_node: ::core::option::Option<::sekas_api::server::v1::NodeDesc>,
    #[prost(message, optional, tag = "5")]
    pub dest_replica: ::core::option::Option<::sekas_api::server::v1::ReplicaDesc>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MigrateShardTask {
    #[prost(uint64, tag = "1")]
    pub shard: u64,
    #[prost(uint64, tag = "2")]
    pub src_group: u64,
    #[prost(uint64, tag = "3")]
    pub dest_group: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransferGroupLeaderTask {
    #[prost(uint64, tag = "1")]
    pub group: u64,
    #[prost(uint64, tag = "2")]
    pub target_replica: u64,
    #[prost(uint64, tag = "3")]
    pub src_node: u64,
    #[prost(uint64, tag = "4")]
    pub dest_node: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShedLeaderTask {
    #[prost(uint64, tag = "1")]
    pub node_id: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShedRootLeaderTask {
    #[prost(uint64, tag = "1")]
    pub node_id: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SplitShardTask {
    #[prost(uint64, tag = "1")]
    pub shard_id: u64,
    #[prost(uint64, tag = "2")]
    pub group_id: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BackgroundJob {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(oneof = "background_job::Job", tags = "2, 3, 4, 5")]
    pub job: ::core::option::Option<background_job::Job>,
}

impl BackgroundJob {
    pub fn to_json(&self) -> serde_json::Value {
        use background_job::Job;
        use serde_json::json;

        match self.job.as_ref().unwrap() {
            Job::CreateTable(c) => {
                let state = format!("{:?}", CreateTableJobStatus::from_i32(c.status).unwrap());
                let wait_create = c.wait_create.len();
                let wait_cleanup = c.wait_cleanup.len();
                json!({
                    "type": "create table",
                    "name": c.table_name,
                    "status": state,
                    "wait_create": wait_create,
                    "wait_cleanup": wait_cleanup,
                })
            }
            Job::CreateOneGroup(c) => {
                let status = format!("{:?}", CreateOneGroupStatus::from_i32(c.status).unwrap());
                let wait_create = c.wait_create.len();
                let wait_cleanup = c.wait_cleanup.len();
                let retired = c.create_retry;
                let group_id = c.group_desc.as_ref().map(|g| g.id).unwrap_or_default();
                json!({
                    "type": "create group",
                    "status": status,
                    "replica_count": c.request_replica_cnt,
                    "wait_create": wait_create,
                    "wait_cleanup": wait_cleanup,
                    "retry_count": retired,
                    "group_id": group_id,
                })
            }
            Job::PurgeTable(p) => {
                json!({
                    "type": "purge table",
                    "database": p.database_id,
                    "table": p.table_id,
                    "name": p.table_name,
                })
            }
            Job::PurgeDatabase(p) => {
                json!({
                    "type": "purge database",
                    "database": p.database_id,
                })
            }
        }
    }
}

/// Nested message and enum types in `BackgroundJob`.
pub mod background_job {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Job {
        #[prost(message, tag = "2")]
        CreateTable(super::CreateTableJob),
        #[prost(message, tag = "3")]
        CreateOneGroup(super::CreateOneGroupJob),
        #[prost(message, tag = "4")]
        PurgeTable(super::PurgeTableJob),
        #[prost(message, tag = "5")]
        PurgeDatabase(super::PurgeDatabaseJob),
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateTableJob {
    #[prost(uint64, tag = "1")]
    pub database: u64,
    #[prost(string, tag = "2")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub wait_create: ::prost::alloc::vec::Vec<::sekas_api::server::v1::ShardDesc>,
    #[prost(message, repeated, tag = "4")]
    pub wait_cleanup: ::prost::alloc::vec::Vec<::sekas_api::server::v1::ShardDesc>,
    #[prost(enumeration = "CreateTableJobStatus", tag = "5")]
    pub status: i32,
    #[prost(string, tag = "6")]
    pub remark: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "7")]
    pub desc: ::core::option::Option<::sekas_api::server::v1::TableDesc>,
    #[prost(string, tag = "89")]
    pub created_time: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateOneGroupJob {
    #[prost(uint64, tag = "1")]
    pub request_replica_cnt: u64,
    #[prost(message, optional, tag = "2")]
    pub group_desc: ::core::option::Option<::sekas_api::server::v1::GroupDesc>,
    #[prost(message, repeated, tag = "3")]
    pub wait_create: ::prost::alloc::vec::Vec<::sekas_api::server::v1::NodeDesc>,
    #[prost(message, repeated, tag = "4")]
    pub wait_cleanup: ::prost::alloc::vec::Vec<::sekas_api::server::v1::ReplicaDesc>,
    #[prost(enumeration = "CreateOneGroupStatus", tag = "5")]
    pub status: i32,
    #[prost(uint64, tag = "6")]
    pub create_retry: u64,
    #[prost(uint64, repeated, tag = "7")]
    pub invoked_nodes: ::prost::alloc::vec::Vec<u64>,
    #[prost(string, tag = "8")]
    pub created_time: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PurgeTableJob {
    #[prost(uint64, tag = "1")]
    pub database_id: u64,
    #[prost(uint64, tag = "2")]
    pub table_id: u64,
    #[prost(string, tag = "3")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub table_name: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub created_time: ::prost::alloc::string::String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PurgeDatabaseJob {
    #[prost(uint64, tag = "1")]
    pub database_id: u64,
    #[prost(string, tag = "2")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub created_time: ::prost::alloc::string::String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TaskStep {
    Initialized = 0,
    CreateReplica = 1,
    AddLearner = 2,
    ReplaceVoter = 3,
    RemoveLearner = 4,
}

impl TaskStep {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic
    /// use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TaskStep::Initialized => "INITIALIZED",
            TaskStep::CreateReplica => "CREATE_REPLICA",
            TaskStep::AddLearner => "ADD_LEARNER",
            TaskStep::ReplaceVoter => "REPLACE_VOTER",
            TaskStep::RemoveLearner => "REMOVE_LEARNER",
        }
    }

    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INITIALIZED" => Some(Self::Initialized),
            "CREATE_REPLICA" => Some(Self::CreateReplica),
            "ADD_LEARNER" => Some(Self::AddLearner),
            "REPLACE_VOTER" => Some(Self::ReplaceVoter),
            "REMOVE_LEARNER" => Some(Self::RemoveLearner),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CreateTableJobStatus {
    Creating = 0,
    Rollbacking = 1,
    WriteDesc = 2,
    Finish = 3,
    Abort = 4,
}

impl CreateTableJobStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic
    /// use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CreateTableJobStatus::Creating => "CREATE_TABLE_CREATING",
            CreateTableJobStatus::Rollbacking => "CREATE_TABLE_ROLLBACKING",
            CreateTableJobStatus::WriteDesc => "CREATE_TABLE_WRITE_DESC",
            CreateTableJobStatus::Finish => "CREATE_TABLE_FINISH",
            CreateTableJobStatus::Abort => "CREATE_TABLE_ABORT",
        }
    }

    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CREATE_TABLE_CREATING" => Some(Self::Creating),
            "CREATE_TABLE_ROLLBACKING" => Some(Self::Rollbacking),
            "CREATE_TABLE_WRITE_DESC" => Some(Self::WriteDesc),
            "CREATE_TABLE_FINISH" => Some(Self::Finish),
            "CREATE_TABLE_ABORT" => Some(Self::Abort),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CreateOneGroupStatus {
    Init = 0,
    Creating = 1,
    Rollbacking = 2,
    Finish = 3,
    Abort = 4,
}

impl CreateOneGroupStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic
    /// use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CreateOneGroupStatus::Init => "CREATE_ONE_GROUP_INIT",
            CreateOneGroupStatus::Creating => "CREATE_ONE_GROUP_CREATING",
            CreateOneGroupStatus::Rollbacking => "CREATE_ONE_GROUP_ROLLBACKING",
            CreateOneGroupStatus::Finish => "CREATE_ONE_GROUP_FINISH",
            CreateOneGroupStatus::Abort => "CREATE_ONE_GROUP_ABORT",
        }
    }

    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CREATE_ONE_GROUP_INIT" => Some(Self::Init),
            "CREATE_ONE_GROUP_CREATING" => Some(Self::Creating),
            "CREATE_ONE_GROUP_ROLLBACKING" => Some(Self::Rollbacking),
            "CREATE_ONE_GROUP_FINISH" => Some(Self::Finish),
            "CREATE_ONE_GROUP_ABORT" => Some(Self::Abort),
            _ => None,
        }
    }
}
