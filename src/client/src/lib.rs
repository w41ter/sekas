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

pub mod error;

mod app_client;
mod collection;
mod database;
mod discovery;
mod group_client;
mod metrics;
mod move_shard_client;
mod retry;
mod rpc;
mod shard_client;
mod txn;
mod write_batch;

use tonic::async_trait;

pub use crate::app_client::{Client as SekasClient, ClientOptions};
pub use crate::collection::{Collection, WriteBatchRequest, WriteBatchResponse, WriteBuilder};
pub use crate::database::Database;
pub use crate::discovery::{ServiceDiscovery, StaticServiceDiscovery};
pub use crate::error::{AppError, AppResult, Error, Result};
pub use crate::group_client::GroupClient;
pub use crate::move_shard_client::MoveShardClient;
pub use crate::retry::RetryState;
pub use crate::rpc::{ConnManager, NodeClient, RootClient, Router, RouterGroupState};
pub use crate::shard_client::ShardClient;
pub use crate::txn::TxnStateTable;
