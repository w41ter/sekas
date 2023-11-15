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

pub use sekas_schema::LOCAL_COLLECTION_ID;

pub const REPLICA_PER_GROUP: usize = 3;

pub const ROOT_GROUP_ID: u64 = 0;
pub const FIRST_GROUP_ID: u64 = ROOT_GROUP_ID + 1;
pub const STATE_REPLICA_ID: u64 = 0;
pub const FIRST_REPLICA_ID: u64 = 1;
pub const INIT_USER_REPLICA_ID: u64 = FIRST_REPLICA_ID + 1;
pub const FIRST_NODE_ID: u64 = 0;
// A group exists at least an replica, so the initial epoch is not zero.
pub const INITIAL_EPOCH: u64 = 1;
pub const INITIAL_JOB_ID: u64 = 0;
