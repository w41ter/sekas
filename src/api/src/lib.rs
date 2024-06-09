// Copyright 2023-present The Engula Authors.
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

mod desc;
mod error;
mod move_shard;
mod request;
mod txn;
mod value;
mod write;

pub mod server {
    pub mod v1 {
        #![allow(clippy::all)]
        tonic::include_proto!("sekas.server.v1");

        /// A helper enum to hold both [`PutRequest`] and [`DeleteRequest`].
        pub type WriteRequest = write_intent_request::Write;
    }
}

const SHARD_UPDATE_DELTA: u64 = 1 << 32;
const CONFIG_CHANGE_DELTA: u64 = 1;

/// A type to present epoch.
pub struct Epoch(pub u64);

impl Epoch {
    #[inline]
    pub fn shard_epoch(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub fn config_epoch(&self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub fn apply_shard_delta(&self) -> Self {
        Epoch(self.0 + SHARD_UPDATE_DELTA)
    }

    #[inline]
    pub fn apply_config_delta(&self) -> Self {
        Epoch(self.0 + CONFIG_CHANGE_DELTA)
    }
}

impl std::fmt::Display for Epoch {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.shard_epoch(), self.config_epoch())
    }
}

#[inline]
pub fn apply_config_delta(epoch: u64) -> u64 {
    Epoch(epoch).apply_config_delta().0
}

#[inline]
pub fn apply_shard_delta(epoch: u64) -> u64 {
    Epoch(epoch).apply_shard_delta().0
}
