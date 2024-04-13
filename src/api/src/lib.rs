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
