// Copyright 2024-present The Sekas Authors.
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

#![allow(clippy::cloned_ref_to_slice_refs)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::manual_async_fn)]
#![allow(clippy::manual_ok_err)]
#![allow(clippy::manual_pop_if)]
#![allow(clippy::manual_saturating_arithmetic)]
#![allow(clippy::result_large_err)]
#![allow(clippy::unnecessary_sort_by)]
#![feature(linked_list_cursors)]

mod bootstrap;
mod config;
mod constants;
mod engine;
mod error;
mod replica;
mod root;
mod schedule;
mod service;
mod transport;

pub mod node;
pub mod raftgroup;
pub mod serverpb;

pub(crate) use tonic::async_trait;

pub use crate::bootstrap::run;
pub use crate::config::*;
pub use crate::error::{Error, Result};
pub use crate::root::diagnosis;
pub use crate::service::Server;

#[cfg(test)]
mod tests {
    #[ctor::ctor]
    fn init() {
        tracing_subscriber::fmt::init();
    }
}
