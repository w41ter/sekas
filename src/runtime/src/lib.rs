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
#![feature(const_type_name)]

mod executor;
mod group;
mod incoming;
mod metrics;
mod shutdown;

pub mod sync;
pub mod time;

use serde::{Deserialize, Serialize};
pub use tokio::select;
pub use tokio::task::yield_now;

pub use self::executor::*;
pub use self::group::TaskGroup;
pub use self::incoming::TcpIncoming;
pub use self::shutdown::{Shutdown, ShutdownNotifier};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExecutorConfig {
    pub event_interval: Option<u32>,
    pub global_event_interval: Option<u32>,
    pub max_blocking_threads: Option<usize>,
}
