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
mod actions;
mod event_source;
mod provider;
mod scheduler;
mod setup;
mod task;
mod tasks;

use sekas_api::server::v1::ScheduleState;

pub(crate) use self::provider::MoveReplicasProvider;
pub(crate) use self::setup::setup_scheduler;

pub trait ScheduleStateObserver: Send + Sync {
    fn on_schedule_state_updated(&self, schedule_state: ScheduleState);
}
