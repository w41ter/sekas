// Copyright 2023-present The Sekas Authors.
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

use std::sync::{Arc, Mutex};

use crate::JoinHandle;

/// A structure to hold a set of async tasks.
///
/// All tasks will be abort when [`TaskGroup`] is drop.
#[derive(Default, Clone)]
pub struct TaskGroup {
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TaskGroup {
    #[inline]
    pub fn add_task(&self, handle: JoinHandle<()>) {
        let mut handles = self.handles.lock().expect("Poisoned");
        handles.retain(|handle| !handle.is_finished());
        handles.push(handle);
    }
}
