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

use log::debug;
use sekas_client::ServiceDiscovery;

use crate::engine::StateEngine;

pub struct RootDiscovery {
    initial_nodes: Vec<String>,
    state_engine: StateEngine,
}

impl RootDiscovery {
    pub fn new(initial_nodes: Vec<String>, state_engine: StateEngine) -> Self {
        RootDiscovery { initial_nodes, state_engine }
    }
}

#[crate::async_trait]
impl ServiceDiscovery for RootDiscovery {
    async fn list_nodes(&self) -> Vec<String> {
        if let Ok(Some(root)) = self.state_engine.load_root_desc().await {
            if !root.root_nodes.is_empty() {
                debug!("load root nodes from state engine, root desc {root:?}");
                return root.root_nodes.into_iter().map(|n| n.addr).collect();
            }
        }
        debug!("load root nodes from initial nodes, {:?}", self.initial_nodes);
        self.initial_nodes.clone()
    }
}
