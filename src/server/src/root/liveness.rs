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

use std::collections::{hash_map, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub struct NodeLiveness {
    expiration: u128,
}

impl NodeLiveness {
    pub fn is_dead(&self) -> bool {
        self.expiration < current_timestamp()
    }

    #[allow(dead_code)]
    pub fn is_alive(&self) -> bool {
        self.expiration > current_timestamp()
    }
}

#[derive(Clone)]
pub struct Liveness {
    liveness_threshold: Duration,
    nodes: Arc<Mutex<HashMap<u64, NodeLiveness>>>,
}

impl Liveness {
    pub fn new(liveness_threshold: Duration) -> Self {
        Self { liveness_threshold, nodes: Default::default() }
    }

    pub fn get(&self, node: &u64) -> NodeLiveness {
        let nodes = self.nodes.lock().unwrap();
        nodes
            .get(node)
            .cloned()
            .unwrap_or_else(|| NodeLiveness { expiration: self.new_expiration() })
    }

    pub fn renew(&self, node_id: u64) {
        let mut nodes = self.nodes.lock().unwrap();
        let entry = nodes.entry(node_id);
        match entry {
            hash_map::Entry::Occupied(mut ent) => {
                let renew = self.new_expiration();
                let ent = ent.get_mut();
                if ent.expiration < renew {
                    ent.expiration = renew
                }
            }
            hash_map::Entry::Vacant(ent) => {
                ent.insert(NodeLiveness { expiration: self.new_expiration() });
            }
        }
    }

    pub fn init_node_if_first_seen(&self, node_id: u64) {
        // Give `liveness_threshold` time window to retry before mark as offline.
        let mut nodes = self.nodes.lock().unwrap();
        if let hash_map::Entry::Vacant(ent) = nodes.entry(node_id) {
            ent.insert(NodeLiveness { expiration: self.new_expiration() });
        }
    }

    pub fn reset(&self) {
        self.nodes.lock().unwrap().clear();
    }

    fn new_expiration(&self) -> u128 {
        current_timestamp() + self.liveness_threshold.as_millis()
    }
}

fn current_timestamp() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    since_the_epoch.as_millis()
}
