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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sekas_api::server::v1::root_client::RootClient;
use tonic::transport::{Channel, Endpoint};

use super::NodeClient;
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct ConnManager {
    connect_timeout: Option<Duration>,
    core: Arc<Mutex<Core>>,
}

#[derive(Debug)]
struct Core {
    channels: HashMap<String, ChannelInfo>,
}

#[derive(Debug)]
struct ChannelInfo {
    channel: Channel,
    access: usize,
}

impl ConnManager {
    pub fn new() -> Self {
        ConnManager::default()
    }

    pub fn with_connect_timeout(timeout: Duration) -> Self {
        let mut mgr = ConnManager::new();
        mgr.connect_timeout = Some(timeout);
        mgr
    }

    // TODO(walter) add tags
    pub fn get(&self, addr: String) -> Result<Channel> {
        let mut core = self.core.lock().unwrap();
        if let Some(info) = core.channels.get_mut(&addr) {
            info.access += 1;
            return Ok(info.channel.clone());
        }

        let channel = match Endpoint::new(format!("http://{}", addr)) {
            Ok(endpoint) => {
                if let Some(connect_timeout) = self.connect_timeout {
                    endpoint.connect_timeout(connect_timeout).connect_lazy()
                } else {
                    endpoint.connect_lazy()
                }
            }
            Err(e) => return Err(Error::Internal(Box::new(e))),
        };
        let info = ChannelInfo { channel: channel.clone(), access: 1 };
        core.channels.insert(addr, info);
        Ok(channel)
    }

    #[inline]
    pub fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let channel = self.get(addr)?;
        Ok(NodeClient::new(channel))
    }

    #[inline]
    pub fn get_root_client(&self, addr: String) -> Result<RootClient<Channel>> {
        let channel = self.get(addr)?;
        Ok(RootClient::new(channel))
    }
}

impl Default for ConnManager {
    fn default() -> Self {
        let core = Arc::new(Mutex::new(Core { channels: HashMap::default() }));
        let cloned_core = core.clone();

        // FIXME
        // 1. graceful shutdown
        // 2. spawn in executor.
        tokio::spawn(async move {
            recycle_conn_main(cloned_core).await;
        });
        ConnManager { core, connect_timeout: None }
    }
}

async fn recycle_conn_main(core: Arc<Mutex<Core>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let mut core = core.lock().unwrap();
        core.channels.retain(|_, v| {
            if v.access == 0 {
                false
            } else {
                v.access = 0;
                true
            }
        });
    }
}
