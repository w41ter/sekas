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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sekas_api::server::v1::root_client::RootClient;
use vaelo::{ConnectionTag, RpcChannel};

static NEXT_CONNECTION_TAG: AtomicU64 = AtomicU64::new(1);

use super::NodeClient;
use crate::{Error, Result};

#[derive(Clone, Debug)]
pub struct ConnManager {
    connect_timeout: Option<Duration>,
    connection_tag: ConnectionTag,
    core: Arc<Mutex<Core>>,
}

#[derive(Debug)]
struct Core {
    channels: HashMap<String, ChannelInfo>,
}

#[derive(Debug)]
struct ChannelInfo {
    channel: RpcChannel,
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

    pub fn get(&self, addr: String) -> Result<RpcChannel> {
        let mut core = self.core.lock().unwrap();
        if let Some(info) = core.channels.get(&addr) {
            return Ok(info.channel.clone());
        }

        let remote = addr.parse().map_err(|e| Error::Internal(Box::new(e)))?;
        let mut builder = RpcChannel::grpc(remote).connection_tag(self.connection_tag);
        if let Some(connect_timeout) = self.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        let channel = builder.build().map_err(|e| Error::Internal(Box::new(e)))?;
        let info = ChannelInfo { channel: channel.clone() };
        core.channels.insert(addr, info);
        Ok(channel)
    }

    #[inline]
    pub fn get_node_client(&self, addr: String) -> Result<NodeClient> {
        let channel = self.get(addr)?;
        Ok(NodeClient::new(channel))
    }

    #[inline]
    pub fn get_root_client(&self, addr: String) -> Result<RootClient> {
        let channel = self.get(addr)?;
        Ok(RootClient::new(channel))
    }
}

impl Default for ConnManager {
    fn default() -> Self {
        let core = Arc::new(Mutex::new(Core { channels: HashMap::default() }));
        ConnManager {
            core,
            connect_timeout: None,
            connection_tag: ConnectionTag::new(NEXT_CONNECTION_TAG.fetch_add(1, Ordering::Relaxed)),
        }
    }
}
