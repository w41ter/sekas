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

use sekas_api::server::v1::*;

use crate::group_client::GroupClient;
use crate::retry::RetryState;
use crate::shard_client::ShardClient;
use crate::{Error, Result, SekasClient};

/// `MigrateClient` wraps `GroupClient` and provides retry for moving shard
/// related functions.
pub struct MoveShardClient {
    group_id: u64,
    client: SekasClient,
}

impl MoveShardClient {
    pub fn new(group_id: u64, client: SekasClient) -> Self {
        MoveShardClient { group_id, client }
    }

    pub async fn acquire_shard(&mut self, desc: &MoveShardDesc) -> Result<()> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.acquire_shard(desc).await {
                Ok(()) => return Ok(()),
                e @ Err(Error::EpochNotMatch(..)) => return e,
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn move_out(&mut self, desc: &MoveShardDesc) -> Result<()> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.move_out(desc).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn pull_shard_chunk(
        &self,
        shard_id: u64,
        last_key: Option<Vec<u8>>,
    ) -> Result<Vec<ValueSet>> {
        let mut retry_state = RetryState::new(None);

        loop {
            let client = ShardClient::new(self.group_id, shard_id, self.client.clone());
            match client.pull(last_key.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn forward(&mut self, req: &ForwardRequest) -> Result<ForwardResponse> {
        let mut retry_state = RetryState::new(None);

        loop {
            let mut client = self.group_client();
            match client.forward(req).await {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    #[inline]
    fn group_client(&self) -> GroupClient {
        GroupClient::lazy(self.group_id, self.client.clone())
    }
}
