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

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;

use crate::group_client::GroupClient;
use crate::retry::RetryState;
use crate::{Error, Result, SekasClient, WriteBuilder};

/// `ShardClient` wraps `GroupClient` and provides retry for shard-related
/// functions.
///
/// Since it will retry all requests in the current group, the user must ensure
/// that the shard will not be migrated during the request process.
pub struct ShardClient {
    group_id: u64,
    shard_id: u64,
    client: SekasClient,
}

impl ShardClient {
    pub fn new(group_id: u64, shard_id: u64, client: SekasClient) -> Self {
        ShardClient { group_id, shard_id, client }
    }

    pub async fn prefix_list(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut retry_state = RetryState::new(None);

        loop {
            match self.prefix_list_inner(prefix).await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut retry_state = RetryState::new(None);

        loop {
            match self.delete_inner(key).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn pull(&self, last_key: Option<Vec<u8>>) -> Result<Vec<ValueSet>> {
        let req = Request::Scan(ShardScanRequest {
            shard_id: self.shard_id,
            start_version: u64::MAX,
            prefix: None,
            limit: 0,
            limit_bytes: 64 * 1024, // 64KB
            exclude_start_key: true,
            exclude_end_key: false,
            start_key: last_key,
            end_key: None,
        });
        let mut client = GroupClient::lazy(self.group_id, self.client.clone());
        match client.request(&req).await? {
            Response::Scan(ShardScanResponse { data }) => Ok(data),
            _ => Err(Error::Internal(
                "invalid response type, `ShardScanResponse` is required".into(),
            )),
        }
    }

    async fn prefix_list_inner(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let req = Request::Scan(ShardScanRequest {
            shard_id: self.shard_id,
            prefix: Some(prefix.to_owned()),
            ..Default::default()
        });
        let mut client = GroupClient::lazy(self.group_id, self.client.clone());
        match client.request(&req).await? {
            Response::Scan(ShardScanResponse { data }) => {
                Ok(data.into_iter().map(|v| v.values).collect())
            }
            _ => Err(Error::Internal(
                "invalid response type, `ShardScanResponse` is required".into(),
            )),
        }
    }

    async fn delete_inner(&self, key: &[u8]) -> Result<()> {
        let req = Request::Write(ShardWriteRequest {
            shard_id: self.shard_id,
            deletes: vec![WriteBuilder::new(key.to_owned()).ensure_delete()],
            ..Default::default()
        });
        let mut client = GroupClient::lazy(self.group_id, self.client.clone());
        client.request(&req).await?;
        Ok(())
    }
}
