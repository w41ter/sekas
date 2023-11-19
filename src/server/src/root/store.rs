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

use std::sync::Arc;

use sekas_api::server::v1::group_request_union::Request::{self, *};
use sekas_api::server::v1::{GroupRequest, GroupRequestUnion, *};

use crate::constants::ROOT_GROUP_ID;
use crate::replica::Replica;
use crate::{Error, Result};

pub struct RootStore {
    replica: Arc<Replica>,
}

impl RootStore {
    pub fn new(replica: Arc<Replica>) -> Self {
        Self { replica }
    }

    pub async fn batch_write(&self, batch: ShardWriteRequest) -> Result<()> {
        self.submit_request(Request::Write(batch)).await?;
        Ok(())
    }

    pub async fn put(&self, shard_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let write = ShardWriteRequest {
            shard_id,
            puts: vec![PutRequest {
                put_type: PutType::None.into(),
                key,
                value,
                ..Default::default()
            }],
            ..Default::default()
        };
        self.submit_request(Request::Write(write)).await?;
        Ok(())
    }

    pub async fn get(&self, shard_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let get = ShardGetRequest {
            shard_id,
            start_version: sekas_schema::system::txn::TXN_MAX_VERSION,
            key: key.to_owned(),
        };
        let resp = self.submit_request(Request::Get(get)).await?;
        let resp = resp
            .response
            .ok_or_else(|| Error::InvalidArgument("GetResponse".into()))?
            .response
            .ok_or_else(|| Error::InvalidArgument("GetResponseUnion".into()))?;
        if let group_response_union::Response::Get(resp) = resp {
            Ok(resp.value.and_then(|v| v.content))
        } else {
            Err(Error::InvalidArgument("GetResponse".into()))
        }
    }

    pub async fn delete(&self, shard_id: u64, key: &[u8]) -> Result<()> {
        let write = ShardWriteRequest {
            shard_id,
            deletes: vec![DeleteRequest { key: key.to_owned(), ..Default::default() }],
            ..Default::default()
        };
        self.submit_request(Request::Write(write)).await?;
        Ok(())
    }

    pub async fn list(&self, shard_id: u64, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let resp = self
            .submit_request(Scan(ShardScanRequest {
                shard_id,
                prefix: Some(prefix.to_owned()),
                start_version: sekas_schema::system::txn::TXN_MAX_VERSION,
                ..Default::default()
            }))
            .await?;
        let resp = resp
            .response
            .ok_or_else(|| Error::InvalidArgument("PrefixListResponse".into()))?
            .response
            .ok_or_else(|| Error::InvalidArgument("PrefixListUnionResponse".into()))?;

        if let group_response_union::Response::Scan(resp) = resp {
            Ok(resp
                .data
                .into_iter()
                .filter_map(|v| v.values.last().and_then(|v| v.content.clone()))
                .collect())
        } else {
            Err(Error::InvalidArgument("PrefixListResponse".into()))
        }
    }

    async fn submit_request(&self, req: Request) -> Result<GroupResponse> {
        use crate::replica::retry::execute;
        use crate::replica::ExecCtx;

        let request = GroupRequest {
            group_id: ROOT_GROUP_ID,
            epoch: self.replica.epoch(),
            request: Some(GroupRequestUnion { request: Some(req) }),
        };

        execute(&self.replica, &ExecCtx::default(), &request).await
    }
}
