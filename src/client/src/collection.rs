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

use std::time::Duration;

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::{
    CollectionDesc, DeleteRequest, PutRequest, PutType, ShardGetRequest, ShardGetResponse, Value,
    WriteCondition,
};

use crate::group_client::GroupClient;
use crate::metrics::*;
use crate::retry::RetryState;
use crate::{record_latency, AppResult, SekasClient};

#[derive(Debug, Default, Clone)]
pub struct WriteBatchRequest {
    pub deletes: Vec<DeleteRequest>,
    pub puts: Vec<PutRequest>,
}

#[derive(Debug, Default, Clone)]
pub struct WriteBatchResponse {
    /// The version of this batch.
    pub version: u64,
    /// The prev value of the target key.
    ///
    /// Only for the requests with `take_prev_value`.
    pub deletes: Vec<Option<Value>>,
    /// The prev value of the target key.
    ///
    /// Only for the requests with `take_prev_value`.
    pub puts: Vec<Option<Value>>,
}

pub struct WriteBuilder {
    /// The key to operate.
    key: Vec<u8>,
    /// The type of request operation.
    put_type: PutType,
    /// The cas conditions.
    conditions: Vec<WriteCondition>,
}

#[derive(Debug, Clone)]
pub struct Collection {
    client: SekasClient,
    co_desc: CollectionDesc,
    rpc_timeout: Option<Duration>,
}

impl WriteBuilder {
    pub fn new(key: Vec<u8>) -> Self {
        WriteBuilder { key, put_type: PutType::None, conditions: vec![] }
    }

    /// With ttl, in seconds.
    ///
    /// Only works for put request.
    pub fn with_ttl(self, ttl: Option<u64>) -> Self {
        todo!()
    }

    /// Build a put request.
    pub fn put(self, value: Vec<u8>) -> AppResult<PutRequest> {
        todo!()
    }

    /// Build a put request without any error.
    pub fn ensure_put(self, value: Vec<u8>) -> PutRequest {
        todo!()
    }

    /// Build a delete request.
    pub fn delete(self) -> AppResult<DeleteRequest> {
        // delete not support nop
        todo!()
    }

    /// Build a delete request without any error.
    pub fn ensure_delete(self) -> DeleteRequest {
        todo!()
    }

    /// Build a nop request.
    pub fn nop(self) -> AppResult<PutRequest> {
        todo!()
    }

    /// Build a nop request without any error.
    pub fn ensure_nop(self) -> PutRequest {
        todo!()
    }

    /// Build an add request, the value will be interpreted as i64.
    pub fn add(self, val: i64) -> AppResult<PutRequest> {
        todo!()
    }

    /// Build an add request without any error, the value will be interpreted as
    /// i64.
    pub fn ensure_add(self, val: i64) -> PutRequest {
        todo!()
    }

    /// Expect that the max version of the key is less than the input value.
    ///
    /// One request only can contains one version releated expection.
    pub fn expect_version_lt(self, expect: u64) -> Self {
        todo!()
    }

    /// Expect that the max version of the key is less than or equals to the
    /// input value.
    ///
    /// One request only can contains one version releated expection.
    pub fn expect_version_le(self, expect: u64) -> Self {
        todo!()
    }

    /// Expect that the max version of the key is great than the input value.
    ///
    /// One request only can contains one version releated expection.
    pub fn expect_version_gt(self, expect: u64) -> Self {
        todo!()
    }

    /// Expect that the max version of the key is great than or equal to the
    /// input value.
    ///
    /// One request only can contains one version releated expection.
    pub fn expect_version_ge(self, expect: u64) -> Self {
        todo!()
    }

    /// Expect that the max version of the key is equal to the input value.
    ///
    /// One request only can contains one version releated expection.
    pub fn expect_version(self, expect: u64) -> Self {
        todo!()
    }

    /// Expect that the target key is not exists.
    pub fn expect_not_exists(self) -> Self {
        todo!()
    }

    /// Expect that the target key is equal to the input value.
    pub fn expect_value(self, value: Vec<u8>) -> Self {
        todo!()
    }

    /// Expect that the key contains the input value.
    pub fn expect_contains(self, value: Vec<u8>) -> Self {
        todo!()
    }

    /// Expect that the slice of the value is equal to the input value.
    pub fn expect_slice(self, beg: u64, value: Vec<u8>) -> Self {
        todo!()
    }

    /// Expect that the value of key is starts with the input value.
    pub fn expect_starts_with(self, value: Vec<u8>) -> Self {
        todo!()
    }

    /// Expect that the value of key is ends with the input value.
    pub fn expect_ends_with(self, value: Vec<u8>) -> Self {
        todo!()
    }

    /// Take the prev value.
    ///
    /// It is useful for cas operations.
    pub fn take_prev_value(self) -> Self {
        todo!()
    }
}

impl Collection {
    pub fn new(
        client: SekasClient,
        co_desc: CollectionDesc,
        rpc_timeout: Option<Duration>,
    ) -> Collection {
        Collection { client, co_desc, rpc_timeout }
    }

    pub async fn delete(&self, key: Vec<u8>) -> AppResult<()> {
        let batch = WriteBatchRequest {
            deletes: vec![WriteBuilder::new(key).ensure_delete()],
            ..Default::default()
        };
        self.write_batch(batch).await?;
        Ok(())
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> AppResult<()> {
        let batch = WriteBatchRequest {
            puts: vec![WriteBuilder::new(key).ensure_put(value)],
            ..Default::default()
        };
        self.write_batch(batch).await?;
        Ok(())
    }

    pub async fn get(&self, key: Vec<u8>) -> AppResult<Option<Value>> {
        CLIENT_DATABASE_BYTES_TOTAL.rx.inc_by(key.len() as u64);
        CLIENT_DATABASE_REQUEST_TOTAL.get.inc();
        record_latency!(&CLIENT_DATABASE_REQUEST_DURATION_SECONDS.get);
        let mut retry_state = RetryState::new(self.rpc_timeout);

        loop {
            match self.get_inner(&key, retry_state.timeout()).await {
                Ok(value) => {
                    CLIENT_DATABASE_BYTES_TOTAL.tx.inc_by(
                        value
                            .as_ref()
                            .map(|v| v.content.as_ref().map(Vec::len).unwrap_or_default())
                            .unwrap_or_default() as u64,
                    );
                    return Ok(value);
                }
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    pub async fn write_batch(&self, req: WriteBatchRequest) -> AppResult<WriteBatchResponse> {
        todo!()
    }

    async fn get_inner(
        &self,
        key: &[u8],
        timeout: Option<Duration>,
    ) -> crate::Result<Option<Value>> {
        let router = self.client.router();
        let (group, shard) = router.find_shard(self.co_desc.clone(), key)?;
        let mut client = GroupClient::new(group, self.client.clone());
        let req = Request::Get(ShardGetRequest {
            shard_id: shard.id,
            start_version: u64::MAX,
            key: key.to_owned(),
        });
        if let Some(duration) = timeout {
            client.set_timeout(duration);
        }
        match client.request(&req).await? {
            Response::Get(ShardGetResponse { value }) => Ok(value),
            _ => Err(crate::Error::Internal(wrap("invalid response type, Get is required"))),
        }
    }

    #[allow(dead_code)]
    fn name(&self) -> String {
        self.co_desc.name.to_owned()
    }

    #[inline]
    pub fn desc(&self) -> CollectionDesc {
        self.co_desc.clone()
    }
}

#[inline]
fn wrap(msg: &str) -> Box<dyn std::error::Error + Sync + Send + 'static> {
    let msg = String::from(msg);
    msg.into()
}
