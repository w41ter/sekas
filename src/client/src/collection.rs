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
use sekas_api::server::v1::*;

use crate::group_client::GroupClient;
use crate::metrics::*;
use crate::retry::RetryState;
use crate::write_batch::WriteBatchContext;
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
    /// The cas conditions.
    conditions: Vec<WriteCondition>,
    /// The TTL of key.
    ttl: Option<u64>,
    /// Whether to take prev values.
    take_prev_value: bool,
}

#[derive(Debug, Clone)]
pub struct Collection {
    client: SekasClient,
    co_desc: CollectionDesc,
    rpc_timeout: Option<Duration>,
}

impl WriteBuilder {
    pub fn new(key: Vec<u8>) -> Self {
        WriteBuilder { key, conditions: vec![], ttl: None, take_prev_value: false }
    }

    /// With ttl, in seconds.
    ///
    /// Only works for put request.
    pub fn with_ttl(mut self, ttl: Option<u64>) -> Self {
        self.ttl = ttl;
        self
    }

    /// Build a put request.
    pub fn put(self, value: Vec<u8>) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::None.into(),
            key: self.key,
            value,
            ttl: self.ttl.unwrap_or_default(),
            take_prev_value: self.take_prev_value,
            conditions: self.conditions,
        })
    }

    /// Build a put request without any error.
    pub fn ensure_put(self, value: Vec<u8>) -> PutRequest {
        self.put(value).expect("Invalid put conditions")
    }

    /// Build a delete request.
    pub fn delete(self) -> AppResult<DeleteRequest> {
        self.verify_conditions()?;
        Ok(DeleteRequest {
            key: self.key,
            conditions: self.conditions,
            take_prev_value: self.take_prev_value,
        })
    }

    /// Build a delete request without any error.
    pub fn ensure_delete(self) -> DeleteRequest {
        self.delete().expect("Invalid delete conditions")
    }

    /// Build a nop request.
    pub fn nop(self) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::Nop.into(),
            key: self.key,
            value: vec![],
            ttl: 0,
            conditions: self.conditions,
            take_prev_value: false,
        })
    }

    /// Build a nop request without any error.
    pub fn ensure_nop(self) -> PutRequest {
        self.nop().expect("Invalid nop conditions")
    }

    /// Build an add request, the value will be interpreted as i64.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, val: i64) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::AddI64.into(),
            key: self.key,
            value: val.to_le_bytes().to_vec(),
            ttl: self.ttl.unwrap_or_default(),
            conditions: self.conditions,
            take_prev_value: self.take_prev_value,
        })
    }

    /// Build an add request without any error, the value will be interpreted as
    /// i64.
    pub fn ensure_add(self, val: i64) -> PutRequest {
        self.add(val).expect("Invalid add conditions")
    }

    /// Expect that the max version of the key is less than the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_lt(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionLt.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is less than or equals to the
    /// input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_le(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionLe.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is great than the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_gt(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionGt.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is great than or equal to the
    /// input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_ge(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionGe.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is equal to the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersion.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is exists exists.
    pub fn expect_exists(mut self) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectExists.into(),
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is not exists.
    pub fn expect_not_exists(mut self) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectNotExists.into(),
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is equal to the input value.
    pub fn expect_value(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectValue.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the key contains the input value.
    pub fn expect_contains(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectContains.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the slice of the value is equal to the input value.
    pub fn expect_slice(mut self, begin: u64, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectSlice.into(),
            value,
            begin,
            ..Default::default()
        });
        self
    }

    /// Expect that the value of key is starts with the input value.
    pub fn expect_starts_with(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectStartsWith.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the value of key is ends with the input value.
    pub fn expect_ends_with(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectEndsWith.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Take the prev value.
    ///
    /// It is useful for cas operations. Default is `false`.
    pub fn take_prev_value(mut self) -> Self {
        self.take_prev_value = true;
        self
    }

    fn verify_conditions(&self) -> AppResult<()> {
        // TODO(walter) check conditions
        Ok(())
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

    pub async fn write_batch(&self, req: WriteBatchRequest) -> crate::Result<WriteBatchResponse> {
        let ctx = WriteBatchContext::new(
            self.co_desc.clone(),
            req,
            self.client.clone(),
            self.rpc_timeout,
        );
        ctx.commit().await
    }

    pub async fn get(&self, key: Vec<u8>) -> crate::Result<Option<Vec<u8>>> {
        let value = self.get_raw_value(key).await?;
        Ok(value.and_then(|v| v.content))
    }

    pub async fn get_raw_value(&self, key: Vec<u8>) -> crate::Result<Option<Value>> {
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

    /// To issue a batch writes to a shard.
    #[allow(dead_code)]
    pub(crate) async fn write(
        &self,
        request: ShardWriteRequest,
    ) -> crate::Result<ShardWriteResponse> {
        let mut retry_state = RetryState::new(None);
        loop {
            match self.write_inner(&request, retry_state.timeout()).await {
                Ok(value) => {
                    return Ok(value);
                }
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn write_inner(
        &self,
        request: &ShardWriteRequest,
        timeout: Option<Duration>,
    ) -> crate::Result<ShardWriteResponse> {
        let router = self.client.router();
        let group_state = router.find_group_by_shard(request.shard_id)?;
        let mut group_client = GroupClient::new(group_state, self.client.clone());
        if let Some(duration) = timeout {
            group_client.set_timeout(duration);
        }

        let request = Request::Write(request.clone());
        match group_client.request(&request).await? {
            Response::Write(resp) => Ok(resp),
            _ => Err(crate::Error::Internal(wrap("invalid response type, Write is required"))),
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
