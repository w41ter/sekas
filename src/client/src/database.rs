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

use crate::metrics::*;
use crate::write_batch::WriteBatchContext;
use crate::{
    record_latency, AppError, AppResult, GroupClient, RetryState, SekasClient, WriteBatchRequest,
    WriteBatchResponse, WriteBuilder,
};

#[derive(Debug, Clone)]
pub struct Database {
    client: SekasClient,
    desc: DatabaseDesc,
    rpc_timeout: Option<Duration>,
}

impl Database {
    pub fn new(client: SekasClient, desc: DatabaseDesc, rpc_timeout: Option<Duration>) -> Self {
        Database { client, desc, rpc_timeout }
    }

    pub async fn create_collection(&self, name: String) -> AppResult<CollectionDesc> {
        let desc = self.client.root_client().create_collection(self.desc.clone(), name).await?;
        Ok(desc)
    }

    pub async fn delete_collection(&self, name: String) -> AppResult<()> {
        self.client.root_client().delete_collection(self.desc.clone(), name).await?;
        Ok(())
    }

    pub async fn list_collection(&self) -> AppResult<Vec<CollectionDesc>> {
        let collections = self.client.root_client().list_collection(self.desc.clone()).await?;
        Ok(collections)
    }

    pub async fn open_collection(&self, name: String) -> AppResult<CollectionDesc> {
        match self.client.root_client().get_collection(self.desc.clone(), name.clone()).await? {
            None => Err(AppError::NotFound(format!("collection {}", name))),
            Some(co_desc) => Ok(co_desc),
        }
    }

    pub async fn delete(&self, collection_id: u64, key: Vec<u8>) -> AppResult<()> {
        let delete = WriteBuilder::new(key).ensure_delete();
        let batch =
            WriteBatchRequest { deletes: vec![(collection_id, delete)], ..Default::default() };
        self.write_batch(batch).await?;
        Ok(())
    }

    pub async fn put(&self, collection_id: u64, key: Vec<u8>, value: Vec<u8>) -> AppResult<()> {
        let put = WriteBuilder::new(key).ensure_put(value);
        let batch = WriteBatchRequest { puts: vec![(collection_id, put)], ..Default::default() };
        self.write_batch(batch).await?;
        Ok(())
    }

    pub async fn write_batch(&self, req: WriteBatchRequest) -> crate::Result<WriteBatchResponse> {
        let ctx = WriteBatchContext::new(req, self.client.clone(), self.rpc_timeout);
        ctx.commit().await
    }

    pub async fn get(&self, collection_id: u64, key: Vec<u8>) -> crate::Result<Option<Vec<u8>>> {
        let value = self.get_raw_value(collection_id, key).await?;
        Ok(value.and_then(|v| v.content))
    }

    pub async fn get_raw_value(
        &self,
        collection_id: u64,
        key: Vec<u8>,
    ) -> crate::Result<Option<Value>> {
        CLIENT_DATABASE_BYTES_TOTAL.rx.inc_by(key.len() as u64);
        CLIENT_DATABASE_REQUEST_TOTAL.get.inc();
        record_latency!(&CLIENT_DATABASE_REQUEST_DURATION_SECONDS.get);
        let mut retry_state = RetryState::new(self.rpc_timeout);

        loop {
            match self.get_inner(collection_id, &key, &mut retry_state).await {
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
        collection_id: u64,
        user_key: &[u8],
        retry_state: &mut RetryState,
    ) -> crate::Result<Option<Value>> {
        let root_client = self.client.root_client();
        let start_version = root_client.alloc_txn_id(1, retry_state.timeout()).await?;

        let router = self.client.router();
        let (group, shard) = router.find_shard(collection_id, user_key)?;
        let mut client = GroupClient::new(group, self.client.clone());
        let req = Request::Get(ShardGetRequest {
            shard_id: shard.id,
            start_version,
            user_key: user_key.to_owned(),
        });
        if let Some(duration) = retry_state.timeout() {
            client.set_timeout(duration);
        }
        match client.request(&req).await? {
            Response::Get(ShardGetResponse { value }) => Ok(value),
            _ => Err(crate::Error::Internal("invalid response type, Get is required".into())),
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
            _ => Err(crate::Error::Internal("invalid response type, Write is required".into())),
        }
    }

    #[allow(dead_code)]
    pub fn name(&self) -> String {
        self.desc.name.to_owned()
    }

    #[inline]
    pub fn desc(&self) -> DatabaseDesc {
        self.desc.clone()
    }
}
