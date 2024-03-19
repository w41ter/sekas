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

use crate::range::{RangeRequest, RangeStream};
use crate::{AppError, AppResult, GroupClient, RetryState, SekasClient, Txn, WriteBuilder};

#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) client: SekasClient,
    pub(crate) desc: DatabaseDesc,
    pub(crate) rpc_timeout: Option<Duration>,
    /// Read value by ignore any versions.
    pub(crate) read_without_version: bool,
}

impl Database {
    pub fn new(client: SekasClient, desc: DatabaseDesc, rpc_timeout: Option<Duration>) -> Self {
        let read_without_version = desc.id == sekas_schema::system::db::ID;
        Database { client, desc, rpc_timeout, read_without_version }
    }

    pub async fn create_table(&self, name: String) -> AppResult<TableDesc> {
        let desc = self.client.root_client().create_table(self.desc.clone(), name).await?;
        Ok(desc)
    }

    pub async fn delete_table(&self, name: String) -> AppResult<()> {
        self.client.root_client().delete_table(self.desc.clone(), name).await?;
        Ok(())
    }

    pub async fn list_table(&self) -> AppResult<Vec<TableDesc>> {
        let tables = self.client.root_client().list_table(self.desc.clone()).await?;
        Ok(tables)
    }

    pub async fn open_table(&self, name: String) -> AppResult<TableDesc> {
        match self.client.root_client().get_table(self.desc.clone(), name.clone()).await? {
            None => Err(AppError::NotFound(format!("table {}", name))),
            Some(co_desc) => Ok(co_desc),
        }
    }

    #[inline]
    pub async fn delete(&self, table_id: u64, key: Vec<u8>) -> AppResult<()> {
        let mut txn = Txn::new(self.clone());
        txn.delete(table_id, WriteBuilder::new(key).ensure_delete());
        txn.commit().await?;
        Ok(())
    }

    #[inline]
    pub async fn put(&self, table_id: u64, key: Vec<u8>, value: Vec<u8>) -> AppResult<()> {
        let mut txn = Txn::new(self.clone());
        txn.put(table_id, WriteBuilder::new(key).ensure_put(value));
        txn.commit().await?;
        Ok(())
    }

    #[inline]
    pub fn begin_txn(&self) -> Txn {
        Txn::new(self.clone())
    }

    #[inline]
    pub async fn get(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Vec<u8>>> {
        let mut txn = Txn::new(self.clone());
        txn.get(table_id, key).await
    }

    #[inline]
    pub async fn get_raw_value(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Value>> {
        let mut txn = Txn::new(self.clone());
        txn.get_raw_value(table_id, key).await
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

    /// To scan a shard.
    #[inline]
    pub async fn scan(
        &self,
        request: ShardScanRequest,
        timeout: Option<Duration>,
    ) -> AppResult<ShardScanResponse> {
        let txn = Txn::new(self.clone());
        txn.scan(request, timeout).await
    }

    /// Scan an range.
    pub fn range(
        &self,
        request: RangeRequest,
        timeout: Option<Duration>,
    ) -> AppResult<RangeStream> {
        let txn = Txn::new(self.clone());
        txn.range(request, timeout)
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
