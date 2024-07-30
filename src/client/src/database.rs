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

use sekas_api::server::v1::*;

use crate::range::{RangeRequest, RangeStream};
use crate::txn::WatchKeyStream;
use crate::{AppError, AppResult, SekasClient, Txn, WriteBuilder};

#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) client: SekasClient,
    pub(crate) desc: DatabaseDesc,
    /// Read value by ignore any versions.
    pub(crate) read_without_version: bool,
}

impl Database {
    /// Create a new database instance.
    pub(crate) fn new(client: SekasClient, desc: DatabaseDesc) -> Self {
        let read_without_version = desc.id == sekas_schema::system::db::ID;
        Database { client, desc, read_without_version }
    }

    /// Create a new table if not exists.
    pub async fn create_table(&self, name: String) -> AppResult<TableDesc> {
        let desc = self.client.root_client().create_table(self.desc.clone(), name).await?;
        Ok(desc)
    }

    /// Delete a specified table.
    pub async fn delete_table(&self, name: String) -> AppResult<()> {
        self.client.root_client().delete_table(self.desc.clone(), name).await?;
        Ok(())
    }

    /// List tables in the database.
    pub async fn list_table(&self) -> AppResult<Vec<TableDesc>> {
        let tables = self.client.root_client().list_table(self.desc.clone()).await?;
        Ok(tables)
    }

    /// Open a table.
    pub async fn open_table(&self, name: String) -> AppResult<TableDesc> {
        match self.client.root_client().get_table(self.desc.clone(), name.clone()).await? {
            None => Err(AppError::NotFound(format!("table {}", name))),
            Some(co_desc) => Ok(co_desc),
        }
    }

    /// A helper function to delete a key.
    #[inline]
    pub async fn delete(&self, table_id: u64, key: Vec<u8>) -> AppResult<()> {
        let mut txn = Txn::new(self.clone());
        txn.delete(table_id, WriteBuilder::new(key).ensure_delete());
        txn.commit().await?;
        Ok(())
    }

    /// A helper function to put a key value.
    #[inline]
    pub async fn put(&self, table_id: u64, key: Vec<u8>, value: Vec<u8>) -> AppResult<()> {
        let mut txn = Txn::new(self.clone());
        txn.put(table_id, WriteBuilder::new(key).ensure_put(value));
        txn.commit().await?;
        Ok(())
    }

    /// Begin a transcation at the database, which supports serializable
    /// snapshot isolation (WIP...)
    #[inline]
    pub fn begin_txn(&self) -> Txn {
        Txn::new(self.clone())
    }

    /// A helper function to get the value of a key.
    #[inline]
    pub async fn get(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Vec<u8>>> {
        let txn = Txn::new(self.clone());
        txn.get(table_id, key).await
    }

    /// A helper function to get the raw value (version, tombstone ...) of a
    /// key.
    #[inline]
    pub async fn get_raw_value(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Value>> {
        let txn = Txn::new(self.clone());
        txn.get_raw_value(table_id, key).await
    }

    /// A helper function to scan an range in a shard.
    #[inline]
    pub async fn scan(&self, request: ShardScanRequest) -> AppResult<ShardScanResponse> {
        let txn = Txn::new(self.clone());
        txn.scan(request).await
    }

    /// A helper function to scan an range.
    pub async fn range(&self, request: RangeRequest) -> AppResult<RangeStream> {
        let txn = Txn::new(self.clone());
        txn.range(request).await
    }

    /// A helper function to watch a key.
    pub async fn watch(&self, table_id: u64, key: &[u8]) -> AppResult<WatchKeyStream> {
        Txn::new(self.clone()).watch(table_id, key).await
    }

    /// Watch an key with version.
    ///
    /// The values below this version are ignored.
    pub async fn watch_with_version(
        &self,
        table_id: u64,
        key: &[u8],
        version: u64,
    ) -> AppResult<WatchKeyStream> {
        Txn::new(self.clone()).watch_with_version(table_id, key, version).await
    }

    /// Return the name of the database.
    #[allow(dead_code)]
    pub fn name(&self) -> String {
        self.desc.name.to_owned()
    }

    /// Return the desc of the database.
    #[inline]
    pub fn desc(&self) -> DatabaseDesc {
        self.desc.clone()
    }
}
