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
use std::time::Duration;

use crate::discovery::StaticServiceDiscovery;
use crate::rpc::{ConnManager, RootClient, Router};
use crate::{AppError, AppResult, Database};

#[derive(Debug, Clone, Default)]
pub struct ClientOptions {
    /// The duration of connection timeout, an error is issued if establish
    /// connection is not finished after the duration.
    pub connect_timeout: Option<Duration>,

    /// The duration of RPC over this client.
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct SekasClient {
    inner: Arc<ClientInner>,
}

#[derive(Debug, Clone)]
struct ClientInner {
    opts: ClientOptions,
    root_client: RootClient,
    router: Router,
    conn_manager: ConnManager,
}

impl SekasClient {
    pub async fn new(opts: ClientOptions, addrs: Vec<String>) -> AppResult<Self> {
        let conn_manager = if let Some(connect_timeout) = opts.connect_timeout {
            ConnManager::with_connect_timeout(connect_timeout)
        } else {
            ConnManager::new()
        };

        let discovery = Arc::new(StaticServiceDiscovery::new(addrs.clone()));
        let root_client = RootClient::new(discovery, conn_manager.clone());
        let router = Router::new(root_client.clone()).await;
        Ok(Self { inner: Arc::new(ClientInner { opts, root_client, router, conn_manager }) })
    }

    pub fn build(
        opts: ClientOptions,
        router: Router,
        root_client: RootClient,
        conn_manager: ConnManager,
    ) -> Self {
        SekasClient { inner: Arc::new(ClientInner { opts, root_client, router, conn_manager }) }
    }

    /// Create a new database if it not exists.
    pub async fn create_database(&self, name: String) -> AppResult<Database> {
        let db_desc = self.inner.root_client.create_database(name).await?;
        Ok(Database::new(self.clone(), db_desc))
    }

    /// Delete a database.
    pub async fn delete_database(&self, name: String) -> AppResult<()> {
        self.inner.root_client.delete_database(name).await?;
        Ok(())
    }

    /// List the databases.
    pub async fn list_database(&self) -> AppResult<Vec<Database>> {
        let databases = self.inner.root_client.list_database().await?;
        Ok(databases.into_iter().map(|desc| Database::new(self.clone(), desc)).collect::<Vec<_>>())
    }

    /// Open a database.
    pub async fn open_database(&self, name: String) -> AppResult<Database> {
        match self.inner.root_client.get_database(name.clone()).await? {
            None => Err(AppError::NotFound(format!("database {}", name))),
            Some(desc) => Ok(Database::new(self.clone(), desc)),
        }
    }

    /// Issue a statement to root.
    #[inline]
    pub async fn handle_statement(&self, statement: &str) -> AppResult<Vec<u8>> {
        Ok(self.inner.root_client.handle_statement(statement).await?)
    }

    /// Return the options.
    #[inline]
    pub fn options(&self) -> &ClientOptions {
        &self.inner.opts
    }

    #[inline]
    pub(crate) fn root_client(&self) -> &RootClient {
        &self.inner.root_client
    }

    #[inline]
    pub(crate) fn router(&self) -> &Router {
        &self.inner.router
    }

    #[inline]
    pub(crate) fn conn_mgr(&self) -> &ConnManager {
        &self.inner.conn_manager
    }
}
