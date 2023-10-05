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

use sekas_api::server::v1::*;

use crate::{AppError, AppResult, Collection, SekasClient};

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

    pub async fn create_collection(&self, name: String) -> AppResult<Collection> {
        let co_desc = self.client.root_client().create_collection(self.desc.clone(), name).await?;
        Ok(Collection::new(self.client.clone(), co_desc, self.rpc_timeout))
    }

    pub async fn delete_collection(&self, name: String) -> AppResult<()> {
        self.client.root_client().delete_collection(self.desc.clone(), name).await?;
        Ok(())
    }

    pub async fn list_collection(&self) -> AppResult<Vec<Collection>> {
        let collections = self.client.root_client().list_collection(self.desc.clone()).await?;
        Ok(collections
            .into_iter()
            .map(|co_desc| Collection::new(self.client.clone(), co_desc, self.rpc_timeout))
            .collect::<Vec<_>>())
    }

    pub async fn open_collection(&self, name: String) -> AppResult<Collection> {
        match self.client.root_client().get_collection(self.desc.clone(), name.clone()).await? {
            None => Err(AppError::NotFound(format!("collection {}", name))),
            Some(co_desc) => Ok(Collection::new(self.client.clone(), co_desc, self.rpc_timeout)),
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
