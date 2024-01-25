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

use ::sekas_client::{Table, Database};
use sekas_api::v1::*;
use tonic::{Request, Response, Status};

use super::ProxyServer;
use crate::service::metrics::take_database_request_metrics;
use crate::{record_latency, Error};

#[tonic::async_trait]
impl sekas_server::Sekas for ProxyServer {
    async fn admin(
        &self,
        request: Request<AdminRequest>,
    ) -> Result<Response<AdminResponse>, Status> {
        use sekas_api::v1::admin_request_union::Request;
        use sekas_api::v1::admin_response_union::Response;
        let req = request.into_inner().request.and_then(|r| r.request).ok_or_else(|| {
            Error::InvalidArgument(
                "AdminRequest::request or AdminRequestUnion::request is required".to_owned(),
            )
        })?;
        let resp = match req {
            Request::GetDatabase(req) => Response::GetDatabase(self.get_database(req).await?),
            Request::ListDatabases(req) => Response::ListDatabases(self.list_database(req).await?),
            Request::CreateDatabase(req) => {
                Response::CreateDatabase(self.create_database(req).await?)
            }
            Request::UpdateDatabase(req) => {
                Response::UpdateDatabase(self.update_database(req).await?)
            }
            Request::DeleteDatabase(req) => {
                Response::DeleteDatabase(self.delete_database(req).await?)
            }
            Request::GetTable(req) => Response::GetTable(self.get_table(req).await?),
            Request::ListTables(req) => {
                Response::ListTables(self.list_tables(req).await?)
            }
            Request::CreateTable(req) => {
                Response::CreateTable(self.create_table(req).await?)
            }
            Request::UpdateTable(req) => {
                Response::UpdateTable(self.update_table(req).await?)
            }
            Request::DeleteTable(req) => {
                Response::DeleteTable(self.delete_table(req).await?)
            }
        };

        Ok(tonic::Response::new(AdminResponse {
            response: Some(AdminResponseUnion { response: Some(resp) }),
        }))
    }

    async fn database(
        &self,
        request: Request<DatabaseRequest>,
    ) -> Result<Response<DatabaseResponse>, Status> {
        use sekas_api::v1::table_request_union::Request;
        use sekas_api::v1::table_response_union::Response;

        let request = request.into_inner();
        let request = request.request.ok_or_else(|| {
            Error::InvalidArgument("DatabaseRequest::request is required".to_owned())
        })?;
        let table = request.table.ok_or_else(|| {
            Error::InvalidArgument("TableRequest::table is required".to_owned())
        })?;
        let request = request.request.and_then(|r| r.request).ok_or_else(|| {
            Error::InvalidArgument(
                "TableRequest::request or TableRequestUnion is required".to_owned(),
            )
        })?;
        record_latency!(take_database_request_metrics(&request));
        let resp = match request {
            Request::Get(req) => Response::Get(self.handle_get(table, req).await?),
            Request::Put(req) => Response::Put(self.handle_put(table, req).await?),
            Request::Delete(req) => Response::Delete(self.handle_delete(table, req).await?),
            Request::Batch(req) => Response::Batch(self.handle_batch(table, req).await?),
        };
        Ok(tonic::Response::new(DatabaseResponse {
            response: Some(TableResponse {
                response: Some(TableResponseUnion { response: Some(resp) }),
            }),
        }))
    }
}

impl ProxyServer {
    async fn get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse, Status> {
        let database = self.client.open_database(req.name).await?;
        Ok(GetDatabaseResponse { database: Some(database.desc()) })
    }

    async fn list_database(
        &self,
        _req: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse, Status> {
        let databases = self.client.list_database().await?.into_iter().map(|d| d.desc()).collect();
        Ok(ListDatabasesResponse { databases })
    }

    async fn create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse, Status> {
        let database = self.client.create_database(req.name).await?;
        Ok(CreateDatabaseResponse { database: Some(database.desc()) })
    }

    async fn update_database(
        &self,
        _req: UpdateDatabaseRequest,
    ) -> Result<UpdateDatabaseResponse, Status> {
        Err(Status::unimplemented("ProxyServer::update_database"))
    }

    async fn delete_database(
        &self,
        req: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse, Status> {
        self.client.delete_database(req.name).await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn get_table(
        &self,
        req: GetTableRequest,
    ) -> Result<GetTableResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("GetTableRequest::database is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        let table = database.open_table(name).await?;
        Ok(GetTableResponse { table: Some(table.desc()) })
    }

    async fn list_tables(
        &self,
        req: ListTablesRequest,
    ) -> Result<ListTablesResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("ListTableRequest::database is required".to_owned())
        })?;
        let database = Database::new(self.client.clone(), desc, None);
        let tables = database.list_table().await?.into_iter().map(|c| c.desc()).collect();
        Ok(ListTablesResponse { tables })
    }

    async fn create_table(
        &self,
        req: CreateTableRequest,
    ) -> Result<CreateTableResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("CreateTableRequest::database is required".to_owned())
        })?;
        let partition = req.partition.ok_or_else(|| {
            Error::InvalidArgument("CreateTableRequest::partition is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        let table = database.create_table(name, Some(partition.into())).await?;
        Ok(CreateTableResponse { table: Some(table.desc()) })
    }

    async fn update_table(
        &self,
        _req: UpdateTableRequest,
    ) -> Result<UpdateTableResponse, Status> {
        Err(Status::unimplemented("ProxyServer::update_table"))
    }

    async fn delete_table(
        &self,
        req: DeleteTableRequest,
    ) -> Result<DeleteTableResponse, Status> {
        let desc = req.database.ok_or_else(|| {
            Error::InvalidArgument("DeleteTableRequest::database is required".to_owned())
        })?;
        let name = req.name;
        let database = Database::new(self.client.clone(), desc, None);
        database.delete_table(name).await?;
        Ok(DeleteTableResponse {})
    }
}

impl ProxyServer {
    async fn handle_get(
        &self,
        desc: TableDesc,
        req: GetRequest,
    ) -> Result<GetResponse, Status> {
        let table = Table::new(self.client.clone(), desc, None);
        let resp = table.get(req.key).await?;
        Ok(GetResponse { value: resp })
    }

    async fn handle_put(
        &self,
        desc: TableDesc,
        req: PutRequest,
    ) -> Result<PutResponse, Status> {
        let table = Table::new(self.client.clone(), desc, None);
        table
            .put(req.key, req.value, Some(req.ttl), PutOperation::from_i32(req.op), req.conditions)
            .await?;
        Ok(PutResponse {})
    }

    async fn handle_delete(
        &self,
        desc: TableDesc,
        req: DeleteRequest,
    ) -> Result<DeleteResponse, Status> {
        let table = Table::new(self.client.clone(), desc, None);
        table.delete(req.key, req.conditions).await?;
        Ok(DeleteResponse {})
    }

    async fn handle_batch(
        &self,
        desc: TableDesc,
        req: WriteBatchRequest,
    ) -> Result<WriteBatchResponse, Status> {
        let table = Table::new(self.client.clone(), desc, None);
        Ok(table.write_batch(req).await?)
    }
}
