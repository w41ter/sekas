// Copyright 2024-present The Sekas Authors.
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

use sekas_api::server::v1::*;
use tonic::{Request, Response, Status};

use super::metrics::*;
use crate::root::Watcher;
use crate::{record_latency, Error, Result, Server};

#[tonic::async_trait]
impl root_server::Root for Server {
    type WatchStream = Watcher;

    async fn admin(&self, req: Request<AdminRequest>) -> Result<Response<AdminResponse>, Status> {
        record_latency!(take_admin_request_metrics());
        let req = req.into_inner();
        let res = self.handle_admin(req).await?;
        Ok(Response::new(res))
    }

    async fn watch(
        &self,
        req: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        record_latency!(take_watch_request_metrics());
        let req = req.into_inner();
        let watcher = self.wrap(self.root.watch(req.cur_group_epochs).await).await?;
        Ok(Response::new(watcher))
    }

    async fn join(
        &self,
        request: Request<JoinNodeRequest>,
    ) -> Result<Response<JoinNodeResponse>, Status> {
        record_latency!(take_join_request_metrics());
        let request = request.into_inner();
        let capacity = request
            .capacity
            .ok_or_else(|| Error::InvalidArgument("capacity is required".into()))?;
        let (cluster_id, node, root) =
            self.wrap(self.root.join(request.addr, capacity).await).await?;
        Ok::<Response<JoinNodeResponse>, Status>(Response::new(JoinNodeResponse {
            cluster_id,
            node_id: node.id,
            root: Some(root),
        }))
    }

    async fn report(
        &self,
        request: Request<ReportRequest>,
    ) -> Result<Response<ReportResponse>, Status> {
        record_latency!(take_report_request_metrics());
        let request = request.into_inner();
        self.wrap(self.root.report(request.updates).await).await?;
        Ok(Response::new(ReportResponse {}))
    }

    async fn alloc_replica(
        &self,
        request: Request<AllocReplicaRequest>,
    ) -> Result<Response<AllocReplicaResponse>, Status> {
        record_latency!(take_alloc_replica_request_metrics());
        let req = request.into_inner();
        let replicas = self
            .wrap(self.root.alloc_replica(req.group_id, req.epoch, req.num_required).await)
            .await?;
        Ok(Response::new(AllocReplicaResponse { replicas }))
    }

    async fn alloc_txn_id(
        &self,
        request: Request<AllocTxnIdRequest>,
    ) -> Result<Response<AllocTxnIdResponse>, Status> {
        let req = request.into_inner();

        let base_txn_id = self.wrap(self.root.alloc_txn_id(req.num_required).await).await?;
        Ok(Response::new(AllocTxnIdResponse { base_txn_id, num: req.num_required }))
    }
}

impl Server {
    async fn handle_admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let mut res = AdminResponse::default();
        let req = req.request.ok_or_else(|| Error::InvalidArgument("AdminRequest".into()))?;
        res.response = Some(self.wrap(self.handle_admin_request(req).await).await?);
        Ok(res)
    }

    async fn handle_admin_request(
        &self,
        req: admin_request::Request,
    ) -> Result<admin_response::Response> {
        use admin_request::Request;
        use admin_response::Response;

        let res = match req {
            Request::CreateDatabase(req) => {
                let res = self.handle_create_database(req).await?;
                Response::CreateDatabase(res)
            }
            Request::UpdateDatabase(_req) => {
                todo!()
            }
            Request::DeleteDatabase(req) => {
                let res = self.handle_delete_database(req).await?;
                Response::DeleteDatabase(res)
            }
            Request::GetDatabase(req) => {
                let res = self.handle_get_database(req).await?;
                Response::GetDatabase(res)
            }
            Request::ListDatabases(req) => {
                let res = self.handle_list_database(req).await?;
                Response::ListDatabases(res)
            }
            Request::CreateTable(req) => {
                let res = self.handle_create_table(req).await?;
                Response::CreateTable(res)
            }
            Request::UpdateTable(_req) => {
                todo!()
            }
            Request::DeleteTable(req) => {
                let res = self.handle_delete_table(req).await?;
                Response::DeleteTable(res)
            }
            Request::GetTable(req) => {
                let res = self.handle_get_table(req).await?;
                Response::GetTable(res)
            }
            Request::ListTables(req) => {
                let res = self.handle_list_table(req).await?;
                Response::ListTables(res)
            }
            Request::Statement(req) => {
                let res = self.handle_statement(req).await?;
                Response::Statement(res)
            }
        };
        Ok(res)
    }

    async fn handle_create_database(
        &self,
        req: CreateDatabaseRequest,
    ) -> Result<CreateDatabaseResponse> {
        let desc = self.root.create_database(req.name).await?;
        Ok(CreateDatabaseResponse { database: Some(desc) })
    }

    async fn handle_delete_database(
        &self,
        req: DeleteDatabaseRequest,
    ) -> Result<DeleteDatabaseResponse> {
        self.root.delete_database(&req.name).await?;
        Ok(DeleteDatabaseResponse {})
    }

    async fn handle_get_database(&self, req: GetDatabaseRequest) -> Result<GetDatabaseResponse> {
        let database = self.root.get_database(&req.name).await?;
        Ok(GetDatabaseResponse { database })
    }

    async fn handle_list_database(
        &self,
        _req: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse> {
        let databases = self.root.list_database().await?;
        Ok(ListDatabasesResponse { databases })
    }

    async fn handle_create_table(&self, req: CreateTableRequest) -> Result<CreateTableResponse> {
        let database = req
            .database
            .ok_or_else(|| Error::InvalidArgument("CreateTableRequest::database".to_owned()))?;
        let desc = self.root.create_table(req.name, database.name).await?;
        Ok(CreateTableResponse { table: Some(desc) })
    }

    async fn handle_delete_table(&self, req: DeleteTableRequest) -> Result<DeleteTableResponse> {
        let database = req.database.ok_or_else(|| {
            Error::InvalidArgument("DeleteTableRequest::database is required".to_owned())
        })?;
        self.root.delete_table(&req.name, &database).await?;
        Ok(DeleteTableResponse {})
    }

    async fn handle_get_table(&self, req: GetTableRequest) -> Result<GetTableResponse> {
        let database = req.database.ok_or_else(|| {
            Error::InvalidArgument("GetTableRequest::database is required".to_owned())
        })?;
        let table = self.root.get_table(&req.name, &database).await?;
        Ok(GetTableResponse { table })
    }

    async fn handle_list_table(&self, req: ListTablesRequest) -> Result<ListTablesResponse> {
        let database = req.database.ok_or_else(|| {
            Error::InvalidArgument("ListTableRequest::database is required".to_owned())
        })?;
        let tables = self.root.list_table(&database).await?;
        Ok(ListTablesResponse { tables })
    }

    async fn handle_statement(&self, req: StatementRequest) -> Result<StatementResponse> {
        let json_body = self.root.handle_statement(&req.statement).await?;
        Ok(StatementResponse { json_body })
    }

    async fn wrap<T>(&self, result: Result<T>) -> Result<T> {
        match result {
            Err(Error::NotRootLeader(..) | Error::GroupNotFound(_)) => {
                let roots = self.node.get_root().await;
                Err(Error::NotRootLeader(roots, 0, None))
            }
            Err(Error::NotLeader(_, term, leader)) => {
                let roots = self.node.get_root().await;
                Err(Error::NotRootLeader(roots, term, leader))
            }
            Err(
                e @ (Error::Forward(_)
                | Error::EpochNotMatch(_)
                | Error::ServiceIsBusy(_)
                | Error::GroupNotReady(_)),
            ) => {
                panic!("root should not returns {e:?}");
            }
            _ => result,
        }
    }
}
