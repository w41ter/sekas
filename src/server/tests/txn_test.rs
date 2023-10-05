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
mod helper;

use std::time::Duration;

use log::info;
use sekas_api::server::v1::TxnState;
use sekas_api::v1::{PutOperation, PutRequest, WriteBatchRequest};
use sekas_client::{AppError, ClientOptions, Error, Partition};

use crate::helper::client::*;
use crate::helper::context::*;
use crate::helper::init::setup_panic_hook;
use crate::helper::runtime::*;

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

#[test]
fn write_batch() {
    block_on_current(async {
        let mut ctx = TestContext::new("txn_test__write_batch");
        ctx.disable_all_balance();
        let nodes = ctx.bootstrap_servers(3).await;
        let c = ClusterClient::new(nodes).await;
        let opts = ClientOptions {
            connect_timeout: Some(Duration::from_millis(50)),
            timeout: Some(Duration::from_millis(200)),
        };
        let client = c.app_client_with_options(opts).await;
        let db = client.create_database("test_db".to_string()).await.unwrap();
        let co = db
            .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 3 }))
            .await
            .unwrap();
        c.assert_collection_ready(&co.desc()).await;

        let req = WriteBatchRequest {
            puts: vec![
                PutRequest {
                    key: vec![b'1', b'2'],
                    value: vec![b'1', b'2'],
                    ttl: 500,
                    conditions: vec![],
                    op: PutOperation::None.into(),
                },
                PutRequest {
                    key: vec![b'2', b'2'],
                    value: vec![b'1', b'2'],
                    ttl: 500,
                    conditions: vec![],
                    op: PutOperation::None.into(),
                },
                PutRequest {
                    key: vec![b'3', b'2'],
                    value: vec![b'1', b'2'],
                    ttl: 500,
                    conditions: vec![],
                    op: PutOperation::None.into(),
                },
            ],
            deletes: vec![],
        };
        co.write_batch(req).await.unwrap();
    });
}
