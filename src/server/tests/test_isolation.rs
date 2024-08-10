// Copyright 2024-present The Sekas Authors.
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

use helper::client::ClusterClient;
use helper::context::TestContext;
use helper::init::setup_panic_hook;
use helper::runtime::spawn;
use log::info;
use sekas_client::{AppError, Database, TableDesc, Txn, WriteBuilder};
use sekas_rock::fn_name;

const DB: &str = "DB";
const TABLE_A: &str = "TABLE_A";
const TABLE_B: &str = "TABLE_B";

#[ctor::ctor]
fn init() {
    setup_panic_hook();
    tracing_subscriber::fmt::init();
}

/// build a cluster and create a DB and two table.
async fn bootstrap_servers_and_tables(
    name: &str,
) -> (TestContext, ClusterClient, Database, TableDesc, TableDesc) {
    let mut ctx = TestContext::new(name);
    let nodes = ctx.bootstrap_servers(3).await;
    let c = ClusterClient::new(nodes).await;
    let app = c.app_client().await;

    let db = app.create_database(DB.to_string()).await.unwrap();
    let table_a = db.create_table(TABLE_A.to_string()).await.unwrap();
    let table_b = db.create_table(TABLE_B.to_string()).await.unwrap();
    c.assert_table_ready(table_a.id).await;
    c.assert_table_ready(table_b.id).await;

    // ATTN: here is an assumption, two table would not be optimized in one txn
    // batch write.

    (ctx, c, db, table_a, table_b)
}

#[sekas_macro::test]
async fn test_lost_update_anomaly() {
    // The constraint:
    //      r1[x]...w2[x]...w1[x]...c1

    let (ctx, c, db, table_a, _table_b) = bootstrap_servers_and_tables(fn_name!()).await;

    let table_a = table_a.id;
    let loop_times = 100;

    let db_clone = db.clone();
    let bumper_a = spawn(async move {
        for i in 0..loop_times {
            loop {
                let mut txn = db_clone.begin_txn();
                let value = read_i64(&txn, table_a, table_a.to_string().into_bytes()).await;
                let a = value & 0x0000FFFF;
                let b = value & 0xFFFF0000;
                if a != i {
                    panic!("a = {}, i = {}, b = {}, the lost update anomaly is exists", a, i, b);
                }
                let value = b | (a + 1);

                let put = WriteBuilder::new(table_a.to_string().into_bytes())
                    .ensure_put(value.to_be_bytes().to_vec());
                txn.put(table_a, put);
                match txn.commit().await {
                    Ok(_) => break,
                    Err(AppError::TxnConflict) => {
                        info!("bumper a txn is conflict, retry later ...");
                    }
                    Err(err) => panic!("commit txn: {err:?}"),
                }
            }
            sekas_runtime::time::sleep(Duration::from_millis(5)).await;
        }
    });

    let db_clone = db.clone();
    let bumper_b = spawn(async move {
        for i in 0..loop_times {
            loop {
                let mut txn = db_clone.begin_txn();
                let value = read_i64(&txn, table_a, table_a.to_string().into_bytes()).await;
                let a = value & 0x0000FFFF;
                let b = (value & 0xFFFF0000) >> 16;
                if b != i {
                    panic!("b = {}, i = {}, a = {}, the lost update anomaly is exists", b, i, a);
                }
                let value = a | ((b + 1) << 16);

                let put = WriteBuilder::new(table_a.to_string().into_bytes())
                    .ensure_put(value.to_be_bytes().to_vec());
                txn.put(table_a, put);
                match txn.commit().await {
                    Ok(_) => break,
                    Err(AppError::TxnConflict) => {
                        info!("bumper b txn is conflict, retry later ...");
                    }
                    Err(err) => panic!("commit txn: {err:?}"),
                }
            }
            sekas_runtime::time::sleep(Duration::from_millis(3)).await;
        }
    });

    bumper_a.await.unwrap();
    bumper_b.await.unwrap();

    let txn = db.begin_txn();
    let value = read_i64(&txn, table_a, table_a.to_string().into_bytes()).await;
    assert_eq!(value, (100 << 16) | 100);

    drop(c);
    drop(ctx);
}

async fn read_i64(txn: &Txn, table_id: u64, key: Vec<u8>) -> i64 {
    match txn.get(table_id, key).await.unwrap() {
        Some(bytes) => sekas_rock::num::decode_i64(&bytes).unwrap(),
        None => 0,
    }
}
