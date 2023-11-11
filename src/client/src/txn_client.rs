// Copyright 2023 The Sekas Authors.
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

use prost::Message;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_rock::time::{timestamp, timestamp_millis};
use sekas_schema::system::{col, keys};

use crate::{
    AppResult, Collection, ConnManager, Error, GroupClient, Result, RootClient, Router,
    SekasClient, WriteBuilder,
};

// Default txn ttl is 5 seconds.
const TXN_TTL: u64 = 5;
const TXN_TIMEOUT: Duration = Duration::from_secs(5);

struct TxnStateTable {
    txn_col: Collection,
}

impl TxnStateTable {
    pub fn new(client: SekasClient) -> Self {
        TxnStateTable { txn_col: Collection::new(client, col::txn_desc(), Some(TXN_TIMEOUT)) }
    }

    /// Begin a new transaction with the specified txn version.
    pub async fn begin_txn(&self, start_version: u64) -> Result<()> {
        let record = TxnRecord {
            state: TxnState::Running.into(),
            heartbeat: timestamp_millis(),
            ttl: TXN_TTL,
            commit_version: None,
        };
        let key = keys::txn_record_key(start_version);
        let value = record.encode_to_vec();
        let request = ShardWriteRequest {
            shard_id: 0,
            puts: vec![WriteBuilder::new(key)
                .expect_not_exists()
                .take_prev_value()
                .ensure_put(value)],
            ..Default::default()
        };
        let resp = match self.txn_col.write(request).await {
            Ok(resp) => resp,
            Err(Error::CasFailed(idx, cond_idx, prev_value)) => {
                todo!()
            }
            Err(err) => return Err(err),
        };
        let Some(put_resp) = resp.puts.first() else {
            return Err(Error::Internal(format!("put response is empty").into()));
        };

        todo!()
    }

    /// Update the txn heartbeat.
    pub async fn heartbeat(&self, start_version: u64) -> Result<()> {
        todo!()
    }

    /// Commit the transaction specified by `start_version`.
    ///
    /// What happen if the commit txn already aborted.
    pub async fn commit_txn(&self, start_version: u64, commit_version: u64) -> Result<()> {
        todo!()
    }

    /// Get the corresponding txn record.
    pub async fn get_txn_record(&self, start_version: u64) -> Result<Option<TxnRecord>> {
        let key = keys::txn_record_key(start_version);
        // FIXME(walter) get key from the specified shard.
        let Some(value) = self.txn_col.get(key).await? else {
            return Ok(None);
        };

        let txn_record: TxnRecord = TxnRecord::decode(&*value)
            .map_err(|e| Error::Internal(format!("decode TxnRecord: {e}").into()))?;
        Ok(Some(txn_record))
    }

    /// Abort the transaction specified by `start_version`.
    ///
    /// What happend if the txn has been committed.
    pub async fn abort_txn(&self, start_version: u64) -> Result<()> {
        todo!()
    }
}
