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

use std::time::{Duration, Instant};

use futures::StreamExt;
use log::{trace, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_rock::num::decode_i64;
use sekas_runtime::sync::OnceCell;
use sekas_schema::system::txn::TXN_MAX_VERSION;
use tokio::sync::mpsc;

use crate::group_client::GroupClient;
use crate::metrics::*;
use crate::range::RangeStream;
use crate::retry::RetryState;
use crate::{
    AppResult, Database, Error, RangeRequest, Result, SekasClient, TxnStateTable, record_latency,
};

const TXN_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Default, Clone, Copy)]
pub struct TxnReadOptions {
    /// Whether point reads should overlay writes buffered in this transaction.
    pub overlay_writes: bool,
}

#[derive(Debug, Default, Clone)]
pub struct WriteBatchResponse {
    /// The version of this batch.
    pub version: u64,
    /// The prev value of the target key.
    ///
    /// Only for the requests with `take_prev_value`.
    pub deletes: Vec<Option<Value>>,
    /// The prev value of the target key.
    ///
    /// Only for the requests with `take_prev_value`.
    pub puts: Vec<Option<Value>>,
}

/// A structure to build write request.
pub struct WriteBuilder {
    /// The key to operate.
    key: Vec<u8>,
    /// The cas conditions.
    conditions: Vec<WriteCondition>,
    /// The TTL of key.
    ttl: Option<u64>,
    /// Whether to take prev values.
    take_prev_value: bool,
}

/// A structure to support snapshot-isolated transaction.
pub struct Txn {
    /// The database to submit transactions.
    db: Database,
    /// The deadline of this txn. The expired txn will be aborted.
    ///
    /// The default value is inherited from database, and it can be overwrite by
    /// [`Txn::set_timeout`].
    deadline: Option<Instant>,
    /// The transaction start version.
    start_version: OnceCell<u64>,
    /// The put request to submit.
    puts: Vec<(u64, PutRequest)>,
    /// The delete request to submit.
    deletes: Vec<(u64, DeleteRequest)>,
}

/// A structure to hold the context about single write request.
struct WriteContext {
    /// The id of table to write.
    table_id: u64,
    /// The request.
    request: WriteRequest,
    /// The response.
    response: Option<WriteResponse>,
    /// The index in the request batch.
    index: usize,
    /// Is this request has been accepted.
    done: bool,
}

/// A structure to hold the context about a write batch request.
pub struct WriteBatchContext {
    client: SekasClient,

    writes: Vec<WriteContext>,
    /// The number of doing requests.
    num_doing_writes: usize,
    /// The number of delete requests in this batch.
    num_deletes: usize,

    start_version: u64,
    commit_version: u64,
    txn_started: bool,

    retry_state: RetryState,
}

impl WriteBuilder {
    pub fn new(key: Vec<u8>) -> Self {
        WriteBuilder { key, conditions: vec![], ttl: None, take_prev_value: false }
    }

    /// With ttl, in seconds. (WIP)
    ///
    /// Only works for put request.
    pub fn with_ttl(mut self, ttl: Option<u64>) -> Self {
        self.ttl = ttl;
        self
    }

    /// Build a put request.
    pub fn put(self, value: Vec<u8>) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::None.into(),
            key: self.key,
            value,
            ttl: self.ttl.unwrap_or_default(),
            take_prev_value: self.take_prev_value,
            conditions: self.conditions,
        })
    }

    /// Build a put request without any error.
    pub fn ensure_put(self, value: Vec<u8>) -> PutRequest {
        self.put(value).expect("Invalid put conditions")
    }

    /// Build a delete request.
    pub fn delete(self) -> AppResult<DeleteRequest> {
        self.verify_conditions()?;
        Ok(DeleteRequest {
            key: self.key,
            conditions: self.conditions,
            take_prev_value: self.take_prev_value,
        })
    }

    /// Build a delete request without any error.
    pub fn ensure_delete(self) -> DeleteRequest {
        self.delete().expect("Invalid delete conditions")
    }

    /// Build a nop request.
    pub fn nop(self) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::Nop.into(),
            key: self.key,
            value: vec![],
            ttl: 0,
            conditions: self.conditions,
            take_prev_value: self.take_prev_value,
        })
    }

    /// Build a nop request without any error.
    pub fn ensure_nop(self) -> PutRequest {
        self.nop().expect("Invalid nop conditions")
    }

    /// Build an add request, the value will be interpreted as i64.
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, val: i64) -> AppResult<PutRequest> {
        self.verify_conditions()?;
        Ok(PutRequest {
            put_type: PutType::AddI64.into(),
            key: self.key,
            value: val.to_be_bytes().to_vec(),
            ttl: self.ttl.unwrap_or_default(),
            conditions: self.conditions,
            take_prev_value: self.take_prev_value,
        })
    }

    /// Build an add request without any error, the value will be interpreted as
    /// i64.
    pub fn ensure_add(self, val: i64) -> PutRequest {
        self.add(val).expect("Invalid add conditions")
    }

    /// Expect that the max version of the key is less than the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_lt(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionLt.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is less than or equals to the
    /// input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_le(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionLe.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is great than the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_gt(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionGt.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is great than or equal to the
    /// input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version_ge(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersionGe.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the max version of the key is equal to the input value.
    ///
    /// One request only can contains one version related expection.
    pub fn expect_version(mut self, expect: u64) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectVersion.into(),
            version: expect,
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is exists exists.
    pub fn expect_exists(mut self) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectExists.into(),
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is not exists.
    pub fn expect_not_exists(mut self) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectNotExists.into(),
            ..Default::default()
        });
        self
    }

    /// Expect that the target key is equal to the input value.
    pub fn expect_value(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectValue.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the key contains the input value.
    pub fn expect_contains(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectContains.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the slice of the value is equal to the input value.
    pub fn expect_slice(mut self, begin: u64, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectSlice.into(),
            value,
            begin,
            ..Default::default()
        });
        self
    }

    /// Expect that the value of key is starts with the input value.
    pub fn expect_starts_with(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectStartsWith.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Expect that the value of key is ends with the input value.
    pub fn expect_ends_with(mut self, value: Vec<u8>) -> Self {
        self.conditions.push(WriteCondition {
            r#type: WriteConditionType::ExpectEndsWith.into(),
            value,
            ..Default::default()
        });
        self
    }

    /// Take the prev value.
    ///
    /// It is useful for cas operations. Default is `false`.
    pub fn take_prev_value(mut self) -> Self {
        self.take_prev_value = true;
        self
    }

    fn verify_conditions(&self) -> AppResult<()> {
        // TODO(walter) check conditions
        Ok(())
    }
}

impl Txn {
    pub(crate) fn new(db: Database) -> Self {
        let deadline = db.client.options().timeout.map(|v| Instant::now() + v);
        Txn {
            db,
            deadline,
            start_version: OnceCell::new(),
            puts: Vec::default(),
            deletes: Vec::default(),
        }
    }

    /// Issue a delete request to transaction.
    #[inline]
    pub fn delete(&mut self, table_id: u64, delete_req: DeleteRequest) {
        self.deletes.push((table_id, delete_req));
    }

    /// Issue a put request to transaction.
    #[inline]
    pub fn put(&mut self, table_id: u64, put_req: PutRequest) {
        self.puts.push((table_id, put_req));
    }

    /// Commit this transaction.
    pub async fn commit(self) -> AppResult<WriteBatchResponse> {
        let start_version = self.get_start_version().await?;
        let ctx = WriteBatchContext::new(
            start_version,
            self.deletes,
            self.puts,
            self.db.client.clone(),
            self.deadline,
        );
        Ok(ctx.commit().await?)
    }

    /// Return the transaction start version, allocating one if necessary.
    pub async fn start_version(&self) -> AppResult<u64> {
        Ok(self.get_start_version().await?)
    }

    /// Get key value within a transaction.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn get(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Vec<u8>>> {
        let value = self.get_raw_value(table_id, key).await?;
        Ok(value.and_then(|v| v.content))
    }

    /// Get key value with explicit read options.
    pub async fn get_with_options(
        &self,
        table_id: u64,
        key: Vec<u8>,
        options: TxnReadOptions,
    ) -> AppResult<Option<Vec<u8>>> {
        let value = self.get_raw_value_with_options(table_id, key, options).await?;
        Ok(value.and_then(|v| v.content))
    }

    /// Get a raw key value from this transaction.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn get_raw_value(&self, table_id: u64, key: Vec<u8>) -> AppResult<Option<Value>> {
        self.get_raw_value_with_options(table_id, key, TxnReadOptions::default()).await
    }

    /// Get a raw key value with explicit read options.
    pub async fn get_raw_value_with_options(
        &self,
        table_id: u64,
        key: Vec<u8>,
        options: TxnReadOptions,
    ) -> AppResult<Option<Value>> {
        CLIENT_DATABASE_BYTES_TOTAL.rx.inc_by(key.len() as u64);
        CLIENT_DATABASE_REQUEST_TOTAL.get.inc();
        record_latency!(&CLIENT_DATABASE_REQUEST_DURATION_SECONDS.get);
        if options.overlay_writes
            && let Some(value) = self.local_write_value(table_id, &key).await?
        {
            CLIENT_DATABASE_BYTES_TOTAL
                .tx
                .inc_by(value.content.as_ref().map(Vec::len).unwrap_or_default() as u64);
            return Ok(Some(value));
        }
        let mut retry_state = RetryState::with_deadline_opt(self.deadline);

        loop {
            match self.get_inner(table_id, &key, retry_state.timeout()).await {
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
        table_id: u64,
        user_key: &[u8],
        timeout: Option<Duration>,
    ) -> crate::Result<Option<Value>> {
        let start_version = self.get_read_version().await?;
        let router = self.db.client.router();
        let (group, shard) = router.find_shard(table_id, user_key)?;
        let req = Request::Get(ShardGetRequest {
            shard_id: shard.id,
            start_version,
            user_key: user_key.to_owned(),
        });

        trace!(
            "get key from shard {}, group: {}, start version: {}",
            shard.id, group.id, start_version
        );

        let mut group_client = GroupClient::new(group, self.db.client.clone());
        group_client.set_timeout_opt(timeout);
        match group_client.request(&req).await? {
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
        let mut retry_state = RetryState::with_deadline_opt(self.deadline);
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
        let router = self.db.client.router();
        let group_state = router.find_group_by_shard(request.shard_id)?;
        let mut group_client = GroupClient::new(group_state, self.db.client.clone());
        group_client.set_timeout_opt(timeout);
        let request = Request::Write(request.clone());
        match group_client.request(&request).await? {
            Response::Write(resp) => Ok(resp),
            _ => Err(crate::Error::Internal("invalid response type, Write is required".into())),
        }
    }

    /// To scan a shard.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn scan(&self, mut request: ShardScanRequest) -> AppResult<ShardScanResponse> {
        let mut retry_state = RetryState::with_deadline_opt(self.deadline);
        loop {
            match self.scan_inner(&mut request, retry_state.timeout()).await {
                Ok(value) => {
                    return Ok(value);
                }
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn scan_inner(
        &self,
        request: &mut ShardScanRequest,
        timeout: Option<Duration>,
    ) -> crate::Result<ShardScanResponse> {
        request.start_version = self.get_read_version().await?;
        let router = self.db.client.router();
        let group_state = router.find_group_by_shard(request.shard_id)?;
        let request = Request::Scan(request.clone());
        let mut group_client = GroupClient::new(group_state, self.db.client.clone());
        group_client.set_timeout_opt(timeout);
        match group_client.request(&request).await? {
            Response::Scan(resp) => Ok(resp),
            _ => Err(crate::Error::Internal("invalid response type, Scan is required".into())),
        }
    }

    /// Scan an range.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn range(&self, mut request: RangeRequest) -> AppResult<RangeStream> {
        if request.version.is_none() {
            request.version = Some(self.get_read_version().await?);
        }
        Ok(RangeStream::init(self.db.client.clone(), request, self.deadline))
    }

    /// Watch an key.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn watch(&self, table_id: u64, key: &[u8]) -> AppResult<WatchKeyStream> {
        self.watch_with_version(table_id, key, 0).await
    }

    /// Watch an key with version.
    ///
    /// The values below this version are ignored.
    ///
    /// NOTE: This request will be sent to node servers, and the put/delete
    /// requests already buffered in this TXN will be ignored.
    pub async fn watch_with_version(
        &self,
        table_id: u64,
        key: &[u8],
        version: u64,
    ) -> AppResult<WatchKeyStream> {
        // TODO(walter) watch a key might have different deadline.
        let mut retry_state = RetryState::with_deadline_opt(self.deadline);
        let (sender, receiver) = mpsc::unbounded_channel();
        let db = self.db.clone();
        let user_key = key.to_vec();
        let _handler = sekas_runtime::spawn(async move {
            let mut ctx = WatchContext { table_id, version, user_key, sender };
            while let Err(err) = watch_key(&mut ctx, &db, retry_state.timeout()).await {
                if let Err(err) = retry_state.retry(err).await {
                    if ctx.sender.send(Err(err.into())).is_err() {
                        break;
                    }
                }
            }
        });

        Ok(WatchKeyStream { _handler, receiver })
    }

    async fn get_start_version(&self) -> crate::Result<u64> {
        trace!("txn get start version");
        let timeout = self.deadline.map(|d| d.saturating_duration_since(Instant::now()));
        self.start_version
            .get_or_try_init(|| async {
                self.db.client.root_client().alloc_txn_id(1, timeout).await
            })
            .await
            .copied()
    }

    async fn get_read_version(&self) -> crate::Result<u64> {
        if self.db.read_without_version {
            Ok(TXN_MAX_VERSION)
        } else {
            self.get_start_version().await
        }
    }

    async fn local_write_value(&self, table_id: u64, key: &[u8]) -> AppResult<Option<Value>> {
        enum LocalWrite<'a> {
            Put(&'a PutRequest),
            Delete,
        }

        let mut local_writes = Vec::new();
        for (write_table_id, delete) in &self.deletes {
            if *write_table_id == table_id && delete.key == key {
                local_writes.push(LocalWrite::Delete);
            }
        }
        for (write_table_id, put) in &self.puts {
            if *write_table_id == table_id && put.key == key {
                local_writes.push(LocalWrite::Put(put));
            }
        }

        if local_writes.is_empty() {
            return Ok(None);
        }

        let start_version = self.get_read_version().await?;
        let mut value = self
            .get_inner(
                table_id,
                key,
                self.deadline.map(|d| d.saturating_duration_since(Instant::now())),
            )
            .await?;
        for local_write in local_writes {
            match local_write {
                LocalWrite::Put(put) => match put.put_type() {
                    PutType::None => {
                        value = Some(Value::with_value(put.value.clone(), start_version));
                    }
                    PutType::Nop => {}
                    PutType::AddI64 => {
                        let delta = decode_i64(&put.value).ok_or_else(|| {
                            Error::InvalidArgument("input value is not a valid i64".into())
                        })?;
                        let former_value = match value.as_ref().and_then(|v| v.content.as_ref()) {
                            Some(content) => decode_i64(content).ok_or_else(|| {
                                Error::InvalidArgument("the exists value is not a valid i64".into())
                            })?,
                            None => 0,
                        };
                        value = Some(Value::with_value(
                            former_value.wrapping_add(delta).to_be_bytes().to_vec(),
                            start_version,
                        ));
                    }
                },
                LocalWrite::Delete => {
                    value = Some(Value::tombstone(start_version));
                }
            }
        }
        Ok(value)
    }
}

impl WriteContext {
    fn with_put((index, (table_id, put)): (usize, (u64, PutRequest))) -> Self {
        WriteContext {
            table_id,
            request: WriteRequest::Put(put),
            response: None,
            index,
            done: false,
        }
    }

    fn with_delete((index, (table_id, delete)): (usize, (u64, DeleteRequest))) -> Self {
        WriteContext {
            table_id,
            request: WriteRequest::Delete(delete),
            response: None,
            index,
            done: false,
        }
    }

    fn user_key(&self) -> &[u8] {
        match &self.request {
            WriteRequest::Put(put) => &put.key,
            WriteRequest::Delete(del) => &del.key,
        }
    }
}

impl WriteBatchContext {
    fn new(
        start_version: u64,
        deletes: Vec<(u64, DeleteRequest)>,
        puts: Vec<(u64, PutRequest)>,
        client: SekasClient,
        deadline: Option<Instant>,
    ) -> Self {
        let num_deletes = deletes.len();
        let num_puts = puts.len();
        let num_doing_writes = num_deletes + num_puts;
        let mut writes = Vec::with_capacity(num_doing_writes);
        writes.extend(deletes.into_iter().enumerate().map(WriteContext::with_delete));
        writes.extend(
            puts.into_iter()
                .enumerate()
                .map(|(idx, put)| (num_deletes + idx, put))
                .map(WriteContext::with_put),
        );

        WriteBatchContext {
            client,
            writes,
            num_deletes,
            num_doing_writes,
            start_version,
            commit_version: 0,
            txn_started: false,
            retry_state: RetryState::with_deadline_opt(deadline),
        }
    }

    pub async fn commit(mut self) -> Result<WriteBatchResponse> {
        self.start_txn().await?;
        self.txn_started = true;

        let start_version = self.start_version;
        let txn_table = TxnStateTable::new(self.client.clone(), self.retry_state.timeout());

        trace!(
            "commit txn, verison: {}, timeout: {:?}",
            self.start_version,
            self.retry_state.timeout()
        );

        let resp = tokio::select! {
            _ = Self::lease_txn(txn_table, start_version) => {
                unreachable!()
            },
            resp = self.commit_inner() => {
                resp
            }
        };

        match resp {
            Ok(resp) => {
                self.commit_intents();
                Ok(resp)
            }
            Err(err) => self.finish_after_error(err).await,
        }
    }

    async fn lease_txn(txn_table: TxnStateTable, start_version: u64) -> ! {
        loop {
            if let Err(err) = txn_table.heartbeat(start_version).await {
                warn!("txn {start_version} lease heartbeat: {err}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn commit_inner(&mut self) -> Result<WriteBatchResponse> {
        self.prepare_intents().await?;
        self.commit_version = self.alloc_txn_version().await?;

        trace!(
            "commit txn, alloc txn version: {}, start version: {}",
            self.commit_version, self.start_version
        );

        self.commit_txn().await?;
        Ok(self.take_response(self.commit_version))
    }

    fn take_response(&mut self, version: u64) -> WriteBatchResponse {
        let mut deletes = Vec::with_capacity(self.num_deletes);
        let mut puts = Vec::with_capacity(self.writes.len() - self.num_deletes);
        for write in &mut self.writes {
            match &write.request {
                WriteRequest::Delete(_) => {
                    deletes.push(write.response.take().and_then(|v| v.prev_value));
                }
                WriteRequest::Put(_) => {
                    puts.push(write.response.take().and_then(|v| v.prev_value));
                }
            }
        }

        WriteBatchResponse { version, deletes, puts }
    }

    async fn finish_after_error(mut self, err: Error) -> Result<WriteBatchResponse> {
        if !self.txn_started {
            return Err(err);
        }

        if let Some(resp) = self.try_finish_committed_txn().await? {
            return Ok(resp);
        }

        if let Err(abort_err) = self.abort_txn().await {
            if let Some(resp) = self.try_finish_committed_txn().await? {
                return Ok(resp);
            }
            return Err(abort_err);
        }

        self.clear_intents().await?;
        Err(err)
    }

    async fn try_finish_committed_txn(&mut self) -> Result<Option<WriteBatchResponse>> {
        let txn_table = TxnStateTable::new(self.client.clone(), Some(TXN_CLEANUP_TIMEOUT));
        let Some(record) = txn_table.get_txn_record(self.start_version).await? else {
            return Ok(None);
        };
        if record.state != TxnState::Committed {
            return Ok(None);
        }

        let commit_version = record.commit_version.ok_or_else(|| {
            Error::Internal(
                format!("txn {} is committed without commit version", self.start_version).into(),
            )
        })?;
        if self.commit_version == 0 {
            self.commit_version = commit_version;
        }
        let resp = self.take_response(commit_version);
        self.finish_commit_intents().await?;
        Ok(Some(resp))
    }

    async fn finish_commit_intents(&mut self) -> Result<()> {
        self.num_doing_writes = self.writes.len();
        for write in &mut self.writes {
            write.done = false;
        }

        let mut retry_state = RetryState::new(TXN_CLEANUP_TIMEOUT);
        loop {
            match self.commit_intents_inner(retry_state.timeout()).await {
                Ok(false) => return Ok(()),
                Ok(true) => retry_state.force_retry().await?,
                Err(err) => retry_state.retry(err).await?,
            }
        }
    }

    async fn alloc_txn_version(&mut self) -> Result<u64> {
        let root_client = self.client.root_client();
        loop {
            match root_client.alloc_txn_id(1, self.retry_state.timeout()).await {
                Ok(value) => {
                    return Ok(value);
                }
                Err(err) => {
                    self.retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn start_txn(&mut self) -> Result<()> {
        trace!("start txn, version={}", self.start_version);
        TxnStateTable::new(self.client.clone(), self.retry_state.timeout())
            .begin_txn(self.start_version)
            .await
    }

    async fn prepare_intents(&mut self) -> Result<()> {
        loop {
            if !self.prepare_intents_inner().await? {
                return Ok(());
            }
            self.retry_state.force_retry().await?;
        }
    }

    async fn prepare_intents_inner(&mut self) -> Result<bool> {
        trace!("txn prepare intents, version: {}", self.start_version);
        let router = self.client.router();
        let mut handles = Vec::with_capacity(self.writes.len());
        for (index, write) in self.writes.iter().enumerate() {
            if write.done {
                continue;
            }
            let (group_state, shard_desc) = router.find_shard(write.table_id, write.user_key())?;
            debug_assert!(
                sekas_schema::shard::belong_to(&shard_desc, write.user_key()),
                "shard desc {:?}, user key {:?}",
                shard_desc,
                write.user_key()
            );

            let mut client = GroupClient::new(group_state, self.client.clone());
            let req = Request::WriteIntent(WriteIntentRequest {
                start_version: self.start_version,
                shard_id: shard_desc.id,
                write: Some(write.request.clone()),
            });
            let write_index = write.index as u64;
            if let Some(duration) = self.retry_state.timeout() {
                client.set_timeout(duration);
            }
            let handle = tokio::spawn(async move {
                match client.request(&req).await.map_err(|err| match err {
                    Error::CasFailed(_, cond_index, prev_value) => {
                        Error::CasFailed(write_index, cond_index, prev_value)
                    }
                    err => err,
                })? {
                    Response::WriteIntent(WriteIntentResponse { write: Some(resp) }) => {
                        Ok((resp, index))
                    }
                    _ => Err(Error::Internal(
                        "invalid response type, Get is required".to_string().into(),
                    )),
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.await? {
                Ok((resp, index)) => {
                    self.num_doing_writes =
                        self.num_doing_writes.checked_sub(1).expect("out of range");
                    let write = &mut self.writes[index];
                    write.done = true;
                    write.response = Some(resp);
                }
                Err(err) => {
                    trace!("txn {} write intent: {err:?}", self.start_version);
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        trace!("txn {} write intent left {} writes", self.start_version, self.num_doing_writes);
        Ok(self.num_doing_writes > 0)
    }

    async fn commit_txn(&mut self) -> Result<()> {
        trace!(
            "commit txn update txn table, start version: {}, commit version: {}",
            self.start_version, self.commit_version
        );
        TxnStateTable::new(self.client.clone(), self.retry_state.timeout())
            .commit_txn(self.start_version, self.commit_version)
            .await
    }

    async fn abort_txn(&mut self) -> Result<()> {
        TxnStateTable::new(self.client.clone(), Some(TXN_CLEANUP_TIMEOUT))
            .abort_txn(self.start_version)
            .await
    }

    fn commit_intents(mut self) {
        tokio::spawn(async move {
            trace!(
                "commit txn intents, start version: {}, commit version: {}",
                self.start_version, self.commit_version
            );
            self.num_doing_writes = self.writes.len();
            for write in &mut self.writes {
                write.done = false;
            }

            for i in [1, 3, 5] {
                match self.commit_intents_inner(None).await {
                    Ok(false) => break,
                    Ok(true) => tokio::time::sleep(Duration::from_millis(i)).await,
                    Err(err) => {
                        warn!("txn {} commit intents: {}", self.start_version, err);
                        break;
                    }
                }
            }
        });
    }

    async fn commit_intents_inner(&mut self, timeout: Option<Duration>) -> Result<bool> {
        let router = self.client.router();

        let mut handles = Vec::with_capacity(self.writes.len());
        for (index, write) in self.writes.iter().enumerate() {
            if write.done {
                continue;
            }

            let user_key = write.user_key();
            let (group_state, shard_desc) = router.find_shard(write.table_id, user_key)?;
            let req = CommitIntentRequest {
                shard_id: shard_desc.id,
                start_version: self.start_version,
                commit_version: self.commit_version,
                user_key: user_key.to_vec(),
            };
            let mut client = GroupClient::new(group_state, self.client.clone());
            client.set_timeout_opt(timeout);
            let handle = tokio::spawn(async move {
                match client.request(&Request::CommitIntent(req)).await {
                    Ok(Response::CommitIntent(_)) => Ok(index),
                    Ok(other) => Err(Error::Internal(
                        format!("invalid response {other:?}, `CommitIntent` is required").into(),
                    )),
                    Err(err) => Err(err),
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            match handle.await? {
                Ok(index) => {
                    self.writes[index].done = true;
                    self.num_doing_writes =
                        self.num_doing_writes.checked_sub(1).expect("out of range");
                }
                Err(err) => {
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        trace!("txn {} commit intent left {} writes", self.start_version, self.num_doing_writes);
        Ok(self.num_doing_writes > 0)
    }

    async fn clear_intents(&mut self) -> Result<()> {
        let mut retry_state = RetryState::new(TXN_CLEANUP_TIMEOUT);
        loop {
            match self.clear_intents_inner(retry_state.timeout()).await {
                Ok(false) => return Ok(()),
                Ok(true) => retry_state.force_retry().await?,
                Err(err) => retry_state.retry(err).await?,
            }
        }
    }

    async fn clear_intents_inner(&mut self, timeout: Option<Duration>) -> Result<bool> {
        let router = self.client.router();
        let mut handles = Vec::with_capacity(self.writes.len());
        for (index, write) in self.writes.iter().enumerate() {
            if !write.done {
                continue;
            }

            let user_key = write.user_key();
            let (group_state, shard_desc) = router.find_shard(write.table_id, user_key)?;
            let req = ClearIntentRequest {
                shard_id: shard_desc.id,
                start_version: self.start_version,
                user_key: user_key.to_vec(),
            };
            let mut client = GroupClient::new(group_state, self.client.clone());
            client.set_timeout_opt(timeout);
            let handle = tokio::spawn(async move {
                match client.request(&Request::ClearIntent(req)).await {
                    Ok(Response::ClearIntent(_)) => Ok(index),
                    Ok(other) => Err(Error::Internal(
                        format!("invalid response {other:?}, `ClearIntent` is required").into(),
                    )),
                    Err(err) => Err(err),
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            match handle.await? {
                Ok(index) => {
                    self.writes[index].done = false;
                }
                Err(err) => {
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }

        Ok(self.writes.iter().any(|write| write.done))
    }
}

pub struct WatchKeyStream {
    _handler: sekas_runtime::JoinHandle<()>,
    receiver: mpsc::UnboundedReceiver<AppResult<Value>>,
}

impl futures::Stream for WatchKeyStream {
    type Item = AppResult<Value>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

struct WatchContext {
    table_id: u64,
    version: u64,
    user_key: Vec<u8>,

    sender: mpsc::UnboundedSender<AppResult<Value>>,
}

async fn watch_key(ctx: &mut WatchContext, db: &Database, timeout: Option<Duration>) -> Result<()> {
    use watch_key_response::WatchResult;

    let router = db.client.router();
    loop {
        let (group_state, shard_desc) = router.find_shard(ctx.table_id, &ctx.user_key)?;
        let mut group_client = GroupClient::new(group_state, db.client.clone());
        group_client.set_timeout_opt(timeout);
        let mut stream = group_client.watch_key(shard_desc.id, &ctx.user_key, ctx.version).await?;

        while let Some(resp) = stream.next().await {
            let resp = resp?;
            match WatchResult::from_i32(resp.result) {
                Some(WatchResult::ShardMoved) => {
                    // The stream will be closed immediately.
                }
                Some(WatchResult::ValueUpdated) => {
                    let value = resp.value.ok_or_else(|| {
                        Error::Internal("The value field in WatchKeyResponse is required".into())
                    })?;
                    ctx.version = value.version + 1;
                    if ctx.sender.send(Ok(value)).is_err() {
                        // This stream has been closed.
                        return Ok(());
                    }
                }
                None => {
                    return Err(Error::Internal(
                        format!("Unknown WatchResult value {}", resp.result).into(),
                    ));
                }
            }
        }
    }
}
