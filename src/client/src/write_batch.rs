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

use std::collections::HashMap;
use std::iter::zip;
use std::time::Duration;

use log::warn;
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;

use crate::{
    Error, GroupClient, Result, RetryState, RouterGroupState, SekasClient, TxnStateTable,
    WriteBatchRequest, WriteBatchResponse,
};

/// A helper enum to hold both [`PutRequest`] and [`DeleteRequest`].
enum WriteRequest {
    Put(PutRequest),
    Delete(DeleteRequest),
}

/// A structure to hold the context about single write request.
struct WriteContext {
    /// The request.
    request: WriteRequest,
    /// The response.
    response: Option<WriteResponse>,
    /// The index in the requests.
    index: usize,
    /// Is this request has been accepted.
    done: bool,
}

/// A structure to hold the context about a write batch request.
pub struct WriteBatchContext {
    client: SekasClient,
    desc: CollectionDesc,

    writes: Vec<WriteContext>,
    /// The number of doing requests.
    num_doing_writes: usize,
    /// The number of delete requests in this batch.
    num_deletes: usize,

    start_version: u64,
    commit_version: u64,

    retry_state: RetryState,
}

impl WriteContext {
    fn with_put((index, put): (usize, PutRequest)) -> Self {
        WriteContext { request: WriteRequest::Put(put), response: None, index, done: false }
    }

    fn with_delete((index, delete): (usize, DeleteRequest)) -> Self {
        WriteContext { request: WriteRequest::Delete(delete), response: None, index, done: false }
    }

    fn key(&self) -> &[u8] {
        match &self.request {
            WriteRequest::Put(put) => &put.key,
            WriteRequest::Delete(del) => &del.key,
        }
    }
}

impl WriteBatchContext {
    pub fn new(
        desc: CollectionDesc,
        request: WriteBatchRequest,
        client: SekasClient,
        timeout: Option<Duration>,
    ) -> Self {
        let num_deletes = request.deletes.len();
        let num_puts = request.puts.len();
        let num_doing_writes = num_deletes + num_puts;
        let mut writes = Vec::with_capacity(num_doing_writes);
        writes.extend(request.deletes.into_iter().enumerate().map(WriteContext::with_delete));
        writes.extend(request.puts.into_iter().enumerate().map(WriteContext::with_put));

        WriteBatchContext {
            client,
            desc,
            writes,
            num_deletes,
            num_doing_writes,
            start_version: 0,
            commit_version: 0,
            retry_state: RetryState::new(timeout),
        }
    }

    pub async fn commit(mut self) -> Result<WriteBatchResponse> {
        // TODO: check parameters

        // TODO: handle errors to abort txn.
        self.start_version = self.alloc_txn_version().await?;
        self.start_txn().await?;

        let start_version = self.start_version;
        let txn_table = TxnStateTable::new(self.client.clone());

        tokio::select! {
            _ = Self::lease_txn(txn_table, start_version) => {
                unreachable!()
            },
            resp = self.commit_inner() => {
                resp
            }
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

    async fn commit_inner(mut self) -> Result<WriteBatchResponse> {
        self.prepare_intents().await?;
        self.commit_version = self.alloc_txn_version().await?;
        self.commit_txn().await?;
        let version = self.commit_version;

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

        self.commit_intents();
        Ok(WriteBatchResponse { version, deletes, puts })
    }

    async fn alloc_txn_version(&mut self) -> Result<u64> {
        let root_client = self.client.root_client();
        loop {
            match root_client.alloc_txn_id(1).await {
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
        let txn_table = TxnStateTable::new(self.client.clone());
        loop {
            match txn_table.begin_txn(self.start_version).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn prepare_intents(&mut self) -> Result<()> {
        loop {
            if self.prepare_intents_inner().await? {
                return Ok(());
            }
            self.retry_state.force_retry().await?;
        }
    }

    async fn prepare_intents_inner(&mut self) -> Result<bool> {
        struct GroupRequestContext {
            group_state: RouterGroupState,
            #[allow(unused)]
            shard_desc: ShardDesc,
            puts: Vec<PutRequest>,
            deletes: Vec<DeleteRequest>,
            delete_indexes: Vec<usize>,
            put_indexes: Vec<usize>,
        }

        let router = self.client.router();
        let mut req_ctx_map = HashMap::new();
        for write in &self.writes {
            if write.done {
                continue;
            }
            let (group_state, shard_desc) = router.find_shard(self.desc.clone(), write.key())?;
            let ctx = req_ctx_map.entry(shard_desc.id).or_insert_with(|| GroupRequestContext {
                group_state,
                shard_desc,
                puts: Vec::default(),
                deletes: Vec::default(),
                delete_indexes: Vec::default(),
                put_indexes: Vec::default(),
            });
            match &write.request {
                WriteRequest::Put(put) => {
                    ctx.puts.push(put.clone());
                    ctx.put_indexes.push(write.index);
                }
                WriteRequest::Delete(del) => {
                    ctx.deletes.push(del.clone());
                    ctx.delete_indexes.push(write.index);
                }
            }
        }

        let mut handles = Vec::with_capacity(req_ctx_map.len());
        for (shard_id, ctx) in req_ctx_map {
            let mut client = GroupClient::new(ctx.group_state, self.client.clone());
            let req = Request::WriteIntent(WriteIntentRequest {
                start_version: self.start_version,
                write: Some(ShardWriteRequest { shard_id, deletes: ctx.deletes, puts: ctx.puts }),
            });
            if let Some(duration) = self.retry_state.timeout() {
                client.set_timeout(duration);
            }
            let handle = tokio::spawn(async move {
                match client.request(&req).await? {
                    Response::WriteIntent(WriteIntentResponse { write: Some(resp) }) => {
                        Ok((resp, ctx.put_indexes, ctx.delete_indexes))
                    }
                    _ => Err(crate::Error::Internal(
                        "invalid response type, Get is required".to_string().into(),
                    )),
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            match handle.await? {
                Ok((resp, put_indexes, delete_indexes)) => {
                    if resp.deletes.len() != delete_indexes.len()
                        || resp.puts.len() != put_indexes.len()
                    {
                        return Err(Error::Internal(
                            "the write intent response has different num of deletes/puts"
                                .to_string()
                                .into(),
                        ));
                    }

                    self.num_doing_writes
                        .checked_sub(delete_indexes.len() + put_indexes.len())
                        .expect("out of range");
                    for (index, delete) in zip(delete_indexes, resp.deletes) {
                        let write = &mut self.writes[index];
                        write.done = true;
                        write.response = Some(delete);
                    }
                    for (index, put) in zip(put_indexes, resp.puts) {
                        let write = &mut self.writes[index];
                        write.done = true;
                        write.response = Some(put);
                    }
                }
                Err(err) => {
                    if self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        Ok(self.num_doing_writes > 0)
    }

    async fn commit_txn(&mut self) -> Result<()> {
        let txn_table = TxnStateTable::new(self.client.clone());
        loop {
            match txn_table.commit_txn(self.start_version, self.commit_version).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.retry_state.retry(err).await?;
                }
            }
        }
    }

    #[allow(unused)]
    async fn abort_txn(&mut self) -> Result<()> {
        let txn_table = TxnStateTable::new(self.client.clone());
        loop {
            match txn_table.abort_txn(self.start_version).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    self.retry_state.retry(err).await?;
                }
            }
        }
    }

    fn commit_intents(mut self) {
        tokio::spawn(async move {
            self.num_doing_writes = self.writes.len();
            for write in &mut self.writes {
                write.done = false;
            }

            for i in [1, 3, 5] {
                match self.commit_intents_inner().await {
                    Ok(true) => break,
                    Ok(false) => tokio::time::sleep(Duration::from_millis(i)).await,
                    Err(err) => {
                        warn!("txn {} commit intents: {}", self.start_version, err);
                        break;
                    }
                }
            }
        });
    }

    async fn commit_intents_inner(&mut self) -> Result<bool> {
        let router = self.client.router();

        let mut handles = Vec::with_capacity(self.writes.len());
        for write in &self.writes {
            if write.done {
                continue;
            }

            let key = write.key();
            let (group_state, shard_desc) = router.find_shard(self.desc.clone(), key)?;
            let req = CommitIntentRequest {
                shard_id: shard_desc.id,
                start_version: self.start_version,
                commit_version: self.commit_version,
                keys: vec![key.to_vec()],
            };
            let index = write.index;
            let mut client = GroupClient::new(group_state, self.client.clone());
            let handle = tokio::spawn(async move {
                match client.request(&Request::CommitIntent(req)).await {
                    Ok(Response::CommitIntent(CommitIntentResponse {})) => Ok(index),
                    _ => Err(Error::Internal(
                        "invalid response, `CommitIntent` is required".to_string().into(),
                    )),
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            match handle.await? {
                Ok(index) => {
                    self.writes[index].done = true;
                    self.num_doing_writes.checked_sub(1).expect("out of range");
                }
                Err(err) => {
                    if !self.retry_state.is_retryable(&err) {
                        return Err(err);
                    }
                }
            }
        }
        Ok(self.num_doing_writes > 0)
    }

    #[allow(unused)]
    async fn clear_intents(&mut self) -> Result<()> {
        todo!()
    }
}
