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

use log::{debug, warn};
use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_rock::num::decode_u64;
use sekas_rock::time::timestamp_millis;
use sekas_schema::system::keys::{self, txn_lower_key};
use sekas_schema::system::{self, col};

use crate::{Error, GroupClient, Result, RetryState, SekasClient, WriteBuilder};

const TXN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default, Debug)]
pub struct TxnRecord {
    /// The txn unique id.
    pub start_version: u64,
    /// The state of txn record, the valid conversation is:
    ///
    /// RUNNING ------> ABORTED
    ///           +---> COMMITTED
    pub state: TxnState,
    /// The heartbeat of txn.
    pub heartbeat: u64,
    /// The commit version of txn, it only used when state is equals to
    /// COMMITTED.
    pub commit_version: Option<u64>,
}

#[derive(Default)]
struct TxnWriteRequest {
    hash_tag: u8,
    puts: Vec<PutRequest>,
    deletes: Vec<DeleteRequest>,
}

pub struct TxnStateTable {
    client: SekasClient,
}

impl TxnStateTable {
    pub fn new(client: SekasClient) -> Self {
        TxnStateTable { client }
    }

    /// Begin a new transaction with the specified txn version.
    ///
    /// [`Error::InvalidArgument`] is returned if the specified txn has been
    /// committed or aborted.
    pub async fn begin_txn(&self, start_version: u64) -> Result<()> {
        let state_value = TxnState::Running.as_str_name().as_bytes().to_vec();
        let heartbeat_value = timestamp_millis().to_le_bytes().to_vec();
        let hash_tag = system::txn::hash_tag(start_version);
        let request = TxnWriteRequest {
            hash_tag,
            puts: vec![
                WriteBuilder::new(keys::txn_state_key(hash_tag, start_version))
                    .expect_not_exists()
                    .take_prev_value()
                    .ensure_put(state_value),
                WriteBuilder::new(keys::txn_heartbeat_key(hash_tag, start_version))
                    .ensure_put(heartbeat_value),
            ],
            ..Default::default()
        };

        let (idx, cond_idx, prev_value) = match self.write(request).await {
            Err(Error::CasFailed(idx, cond_idx, prev_value)) => (idx, cond_idx, prev_value),
            Err(err) => return Err(err),
            Ok(_) => return Ok(()),
        };

        if idx != 0 || cond_idx != 0 {
            return Err(Error::Internal(format!("invalid cas failed response, idx {idx} and cond idx {cond_idx} are not expected").into()));
        }
        let Some(prev_value) = prev_value.as_ref().and_then(|v| v.content.as_ref()) else {
            return Err(Error::NotFound(format!("target txn {start_version}")));
        };

        let prev_state = parse_txn_state(prev_value)?;
        debug!("try begin txn {start_version}, but prev state is {}", prev_state.as_str_name());
        match prev_state {
            TxnState::Running => Ok(()),
            TxnState::Committed | TxnState::Aborted => Err(Error::InvalidArgument(format!(
                "txn {start_version}, txn already {}",
                prev_state.as_str_name()
            ))),
        }
    }

    /// Update the txn heartbeat.
    pub async fn heartbeat(&self, start_version: u64) -> Result<()> {
        let heartbeat_value = txn_u64_value(timestamp_millis());
        let hash_tag = system::txn::hash_tag(start_version);
        let request = TxnWriteRequest {
            hash_tag,
            puts: vec![WriteBuilder::new(keys::txn_heartbeat_key(hash_tag, start_version))
                .expect_exists()
                .ensure_put(heartbeat_value)],
            ..Default::default()
        };

        match self.write(request).await {
            Err(Error::CasFailed(_, _, _)) => {
                warn!("update txn {start_version} heartbeat, but the target txn is not exists");
                Ok(())
            }
            Err(err) => Err(err),
            Ok(_) => Ok(()),
        }
    }

    /// Commit the transaction specified by `start_version`.
    ///
    /// [`Error::InvalidArgument`] is returned if the specified start version
    /// has been aborted.
    pub async fn commit_txn(&self, start_version: u64, commit_version: u64) -> Result<()> {
        debug_assert!(start_version < commit_version);

        let hash_tag = system::txn::hash_tag(start_version);
        let request = TxnWriteRequest {
            hash_tag,
            puts: vec![
                WriteBuilder::new(keys::txn_state_key(hash_tag, start_version))
                    .expect_value(txn_state_value(TxnState::Running))
                    .take_prev_value()
                    .ensure_put(txn_state_value(TxnState::Committed)),
                WriteBuilder::new(keys::txn_commit_key(hash_tag, start_version))
                    .ensure_put(txn_u64_value(commit_version)),
                WriteBuilder::new(keys::txn_heartbeat_key(hash_tag, start_version))
                    .ensure_put(txn_u64_value(timestamp_millis())),
            ],
            ..Default::default()
        };

        let (idx, cond_idx, prev_value) = match self.write(request).await {
            Err(Error::CasFailed(idx, cond_idx, prev_value)) => (idx, cond_idx, prev_value),
            Err(err) => return Err(err),
            Ok(_) => return Ok(()),
        };

        if idx != 0 || cond_idx != 0 {
            return Err(Error::Internal(format!("invalid cas failed response, idx {idx} and cond idx {cond_idx} are not expected").into()));
        }
        let Some(prev_value) = prev_value.as_ref().and_then(|v| v.content.as_ref()) else {
            return Err(Error::NotFound(format!("target txn {start_version}")));
        };

        let prev_state = parse_txn_state(prev_value)?;
        debug!("try commit txn {start_version}, but prev state is {}", prev_state.as_str_name());
        match prev_state {
            TxnState::Running => Err(Error::Internal(
                "invalid cas failed response, the expect value is TxnState::Running, but failed"
                    .to_string()
                    .into(),
            )),
            TxnState::Committed => {
                // ATTN: here assumes that only one could commit a txn, so the commit version is
                // always equals.
                Ok(())
            }
            TxnState::Aborted => {
                Err(Error::InvalidArgument(format!("txn {start_version}, txn already aborted")))
            }
        }
    }

    /// Get the corresponding txn record.
    pub async fn get_txn_record(&self, start_version: u64) -> Result<Option<TxnRecord>> {
        let hash_tag = system::txn::hash_tag(start_version);
        let txn_prefix = keys::txn_prefix(hash_tag, start_version);
        let scan_resp = self.scan_txn_keys(&txn_prefix).await?;
        parse_txn_record(hash_tag, start_version, scan_resp.data)
    }

    /// Abort the transaction specified by `start_version`.
    ///
    /// [`Error::InvalidArgument`] is returned if the specified txn has been
    /// committed.
    pub async fn abort_txn(&self, start_version: u64) -> Result<()> {
        let expect_state_value = txn_state_value(TxnState::Running);
        let state_value = txn_state_value(TxnState::Aborted);
        let heartbeat_value = txn_u64_value(timestamp_millis());
        let hash_tag = system::txn::hash_tag(start_version);
        let request = TxnWriteRequest {
            hash_tag,
            puts: vec![
                WriteBuilder::new(keys::txn_state_key(hash_tag, start_version))
                    .expect_value(expect_state_value)
                    .take_prev_value()
                    .ensure_put(state_value),
                WriteBuilder::new(keys::txn_heartbeat_key(hash_tag, start_version))
                    .ensure_put(heartbeat_value),
            ],
            ..Default::default()
        };

        let (idx, cond_idx, prev_value) = match self.write(request).await {
            Err(Error::CasFailed(idx, cond_idx, prev_value)) => (idx, cond_idx, prev_value),
            Err(err) => return Err(err),
            Ok(_) => return Ok(()),
        };

        if idx != 0 || cond_idx != 0 {
            return Err(Error::Internal(format!("invalid cas failed response, idx {idx} and cond idx {cond_idx} are not expected").into()));
        }
        let Some(prev_value) = prev_value.as_ref().and_then(|v| v.content.as_ref()) else {
            return Err(Error::NotFound(format!("target txn {start_version}")));
        };

        let prev_state = parse_txn_state(prev_value)?;
        debug!("try abort txn {start_version}, but prev state is {}", prev_state.as_str_name());
        match prev_state {
            TxnState::Running => Err(Error::Internal(
                "invalid cas failed response, the expect value is TxnState::Running, but failed"
                    .to_string()
                    .into(),
            )),
            TxnState::Committed => {
                Err(Error::InvalidArgument(format!("txn {start_version}, txn already committed")))
            }
            TxnState::Aborted => Ok(()),
        }
    }
}

impl TxnStateTable {
    async fn scan_txn_keys(&self, txn_prefix: &[u8]) -> Result<ShardScanResponse> {
        let router = self.client.router();
        let mut retry_state = RetryState::new(Some(TXN_TIMEOUT));
        loop {
            let (group_state, shard_desc) = router.find_shard(col::txn_desc(), txn_prefix)?;
            let mut group_client = GroupClient::new(group_state, self.client.clone());

            let request = Request::Scan(ShardScanRequest {
                shard_id: shard_desc.id,
                start_version: system::txn::TXN_MAX_VERSION,
                prefix: Some(txn_prefix.to_vec()),
                ..Default::default()
            });
            match group_client.request(&request).await {
                Ok(Response::Scan(resp)) => return Ok(resp),
                Ok(_) => {
                    return Err(Error::Internal("invalid response type, Scan is required".into()))
                }
                Err(err) => retry_state.retry(err).await?,
            }
        }
    }

    async fn write(&self, request: TxnWriteRequest) -> Result<ShardWriteResponse> {
        let mut retry_state = RetryState::new(Some(TXN_TIMEOUT));
        loop {
            match self.write_inner(&request, retry_state.timeout()).await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    retry_state.retry(err).await?;
                }
            }
        }
    }

    async fn write_inner(
        &self,
        write: &TxnWriteRequest,
        timeout: Option<Duration>,
    ) -> Result<ShardWriteResponse> {
        let router = self.client.router();
        let key = txn_lower_key(write.hash_tag);
        let (group_state, shard_desc) = router.find_shard(col::txn_desc(), &key)?;

        let mut group_client = GroupClient::new(group_state, self.client.clone());
        if let Some(duration) = timeout {
            group_client.set_timeout(duration);
        }

        let request = Request::Write(ShardWriteRequest {
            shard_id: shard_desc.id,
            deletes: write.deletes.clone(),
            puts: write.puts.clone(),
        });
        match group_client.request(&request).await? {
            Response::Write(resp) => Ok(resp),
            _ => Err(crate::Error::Internal("invalid response type, Write is required".into())),
        }
    }
}

fn parse_txn_record(
    hash_tag: u8,
    start_version: u64,
    values: Vec<ValueSet>,
) -> Result<Option<TxnRecord>> {
    // The scan response output orders.
    let txn_commit_key = keys::txn_commit_key(hash_tag, start_version);
    let txn_heartbeat_key = keys::txn_heartbeat_key(hash_tag, start_version);
    let txn_state_key = keys::txn_state_key(hash_tag, start_version);

    let mut txn_record = TxnRecord::default();
    let mut it = values.into_iter().peekable();
    match it.peek() {
        Some(value_set) => {
            if value_set.user_key == txn_commit_key {
                let commit_version = parse_txn_value(value_set, parse_u64)?;
                txn_record.commit_version = Some(commit_version);
                let _ = it.next();
            }
        }
        None => {
            // No any key returned.
            return Ok(None);
        }
    }

    txn_record.start_version = start_version;
    txn_record.heartbeat = parse_next_txn_key(&mut it, &txn_heartbeat_key, parse_u64)?;
    txn_record.state = parse_next_txn_key(&mut it, &txn_state_key, parse_txn_state)?;
    if it.next().is_some() {
        return Err(Error::Internal(
            format!("not all txn record keys are consumed, start version: {start_version}").into(),
        ));
    }

    Ok(Some(txn_record))
}

fn parse_u64(bytes: &[u8]) -> Result<u64> {
    decode_u64(bytes).ok_or_else(|| {
        Error::Internal(
            format!("8 bytes is required to parse u64, but got {} bytes", bytes.len()).into(),
        )
    })
}

fn parse_txn_state(bytes: &[u8]) -> Result<TxnState> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(TxnState::from_str_name)
        .ok_or_else(|| Error::Internal(format!("unknown txn state value: {bytes:?}").into()))
}

fn parse_txn_value<Fn, T>(value_set: &ValueSet, parser: Fn) -> Result<T>
where
    Fn: FnOnce(&[u8]) -> Result<T>,
{
    value_set
        .values
        .first()
        .and_then(|v| v.content.as_ref())
        .ok_or_else(|| Error::Internal("at lease a value in scan value set is required".into()))
        .map(Vec::as_slice)
        .and_then(parser)
}

fn parse_next_txn_key<Fn, T, I>(it: &mut I, key: &[u8], parser: Fn) -> Result<T>
where
    I: Iterator<Item = ValueSet>,
    Fn: FnOnce(&[u8]) -> Result<T>,
{
    let value_set = it
        .next()
        .filter(|v| v.user_key == key)
        .ok_or_else(|| Error::Internal(format!("the next key {key:?} is required").into()))?;
    parse_txn_value(&value_set, parser)
}

#[inline]
fn txn_state_value(state: TxnState) -> Vec<u8> {
    state.as_str_name().as_bytes().to_vec()
}

#[inline]
fn txn_u64_value(val: u64) -> Vec<u8> {
    val.to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use sekas_schema::system::keys::{
        txn_commit_key, txn_heartbeat_key, txn_prefix, txn_state_key,
    };

    use super::*;

    #[test]
    fn encode_and_parse_value() {
        let states = vec![TxnState::Running, TxnState::Committed, TxnState::Aborted];
        for expect_state in states {
            let bytes = txn_state_value(expect_state);
            let state = parse_txn_state(&bytes);
            assert!(
                matches!(state, Ok(state) if state == expect_state),
                "expect state: {}, but got {state:?}",
                expect_state.as_str_name()
            );
        }

        let val = parse_u64(&txn_u64_value(u64::MAX)).unwrap();
        assert_eq!(val, u64::MAX);
        let val = parse_u64(&txn_u64_value(0)).unwrap();
        assert_eq!(val, 0);
    }

    #[test]
    fn parse_empty_txn_record() {
        let result = parse_txn_record(1, 1, vec![]);
        assert!(matches!(result, Ok(None)));
    }

    #[test]
    fn parse_begin_txn_record() {
        let hash_tag = 1;
        let txn_id = 123;
        let state_key = txn_state_key(hash_tag, txn_id);
        let heartbeat_key = txn_heartbeat_key(hash_tag, txn_id);
        let values = vec![
            ValueSet {
                user_key: heartbeat_key.clone(),
                values: vec![
                    Value::with_value(txn_u64_value(123), 1), // heartbeat.
                ],
            },
            ValueSet {
                user_key: state_key.clone(),
                values: vec![
                    Value::with_value(txn_state_value(TxnState::Running), 1), // state
                ],
            },
        ];

        let txn_record =
            parse_txn_record(hash_tag, txn_id, values).expect("no error").expect("value exists");
        assert_eq!(txn_record.heartbeat, 123);
        assert_eq!(txn_record.state, TxnState::Running);
    }

    #[test]
    fn parse_commit_txn_record() {
        let hash_tag = 1;
        let txn_id = 123;
        let commit_key = txn_commit_key(hash_tag, txn_id);
        let state_key = txn_state_key(hash_tag, txn_id);
        let heartbeat_key = txn_heartbeat_key(hash_tag, txn_id);
        let values = vec![
            ValueSet {
                user_key: commit_key.clone(),
                values: vec![
                    Value::with_value(txn_u64_value(321), 1), // commit version.
                ],
            },
            ValueSet {
                user_key: heartbeat_key.clone(),
                values: vec![
                    Value::with_value(txn_u64_value(123), 1), // heartbeat.
                ],
            },
            ValueSet {
                user_key: state_key.clone(),
                values: vec![
                    Value::with_value(txn_state_value(TxnState::Running), 1), // state
                ],
            },
        ];

        let txn_record =
            parse_txn_record(hash_tag, txn_id, values).expect("no error").expect("value exists");
        assert_eq!(txn_record.commit_version, Some(321));
        assert_eq!(txn_record.heartbeat, 123);
        assert_eq!(txn_record.state, TxnState::Running);
    }

    #[test]
    fn parse_abort_txn_record() {
        let hash_tag = 1;
        let txn_id = 123;
        let state_key = txn_state_key(hash_tag, txn_id);
        let heartbeat_key = txn_heartbeat_key(hash_tag, txn_id);
        let values = vec![
            ValueSet {
                user_key: heartbeat_key.clone(),
                values: vec![
                    Value::with_value(txn_u64_value(123), 1), // heartbeat.
                ],
            },
            ValueSet {
                user_key: state_key.clone(),
                values: vec![
                    Value::with_value(txn_state_value(TxnState::Aborted), 1), // state
                ],
            },
        ];

        let txn_record =
            parse_txn_record(hash_tag, txn_id, values).expect("no error").expect("value exists");
        assert_eq!(txn_record.heartbeat, 123);
        assert_eq!(txn_record.state, TxnState::Aborted);
    }

    #[test]
    fn parse_partial_txn_record() {
        let hash_tag = 1;
        let txn_id = 123;
        let state_key = txn_state_key(hash_tag, txn_id);
        let values = vec![ValueSet {
            user_key: state_key.clone(),
            values: vec![
                Value::with_value(txn_state_value(TxnState::Running), 1), // state
            ],
        }];

        let result = parse_txn_record(hash_tag, txn_id, values);
        assert!(matches!(result, Err(Error::Internal(_))));
    }

    #[test]
    fn parse_txn_record_consume_all_keys() {
        let hash_tag = 1;
        let txn_id = 123;
        let state_key = txn_state_key(hash_tag, txn_id);
        let heartbeat_key = txn_heartbeat_key(hash_tag, txn_id);
        let mut other_key = txn_prefix(hash_tag, txn_id);
        other_key.extend_from_slice(b"zzzz");
        let values = vec![
            ValueSet {
                user_key: heartbeat_key.clone(),
                values: vec![
                    Value::with_value(txn_u64_value(123), 1), // heartbeat.
                ],
            },
            ValueSet {
                user_key: state_key.clone(),
                values: vec![
                    Value::with_value(txn_state_value(TxnState::Aborted), 1), // state
                ],
            },
            ValueSet {
                user_key: other_key,
                values: vec![
                    Value::with_value(txn_state_value(TxnState::Aborted), 1), // state
                ],
            },
        ];

        let result = parse_txn_record(hash_tag, txn_id, values);
        assert!(matches!(result, Err(Error::Internal(_))));
    }
}
