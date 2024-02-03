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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use sekas_api::server::v1::group_request_union::Request;
use sekas_api::server::v1::group_response_union::Response;
use sekas_api::server::v1::*;
use sekas_schema::system::txn::TXN_MAX_VERSION;
use tokio::sync::mpsc;

use crate::{GroupClient, RetryState, SekasClient};

/// The range descriptor.
#[derive(Debug, Clone)]
pub enum Range {
    /// Scan range with same prefix.
    Prefix(Vec<u8>),
    /// Scan range with boundary keys.
    Range {
        /// The start key of boundary. All keys are scanned if not specified.
        begin: Option<Vec<u8>>,
        /// The start key of boundary. All keys in [begin, inf) are scanned if
        /// not specified.
        end: Option<Vec<u8>>,
    },
}

/// The range request.
#[derive(Debug, Clone)]
pub struct RangeRequest {
    /// The table to scan.
    pub table_id: u64,
    /// The start version to scan.
    pub version: Option<u64>,
    /// The range to scan.
    pub range: Range,
    /// The num keys to limit.
    pub limit: u64,
    /// The total bytes of key-value pairs to limit.
    pub limit_bytes: u64,
    /// The max number of buffered requests. This is an internal option, do NOT
    /// change it if you don't known what it means.
    ///
    /// Default: 1
    pub buffered_requests: usize,
}

pub struct RangeStream {
    fetch_handle: Option<tokio::task::JoinHandle<()>>,

    receiver: mpsc::Receiver<crate::Result<Vec<ValueSet>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScannerState {
    Normal,
    Finished,
    Cancelled,
}

struct RangeScanner {
    client: SekasClient,
    sender: mpsc::Sender<crate::Result<Vec<ValueSet>>>,

    /// The state of this scanner.
    state: ScannerState,

    /// The target table to request.
    table_id: u64,
    /// The target version to request.
    version: u64,
    /// The num of keys to limit.
    limit: u64,
    /// The num of bytes to limit.
    limit_bytes: u64,

    /// The current cursor to scan.
    cursor_key: Vec<u8>,
    /// The end key to scan.
    end_key: Option<Vec<u8>>,
    /// The num scanned batch.
    num_scanned: usize,
}

impl Range {
    pub fn all() -> Self {
        Range::Range { begin: None, end: None }
    }
}

impl futures::Stream for RangeStream {
    type Item = crate::Result<Vec<ValueSet>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(cx)
    }
}

impl Drop for RangeStream {
    fn drop(&mut self) {
        if let Some(handle) = self.fetch_handle.take() {
            if !handle.is_finished() {
                handle.abort();
            }
        }
    }
}

impl RangeStream {
    pub fn init(
        client: SekasClient,
        request: RangeRequest,
        timeout: Option<Duration>,
    ) -> RangeStream {
        let (sender, receiver) = mpsc::channel(request.buffered_requests);
        let (cursor_key, end_key) = match request.range {
            Range::Prefix(prefix) => {
                let end = lexical_next(&prefix);
                (prefix, Some(end))
            }
            Range::Range { begin, end } => (begin.unwrap_or_default(), end),
        };
        let scanner = RangeScanner {
            client,
            sender,
            state: ScannerState::Normal,
            table_id: request.table_id,
            version: request.version.unwrap_or(TXN_MAX_VERSION),
            limit: request.limit,
            limit_bytes: request.limit_bytes,
            cursor_key,
            end_key,
            num_scanned: 0,
        };

        // Spawn a task to fetch value set in background.
        let handle = tokio::spawn(async move {
            let mut scanner = scanner;
            scanner.scan(timeout).await;
        });
        RangeStream { fetch_handle: Some(handle), receiver }
    }
}

impl RangeScanner {
    async fn scan(&mut self, timeout: Option<Duration>) {
        if let Err(err) = self.scan_inner(timeout).await {
            let _ = self.sender.send(Err(err)).await;
        }
    }

    async fn scan_inner(&mut self, timeout: Option<Duration>) -> crate::Result<()> {
        let mut retry_state = RetryState::new(timeout);
        while self.state == ScannerState::Normal {
            let router = self.client.router();
            let (group_state, shard_desc) = router.find_shard(self.table_id, &self.cursor_key)?;
            let mut group_client = GroupClient::new(group_state, self.client.clone());
            if let Err(err) = self.scan_shard(&mut group_client, &shard_desc).await {
                retry_state.retry(err).await?;
                continue;
            }

            retry_state.reset_wait_interval();
            let Some(shard_range) = shard_desc.range else {
                return Err(crate::Error::Internal(
                    format!("shard range is required, shard={shard_desc:?}").into(),
                ));
            };
            if is_entire_range_scanned(self.end_key.as_deref(), &shard_range.end) {
                self.state = ScannerState::Finished;
            } else {
                // This shard has been scanned, skip to next shard.
                self.cursor_key = shard_range.end;
            }
        }
        Ok(())
    }

    async fn scan_shard(
        &mut self,
        group_client: &mut GroupClient,
        shard_desc: &ShardDesc,
    ) -> crate::Result<()> {
        loop {
            let begin_key = self.cursor_key.clone();
            let req = ShardScanRequest {
                shard_id: shard_desc.id,
                start_version: self.version,
                limit: self.limit,
                limit_bytes: self.limit_bytes,
                start_key: Some(begin_key),
                end_key: self.end_key.clone(),
                ..Default::default()
            };
            let scan_resp = match group_client.request(&Request::Scan(req)).await? {
                Response::Scan(resp) => resp,
                e => {
                    return Err(crate::Error::Internal(
                        format!("Response::Scan is required, but got {e:?}").into(),
                    ));
                }
            };
            if let Some(last_value) = scan_resp.data.last() {
                self.cursor_key = lexical_next_boundary(&last_value.user_key);
            }
            if self.sender.send(Ok(scan_resp.data)).await.is_err() {
                self.state = ScannerState::Cancelled;
                return Ok(());
            }

            self.num_scanned += 1;
            if !scan_resp.has_more {
                // This shard are scanned.
                return Ok(());
            }
        }
    }
}

fn lexical_next_boundary(bytes: &[u8]) -> Vec<u8> {
    let mut r = bytes.to_owned();
    while let Some(&last) = r.last() {
        if last != 0xFF {
            break;
        }
        r.pop();
    }
    if let Some(last) = r.last_mut() {
        *last += 0x1;
    }
    r
}

fn lexical_next(bytes: &[u8]) -> Vec<u8> {
    let mut r = bytes.to_owned();
    r.push(0x0);
    r
}

fn is_entire_range_scanned(scan_end: Option<&[u8]>, shard_end: &[u8]) -> bool {
    if let Some(range_end) = scan_end {
        range_end <= shard_end
    } else {
        shard_end.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_next_boundary_basic() {
        struct TestCase {
            input: &'static [u8],
            expect: &'static [u8],
        }
        let cases = vec![
            TestCase { input: b"", expect: b"" },
            TestCase { input: b"1", expect: b"2" },
            TestCase { input: b"1\xFF", expect: b"2" },
            TestCase { input: b"1\xFF\xFF\xFF", expect: b"2" },
            TestCase { input: b"\xFF\xFF\xFF", expect: b"" },
            TestCase { input: b"123", expect: b"124" },
        ];
        for TestCase { input, expect } in cases {
            let got = lexical_next_boundary(input);
            assert_eq!(&got, expect);
        }
    }
}
