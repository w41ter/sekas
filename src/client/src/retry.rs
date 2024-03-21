// Copyright 2023-present The Sekas Authors.
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

use std::time::{Duration, Instant};

use crate::{Error, Result};

const MIN_INTERVAL_MS: u64 = 8;
const MAX_INTERVAL_MS: u64 = 3000;

pub struct RetryState {
    interval_ms: u64,
    deadline: Option<Instant>,
}

impl Default for RetryState {
    fn default() -> Self {
        RetryState { interval_ms: MIN_INTERVAL_MS, deadline: None }
    }
}

impl RetryState {
    pub fn new(timeout: Duration) -> Self {
        RetryState { interval_ms: MIN_INTERVAL_MS, deadline: Instant::now().checked_add(timeout) }
    }

    pub fn with_timeout_opt(timeout: Option<Duration>) -> Self {
        RetryState {
            interval_ms: MIN_INTERVAL_MS,
            deadline: timeout.and_then(|v| Instant::now().checked_add(v)),
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self::with_deadline_opt(Some(deadline))
    }

    pub fn with_deadline_opt(deadline: Option<Instant>) -> Self {
        RetryState { interval_ms: MIN_INTERVAL_MS, deadline }
    }

    #[inline]
    pub fn timeout(&self) -> Option<Duration> {
        self.deadline.map(|d| d.saturating_duration_since(Instant::now()))
    }

    pub fn reset_wait_interval(&mut self) {
        self.interval_ms = MIN_INTERVAL_MS;
    }

    pub fn is_retryable(&self, err: &Error) -> bool {
        match err {
            Error::NotFound(_) | Error::EpochNotMatch(_) | Error::GroupNotAccessable(_) => true,
            Error::NotLeader(..)
            | Error::GroupNotFound(_)
            | Error::NotRootLeader(..)
            | Error::Connect(_) => {
                unreachable!()
            }
            Error::InvalidArgument(_)
            | Error::DeadlineExceeded(_)
            | Error::ResourceExhausted(_)
            | Error::AlreadyExists(_)
            | Error::CasFailed(_, _, _)
            | Error::Rpc(_)
            | Error::Transport(_)
            | Error::Internal(_) => false,
        }
    }

    pub async fn retry(&mut self, err: Error) -> Result<()> {
        if !self.is_retryable(&err) {
            return Err(err);
        }

        self.force_retry().await
    }

    pub async fn force_retry(&mut self) -> Result<()> {
        let mut interval = Duration::from_millis(self.interval_ms);
        if let Some(deadline) = self.deadline {
            if let Some(duration) = deadline.checked_duration_since(Instant::now()) {
                interval = std::cmp::min(interval, duration);
            } else {
                return Err(Error::DeadlineExceeded("timeout".into()));
            }
        }
        tokio::time::sleep(interval).await;
        self.interval_ms = std::cmp::min(self.interval_ms * 2, MAX_INTERVAL_MS);
        Ok(())
    }
}
