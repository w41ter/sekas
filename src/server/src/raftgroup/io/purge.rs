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
use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use sekas_runtime::JoinHandle;

use crate::Result;

pub fn start_purging_expired_files(engine: Arc<raft_engine::Engine>) -> JoinHandle<()> {
    sekas_runtime::spawn(async move {
        loop {
            sekas_runtime::time::sleep(Duration::from_secs(10)).await;
            match purge_expired_files(engine.clone()).await {
                Err(e) => {
                    warn!("raft engine purge expired files: {e:?}")
                }
                Ok(replicas) => {
                    if !replicas.is_empty() {
                        debug!("raft engine purge expired files, replicas {replicas:?} is too old")
                    }
                }
            }
        }
    })
}

#[inline]
async fn purge_expired_files(
    engine: Arc<raft_engine::Engine>,
) -> Result<Vec<u64>, raft_engine::Error> {
    sekas_runtime::spawn_blocking(move || engine.purge_expired_files()).await.unwrap()
}
