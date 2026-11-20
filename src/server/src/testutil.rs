// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::path::{Path, PathBuf};

/// Run a test server and expose its initialized node/root for deterministic
/// scheduling tests, without changing production polling intervals.
pub fn run_with_server(
    config: crate::Config,
    executor: sekas_runtime::Executor,
    shutdown: sekas_runtime::Shutdown,
    ready: tokio::sync::oneshot::Sender<crate::Server>,
) -> crate::Result<()> {
    executor.block_on(crate::bootstrap::run_in_async(config, shutdown, Some(ready)))
}

/// Supply fixture statistics and drive exactly one scheduler poll.
pub async fn drive_scheduling(
    server: &crate::Server,
    stats: Vec<sekas_api::server::v1::GroupStats>,
) -> crate::Result<()> {
    server.root.poll_with_stats(stats).await
}

/// Exercise the replica request gate directly, without the node's busy retry
/// loop.
pub async fn try_group_request(
    server: &crate::Server,
    group: u64,
    epoch: u64,
    request: &sekas_api::server::v1::group_request_union::Request,
) -> crate::Result<sekas_api::server::v1::group_response_union::Response> {
    let replica =
        server.node.replica_table().find(group).ok_or(crate::Error::GroupNotFound(group))?;
    replica.try_execute(crate::replica::ExecCtx::with_epoch(epoch), request).await
}

/// Compatibility wrapper used while keeping test call sites concise.
pub struct TempDir(tempfile::TempDir);

impl TempDir {
    pub fn new(prefix: &str) -> std::io::Result<Self> {
        tempfile::Builder::new().prefix(prefix).tempdir().map(Self)
    }

    pub fn path(&self) -> &Path {
        self.0.path()
    }

    #[allow(deprecated)]
    pub fn into_path(self) -> PathBuf {
        self.0.into_path()
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}
