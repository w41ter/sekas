// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::borrow::Cow;
use std::future::Future;

use fastrace::collector::{Config, ConsoleReporter};
use fastrace::future::{FutureExt, InSpan};
use fastrace::local::LocalSpan;
use fastrace::prelude::*;
use futures::future::Either;
use vaelo::IntoRequest;

pub mod rpc;

pub const TRACEPARENT_METADATA_KEY: &str = "traceparent";

pub mod span_name {
    pub const CLIENT_GET_READ_VERSION: &str = "client.txn.get_read_version";
    pub const CLIENT_GROUP_REQUEST_GET: &str = "client.group_request.get";
    pub const CLIENT_GROUP_REQUEST_SCAN: &str = "client.group_request.scan";
    pub const CLIENT_GROUP_RPC_ATTEMPT: &str = "client.group_rpc.attempt";
    pub const CLIENT_RANGE_SCAN: &str = "client.range.scan";
    pub const CLIENT_RETRY_BACKOFF: &str = "client.retry.backoff";
    pub const CLIENT_ROUTER_FIND_SHARD: &str = "client.router.find_shard";
    pub const CLIENT_TXN_LOCAL_COMMIT: &str = "client.txn.local_commit";
    pub const CLIENT_TXN_LOCAL_ROUTE: &str = "client.txn.local_commit.route";
    pub const CLIENT_TXN_LOCAL_ALLOC_VERSION: &str = "client.txn.local_commit.alloc_version";
    pub const CLIENT_TXN_DISTRIBUTED_COMMIT: &str = "client.txn.distributed_commit";

    pub const GRPC_NODE_GROUP: &str = "grpc.node.group";
    pub const GRPC_NODE_UNARY_GROUP: &str = "grpc.node.unary_group";
    pub const GRPC_NODE_FORWARD: &str = "grpc.node.forward";
    pub const GRPC_ROOT_ADMIN: &str = "grpc.root.admin";
    pub const GRPC_ROOT_ALLOC_TXN_ID: &str = "grpc.root.alloc_txn_id";
    pub const NODE_FORWARD_REQUEST: &str = "node.forward_request";
    pub const NODE_FORWARD_SCAN: &str = "node.forward_scan";
    pub const NODE_FORWARD_SCAN_SOURCE: &str = "node.forward_scan.source";
    pub const NODE_FORWARD_SCAN_MERGE: &str = "node.forward_scan.merge";

    pub const ENGINE_SNAPSHOT_KEY: &str = "engine.snapshot.key";
    pub const ENGINE_SNAPSHOT_RANGE: &str = "engine.snapshot.range";
    pub const ENGINE_SCAN_MVCC: &str = "engine.scan.mvcc";
    pub const RAFTGROUP_PROPOSE_ENQUEUE: &str = "raftgroup.propose.enqueue";
    pub const RAFTGROUP_PROPOSE_WAIT_RESULT: &str = "raftgroup.propose.wait_result";
    pub const RAFTGROUP_READ_ENQUEUE: &str = "raftgroup.read.enqueue";
    pub const RAFTGROUP_READ_INDEX_WAIT_RESULT: &str = "raftgroup.read.read_index.wait_result";
    pub const RAFTGROUP_LEASE_READ_WAIT_RESULT: &str = "raftgroup.read.lease_read.wait_result";

    pub const REPLICA_CHECK_LEADER_EARLY: &str = "replica.check_leader_early";
    pub const REPLICA_CHECK_LEASE_READ_INDEX: &str = "replica.check_lease.read_index";
    pub const REPLICA_CHECK_REQUEST_EARLY: &str = "replica.check_request_early";
    pub const REPLICA_EVALUATE_COMMAND: &str = "replica.evaluate_command";
    pub const REPLICA_EVAL_GET: &str = "replica.eval_get";
    pub const REPLICA_EVAL_SCAN: &str = "replica.eval_scan";
    pub const REPLICA_EVAL_WRITE: &str = "replica.eval_write";
    pub const REPLICA_RESOLVE_READ_INTENT: &str = "replica.resolve_read_intent";
    pub const REPLICA_TAKE_ROW_LATCHES: &str = "replica.take_row_latches";
    pub const REPLICA_WAIT_COMMIT_FENCE: &str = "replica.wait_commit_fence";
    pub const REPLICA_LOCAL_TXN_BEGIN_COMMIT: &str = "replica.local_txn.begin_commit";
    pub const REPLICA_LOCAL_TXN_BEFORE_READ: &str = "replica.local_txn.before_read";
    pub const REPLICA_WRITE_MVCC: &str = "replica.write.mvcc";
    pub const REPLICA_WRITE_PREPARE_PROPOSAL: &str = "replica.write.prepare_proposal";
    pub const REPLICA_WRITE_WAIT_PENDING_INTENT: &str = "replica.write.wait_pending_intent";
    pub const REPLICA_TAKE_ACL_GUARD: &str = "replica.take_acl_guard";
}

pub trait TraceFutureExt: Future + Sized {
    fn trace(self, name: impl Into<Cow<'static, str>>) -> InSpan<Self> {
        self.in_span(
            Span::root(name, SpanContext::random().sampled(true))
                .with_property(|| ("component", "sekas-client")),
        )
    }

    fn trace_when(
        self,
        enabled: bool,
        name: impl Into<Cow<'static, str>>,
    ) -> Either<Self, InSpan<Self>> {
        if enabled { Either::Right(self.trace(name)) } else { Either::Left(self) }
    }

    fn trace_when_with<N, F>(self, enabled: bool, name: F) -> Either<Self, InSpan<Self>>
    where
        N: Into<Cow<'static, str>>,
        F: FnOnce() -> N,
    {
        if enabled { Either::Right(self.trace(name())) } else { Either::Left(self) }
    }

    fn in_trace(self, name: &'static str) -> InSpan<Self> {
        self.in_span(Span::enter_with_local_parent(name))
    }
}

impl<F: Future> TraceFutureExt for F {}

pub fn local_span(name: &'static str) -> LocalSpan {
    LocalSpan::enter_with_local_parent(name)
}

pub fn init_console_reporter() {
    fastrace::set_reporter(ConsoleReporter, Config::default());
}

pub fn flush() {
    fastrace::flush();
}

pub fn inject_trace_context<T>(req: impl IntoRequest<T>) -> vaelo::Request<T> {
    rpc::request_with_context(req, false)
}

pub fn span_from_rpc_request<T>(
    req: &vaelo::Request<T>,
    name: impl Into<Cow<'static, str>>,
) -> Option<Span> {
    Some(server_span(name, rpc_parent(req.metadata())?))
}

fn rpc_parent(metadata: &vaelo::MetadataMap) -> Option<SpanContext> {
    metadata
        .get(TRACEPARENT_METADATA_KEY)
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(SpanContext::decode_w3c_traceparent)
}

fn server_span(name: impl Into<Cow<'static, str>>, parent: SpanContext) -> Span {
    Span::root(name, parent)
        .with_property(|| ("component", "sekas-server"))
        .with_property(|| ("rpc.system", "grpc"))
}

pub async fn maybe_in_span<F>(span: Option<Span>, future: F) -> F::Output
where
    F: Future,
{
    if let Some(span) = span { future.in_span(span).await } else { future.await }
}
