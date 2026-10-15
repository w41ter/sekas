// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::borrow::Cow;
use std::future::Future;

use fastrace::collector::{Config, ConsoleReporter};
use fastrace::future::{FutureExt, InSpan};
use fastrace::local::LocalSpan;
use fastrace::prelude::*;
use futures::future::Either;
use tonic::IntoRequest;
use tonic::metadata::MetadataValue;

pub const TRACEPARENT_METADATA_KEY: &str = "traceparent";

pub mod span_name {
    pub const CLIENT_GET_READ_VERSION: &str = "client.txn.get_read_version";
    pub const CLIENT_GROUP_REQUEST_GET: &str = "client.group_request.get";
    pub const CLIENT_ROUTER_FIND_SHARD: &str = "client.router.find_shard";

    pub const GRPC_NODE_GROUP: &str = "grpc.node.group";
    pub const GRPC_NODE_UNARY_GROUP: &str = "grpc.node.unary_group";
    pub const GRPC_ROOT_ALLOC_TXN_ID: &str = "grpc.root.alloc_txn_id";

    pub const ENGINE_SNAPSHOT_KEY: &str = "engine.snapshot.key";
    pub const RAFTGROUP_READ_ENQUEUE: &str = "raftgroup.read.enqueue";
    pub const RAFTGROUP_READ_INDEX_WAIT_RESULT: &str = "raftgroup.read.read_index.wait_result";
    pub const RAFTGROUP_LEASE_READ_WAIT_RESULT: &str = "raftgroup.read.lease_read.wait_result";

    pub const REPLICA_CHECK_LEADER_EARLY: &str = "replica.check_leader_early";
    pub const REPLICA_CHECK_LEASE_READ_INDEX: &str = "replica.check_lease.read_index";
    pub const REPLICA_CHECK_REQUEST_EARLY: &str = "replica.check_request_early";
    pub const REPLICA_EVALUATE_COMMAND: &str = "replica.evaluate_command";
    pub const REPLICA_EVAL_GET: &str = "replica.eval_get";
    pub const REPLICA_LOCAL_TXN_BEFORE_READ: &str = "replica.local_txn.before_read";
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

pub fn inject_trace_context<T>(req: impl IntoRequest<T>) -> tonic::Request<T> {
    let mut req = req.into_request();
    if let Some(parent) = SpanContext::current_local_parent()
        && let Ok(value) = MetadataValue::try_from(parent.encode_w3c_traceparent().as_str())
    {
        req.metadata_mut().insert(TRACEPARENT_METADATA_KEY, value);
    }
    req
}

pub fn span_from_tonic_request<T>(
    req: &tonic::Request<T>,
    name: impl Into<Cow<'static, str>>,
) -> Option<Span> {
    let parent = req
        .metadata()
        .get(TRACEPARENT_METADATA_KEY)
        .and_then(|value| value.to_str().ok())
        .and_then(SpanContext::decode_w3c_traceparent)?;
    Some(
        Span::root(name, parent)
            .with_property(|| ("component", "sekas-server"))
            .with_property(|| ("rpc.system", "grpc")),
    )
}

pub async fn maybe_in_span<F>(span: Option<Span>, future: F) -> F::Output
where
    F: Future,
{
    if let Some(span) = span { future.in_span(span).await } else { future.await }
}
