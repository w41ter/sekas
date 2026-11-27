// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fastrace::collector::{Config, Reporter, SpanRecord};
use fastrace::future::FutureExt;
use fastrace::prelude::{Span, SpanContext, SpanId, TraceId};
use prost::Message;
use vaelo::{IntoRequest, Request, Response, Status};

use super::{TRACEPARENT_METADATA_KEY, flush, maybe_in_span, rpc_parent, server_span};

const REPLY_REQUEST: &str = "sekas-trace-reply";
const REPLY_SPANS: &str = "sekas-trace-bin";
// Leave room in the gRPC trailers for status details and binary metadata
// encoding.
const MAX_REPLY_BYTES: usize = 4096;
const REPORT_WAIT: Duration = Duration::from_millis(10);

static CAPTURES: OnceLock<Mutex<HashMap<TraceId, CapturedSpans>>> = OnceLock::new();

struct CapturedSpans {
    display_id: TraceId,
    spans: Vec<SpanRecord>,
}

struct Capture {
    local_id: TraceId,
    root_id: SpanId,
}

pub fn init_reporter(log: Option<Box<dyn Reporter>>) {
    fastrace::set_reporter(CaptureReporter { log }, Config::default());
    CAPTURES.get_or_init(Default::default);
}

struct CaptureReporter {
    log: Option<Box<dyn Reporter>>,
}

impl Reporter for CaptureReporter {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        if spans.is_empty() {
            return;
        }
        let mut logged = Vec::new();
        if let Some(captures) = CAPTURES.get() {
            let mut captures = captures.lock().unwrap();
            for mut span in spans {
                if let Some(capture) = captures.get_mut(&span.trace_id) {
                    span.trace_id = capture.display_id;
                    if self.log.is_some() {
                        logged.push(span.clone());
                    }
                    capture.spans.push(span);
                } else if self.log.is_some() {
                    logged.push(span);
                }
            }
        }
        if let Some(log) = self.log.as_mut() {
            log.report(logged);
        }
    }
}

impl Capture {
    fn new(span: &Span, display_id: TraceId) -> Self {
        let root = SpanContext::from_span(span).expect("sampled root span");
        CAPTURES
            .get()
            .expect("trace capture reporter installed")
            .lock()
            .unwrap()
            .insert(root.trace_id, CapturedSpans { display_id, spans: Vec::new() });
        Capture { local_id: root.trace_id, root_id: root.span_id }
    }

    fn take_spans(&self) -> Vec<SpanRecord> {
        CAPTURES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&self.local_id)
            .map_or_else(Vec::new, |capture| capture.spans)
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        // A timed-out or canceled RPC can drop the capture before the reporter runs.
        CAPTURES.get().unwrap().lock().unwrap().remove(&self.local_id);
    }
}

pub async fn capture_request<F: Future>(
    name: &'static str,
    request: F,
) -> (F::Output, Vec<SpanRecord>) {
    let context = SpanContext::random().sampled(true);
    let span = Span::root(name, context);
    let capture = Capture::new(&span, context.trace_id);
    let output = request.in_span(span).await;
    flush();
    (output, capture.take_spans())
}

/// Collects the sampled spans for a unary RPC and attaches them to its reply
/// when requested.
pub struct RpcTrace {
    span: Option<Span>,
    capture: Option<Capture>,
}

impl RpcTrace {
    pub fn new<T>(request: &Request<T>, name: &'static str) -> Self {
        let Some(parent) = rpc_parent(request.metadata()) else {
            return RpcTrace { span: None, capture: None };
        };
        let display_id = request
            .metadata()
            .get(REPLY_REQUEST)
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse().ok());
        if let Some(display_id) = display_id.filter(|_| parent.sampled && CAPTURES.get().is_some())
        {
            // Isolate this RPC's span collection from concurrent RPCs in the same trace.
            let span = server_span(name, SpanContext::new(TraceId::random(), parent.span_id));
            let capture = Capture::new(&span, display_id);
            return RpcTrace { span: Some(span), capture: Some(capture) };
        }
        RpcTrace { span: Some(server_span(name, parent)), capture: None }
    }

    pub fn span(&self) -> Option<&Span> {
        self.span.as_ref()
    }

    pub async fn run<T>(
        self,
        handle: impl Future<Output = Result<Response<T>, Status>>,
    ) -> Result<Response<T>, Status> {
        let mut result = maybe_in_span(self.span, handle).await;
        if let Some(capture) = self.capture {
            let _ = sekas_runtime::time::timeout(REPORT_WAIT, sekas_runtime::spawn_blocking(flush))
                .await;
            if let Some(reply) = encode_reply(capture.root_id, capture.take_spans()) {
                let metadata = match &mut result {
                    Ok(response) => response.trailers_mut(),
                    Err(status) => status.metadata_mut(),
                };
                let _ = metadata.insert(REPLY_SPANS, reply);
            }
        }
        result
    }
}

pub(crate) fn request_trace<T>(req: impl IntoRequest<T>) -> Request<T> {
    request_with_context(req, true)
}

pub(super) fn request_with_context<T>(req: impl IntoRequest<T>, reply: bool) -> Request<T> {
    let mut req = req.into_request();
    if let Some(parent) = SpanContext::current_local_parent() {
        let metadata = req.metadata_mut();
        let _ = metadata.insert(TRACEPARENT_METADATA_KEY, parent.encode_w3c_traceparent());
        if reply && let Some(captures) = CAPTURES.get() {
            let display_id =
                captures.lock().unwrap().get(&parent.trace_id).map(|capture| capture.display_id);
            if let Some(display_id) = display_id {
                let _ = metadata.insert(REPLY_REQUEST, display_id.to_string());
            }
        }
    }
    req
}

pub(crate) fn receive_trace<T>(
    response: Result<Response<T>, Status>,
) -> Result<Response<T>, Status> {
    let metadata = match &response {
        Ok(response) => response.trailers(),
        Err(status) => status.metadata(),
    };
    if let Some(bytes) = metadata.get(REPLY_SPANS) {
        receive_spans(bytes);
    }
    response
}

#[derive(Message)]
struct TraceReply {
    #[prost(message, repeated, tag = "1")]
    spans: Vec<ReplySpan>,
    #[prost(bool, tag = "2")]
    truncated: bool,
}

#[derive(Message)]
struct ReplySpan {
    #[prost(fixed64, tag = "1")]
    id: u64,
    #[prost(fixed64, tag = "2")]
    parent: u64,
    #[prost(uint64, tag = "3")]
    start_ns: u64,
    #[prost(uint64, tag = "4")]
    duration_ns: u64,
    #[prost(string, tag = "5")]
    name: String,
    #[prost(map = "string, string", tag = "6")]
    properties: HashMap<String, String>,
}

fn encode_reply(root_id: SpanId, mut spans: Vec<SpanRecord>) -> Option<Vec<u8>> {
    spans.sort_unstable_by_key(|span| (span.span_id != root_id, span.begin_time_unix_ns));
    if spans.first()?.span_id != root_id {
        return None;
    }
    let start_ns = spans[0].begin_time_unix_ns;
    let mut reply = TraceReply::default();
    let mut size = prost::encoding::bool::encoded_len(2, &true);
    for span in spans {
        let wire = ReplySpan {
            id: span.span_id.0,
            parent: span.parent_id.0,
            start_ns: span.begin_time_unix_ns.saturating_sub(start_ns),
            duration_ns: span.duration_ns,
            name: span.name.into_owned(),
            properties: span
                .properties
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect(),
        };
        size += prost::encoding::message::encoded_len(1, &wire);
        if size > MAX_REPLY_BYTES {
            reply.truncated = true;
            break;
        }
        reply.spans.push(wire);
    }
    Some(reply.encode_to_vec())
}

fn receive_spans(bytes: &[u8]) {
    if bytes.len() > MAX_REPLY_BYTES {
        return;
    }
    let Some(parent) = SpanContext::current_local_parent() else {
        return;
    };
    let Ok(mut reply) = TraceReply::decode(bytes) else {
        return;
    };
    let Some(root) = reply.spans.first_mut().filter(|span| span.parent == parent.span_id.0) else {
        return;
    };
    if reply.truncated {
        root.properties.insert("trace_truncated".into(), "true".into());
    }
    // Place the remote span tree on the caller's clock, ending at response receipt.
    let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let start_ns = now_ns.saturating_sub(root.duration_ns);
    let Some(captures) = CAPTURES.get() else {
        return;
    };
    if let Some(capture) = captures.lock().unwrap().get_mut(&parent.trace_id) {
        capture.spans.extend(reply.spans.into_iter().map(|span| SpanRecord {
            trace_id: capture.display_id,
            span_id: SpanId(span.id),
            parent_id: SpanId(span.parent),
            begin_time_unix_ns: start_ns.saturating_add(span.start_ns),
            duration_ns: span.duration_ns,
            name: span.name.into(),
            properties: span.properties.into_iter().map(|(k, v)| (k.into(), v.into())).collect(),
            ..Default::default()
        }));
    }
}
