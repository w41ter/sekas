// Copyright 2026-present The Sekas Authors.
// Licensed under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::time::Duration;

use fastrace::collector::{Reporter, SpanRecord};
use fastrace::prelude::SpanId;

pub(crate) fn tree_reporter() -> Box<dyn Reporter> {
    Box::new(TreeReporter)
}

pub(crate) fn print_traces(spans: Vec<SpanRecord>) {
    let _ = write_traces(&mut io::stderr().lock(), spans);
}

struct TreeReporter;

impl Reporter for TreeReporter {
    fn report(&mut self, spans: Vec<SpanRecord>) {
        print_traces(spans);
    }
}

fn write_traces(out: &mut impl Write, mut spans: Vec<SpanRecord>) -> io::Result<()> {
    spans.sort_unstable_by_key(|span| (span.trace_id, span.begin_time_unix_ns, span.span_id));
    for trace in spans.chunk_by(|left, right| left.trace_id == right.trace_id) {
        let first = &trace[0];
        let span_ids: HashSet<_> = trace.iter().map(|span| span.span_id).collect();
        let mut seen = HashSet::new();
        let mut children: HashMap<_, Vec<_>> = HashMap::new();
        let mut roots = Vec::new();
        for span in trace {
            if !seen.insert(span.span_id) {
                continue;
            }
            if span_ids.contains(&span.parent_id) {
                children.entry(span.parent_id).or_default().push(span);
            } else {
                // A parent may have been recorded by another process or in a previous batch.
                roots.push(span);
            }
        }

        writeln!(out, "trace_id={}", first.trace_id)?;
        writeln!(out, "{:>10}  {:>10}  Operation", "Duration", "Start")?;
        for root in roots {
            write_span(out, root, first.begin_time_unix_ns, &children, "", None)?;
        }
    }
    Ok(())
}

fn write_span(
    out: &mut impl Write,
    span: &SpanRecord,
    trace_start_ns: u64,
    children: &HashMap<SpanId, Vec<&SpanRecord>>,
    indent: &str,
    last_child: Option<bool>,
) -> io::Result<()> {
    let (branch, continuation) = match last_child {
        None => ("", ""),
        Some(true) => ("└─ ", "   "),
        Some(false) => ("├─ ", "│  "),
    };
    let duration = format!("{:.2?}", Duration::from_nanos(span.duration_ns));
    let start = Duration::from_nanos(span.begin_time_unix_ns - trace_start_ns);
    let start = format!("+{start:.2?}");
    write!(out, "{duration:>10}  {start:>10}  {indent}{branch}{}", span.name.escape_debug())?;
    for (key, value) in &span.properties {
        if !matches!(key.as_ref(), "component" | "rpc.system") {
            write!(out, "  {}={}", key.escape_debug(), value.escape_debug())?;
        }
    }
    writeln!(out)?;

    if let Some(siblings) = children.get(&span.span_id) {
        let indent = format!("{indent}{continuation}");
        for (index, child) in siblings.iter().enumerate() {
            let last = index + 1 == siblings.len();
            write_span(out, child, trace_start_ns, children, &indent, Some(last))?;
        }
    }
    Ok(())
}
