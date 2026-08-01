// Copyright 2026-present The Sekas Authors.
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

use serde::{Deserialize, Serialize};

pub type CallId = u64;
pub type ProcessId = usize;
pub type KvValue = Vec<u8>;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum KvOp {
    Get { key: Vec<u8> },
    Put { key: Vec<u8>, value: KvValue },
    Delete { key: Vec<u8> },
}

impl KvOp {
    pub fn key(&self) -> &[u8] {
        match self {
            KvOp::Get { key } | KvOp::Put { key, .. } | KvOp::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum CallResult {
    Get(Option<KvValue>),
    Put,
    Delete,
    Fail(String),
    Info(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Call {
    pub id: CallId,
    pub process: ProcessId,
    pub op: KvOp,
    pub result: CallResult,
    pub invoke_time: u64,
    pub complete_time: u64,
}

impl Call {
    pub fn key(&self) -> &[u8] {
        self.op.key()
    }

    pub fn is_read(&self) -> bool {
        matches!(self.op, KvOp::Get { .. })
    }

    pub fn is_success(&self) -> bool {
        matches!(self.result, CallResult::Get(_) | CallResult::Put | CallResult::Delete)
    }

    pub fn is_info(&self) -> bool {
        matches!(self.result, CallResult::Info(_))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventKind {
    Invoke,
    Ok,
    Fail,
    Info,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: CallId,
    pub process: ProcessId,
    pub kind: EventKind,
    pub op: KvOp,
    pub result: Option<CallResult>,
    pub time: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct History {
    pub calls: Vec<Call>,
    pub events: Vec<Event>,
}

impl History {
    pub fn from_calls(calls: Vec<Call>) -> Self {
        let mut events = Vec::with_capacity(calls.len() * 2);
        for call in &calls {
            events.push(Event {
                id: call.id,
                process: call.process,
                kind: EventKind::Invoke,
                op: call.op.clone(),
                result: None,
                time: call.invoke_time,
            });
            events.push(Event {
                id: call.id,
                process: call.process,
                kind: match call.result {
                    CallResult::Fail(_) => EventKind::Fail,
                    CallResult::Info(_) => EventKind::Info,
                    CallResult::Get(_) | CallResult::Put | CallResult::Delete => EventKind::Ok,
                },
                op: call.op.clone(),
                result: Some(call.result.clone()),
                time: call.complete_time,
            });
        }
        events.sort_by_key(|event| (event.time, event.id, matches!(event.kind, EventKind::Invoke)));
        History { calls, events }
    }
}
