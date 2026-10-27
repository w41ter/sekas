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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::task::Context;
use std::time::{Duration, Instant};

use futures::channel::{mpsc, oneshot};
use futures::stream::FusedStream;
use futures::{FutureExt, SinkExt, StreamExt};
use log::{debug, info, warn};
use raft::prelude::*;
use raft::{SoftState, StateRole};
use raft_engine::{Engine, LogBatch};
use sekas_api::server::v1::{ChangeReplicas, RaftRole, ReplicaDesc};
use sekas_runtime::TaskGroup;
use tokio::time::{Interval, MissedTickBehavior, interval};

use super::applier::{Applier, ReplicaCache};
use super::fsm::StateMachine;
use super::io::{Channel, ChannelManager, LogWriter};
use super::metrics::*;
use super::monitor::WorkerPerfContext;
use super::node::RaftNode;
use super::snap::apply::apply_snapshot;
use super::snap::{RecycleSnapMode, SnapManager};
use super::{RaftManager, ReadPolicy};
use crate::raftgroup::monitor::record_perf_point;
use crate::serverpb::v1::{EvalResult, RaftMessage};
use crate::{RaftConfig, Result, record_latency};

// Leave room below the RPC hard limit for protobuf envelopes and future
// transport metadata. Oversized MsgAppend messages are split on entry
// boundaries before they reach the network stream.
const MAX_RAFT_TRANSPORT_MESSAGE_SIZE: usize = sekas_api::MAX_RPC_MESSAGE_SIZE - 1024 * 1024;

pub enum Request {
    Read {
        policy: ReadPolicy,
        sender: oneshot::Sender<Result<()>>,
    },
    Propose {
        eval_result: EvalResult,
        start: Instant,
        /// The expected term in raft node.
        term: Option<u64>,
        sender: oneshot::Sender<Result<()>>,
    },
    CreateSnapshotFinished,
    InstallSnapshot {
        msg: Message,
    },
    RejectSnapshot {
        msg: Message,
    },
    ChangeConfig {
        change: ChangeReplicas,
        sender: oneshot::Sender<Result<()>>,
    },
    Transfer {
        transferee: u64,
    },
    Message(RaftMessage),
    Unreachable {
        target_id: u64,
    },
    State(oneshot::Sender<RaftGroupState>),
    Monitor(oneshot::Sender<Box<WorkerPerfContext>>),
    Start,
}

pub struct PeerState {
    /// How much state is matched.
    pub matched: u64,
    /// The next index to apply
    pub next_idx: u64,

    /// Committed index in raft_log
    pub committed_index: u64,

    pub might_lost: bool,
}

#[derive(Default)]
pub struct RaftGroupState {
    /// The hardstate of the raft, representing voted state.
    pub hs: HardState,
    /// The softstate of the raft, representing proposed state.
    pub ss: SoftState,
    /// The index of the last entry which has been applied.
    pub applied: u64,
    /// The index of the last entry which has been committed.
    pub committed: u64,
    /// The first index of log entries.
    pub first_index: u64,
    /// The last index of log entries.
    pub last_index: u64,

    pub peers: HashMap<u64, PeerState>,
}

/// An abstraction for observing raft roles and state changes.
pub trait StateObserver: Send {
    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole);
}

struct SlowIoGuard {
    threshold: u64,
    start: Instant,
}

struct AdvanceImpl<'a> {
    group_id: u64,
    replica_id: u64,
    desc: ReplicaDesc,
    channels: &'a mut HashMap<u64, Channel>,
    trans_mgr: &'a Arc<ChannelManager>,
    snap_mgr: &'a SnapManager,
    observer: &'a mut Box<dyn StateObserver>,
    replica_cache: &'a mut ReplicaCache,
}

impl<'a> super::node::AdvanceTemplate for AdvanceImpl<'a> {
    fn send_messages(&mut self, msgs: Vec<Message>) {
        let mut separated_msgs: HashMap<u64, Vec<Message>> = HashMap::default();
        for msg in msgs {
            separated_msgs.entry(msg.to).or_default().push(msg);
        }
        for (target_id, msgs) in separated_msgs {
            let to_replica = match self.replica_cache.get(target_id) {
                Some(to_replica) => to_replica,
                None => {
                    warn!(
                        "send message to unknown target. group={}, target={target_id}",
                        self.group_id
                    );
                    continue;
                }
            };
            let channel = self
                .channels
                .entry(target_id)
                .or_insert_with(|| Channel::new(self.trans_mgr.clone()));
            for message in batch_raft_messages(self.group_id, self.desc, to_replica, msgs) {
                channel.send_message(message);
            }
        }
    }

    fn on_state_updated(&mut self, leader_id: u64, voted_for: u64, term: u64, role: RaftRole) {
        self.observer.on_state_updated(leader_id, voted_for, term, role);
    }

    fn mut_replica_cache(&mut self) -> &mut ReplicaCache {
        self.replica_cache
    }

    #[inline]
    fn apply_snapshot<M: StateMachine>(&mut self, applier: &mut Applier<M>, snapshot: &Snapshot) {
        apply_snapshot(self.replica_id, self.snap_mgr, applier, snapshot);
    }
}

fn batch_raft_messages(
    group_id: u64,
    from_replica: ReplicaDesc,
    to_replica: ReplicaDesc,
    messages: Vec<Message>,
) -> Vec<RaftMessage> {
    batch_raft_messages_with_limit(
        group_id,
        from_replica,
        to_replica,
        messages,
        MAX_RAFT_TRANSPORT_MESSAGE_SIZE,
    )
}

fn batch_raft_messages_with_limit(
    group_id: u64,
    from_replica: ReplicaDesc,
    to_replica: ReplicaDesc,
    messages: Vec<Message>,
    max_batch_len: usize,
) -> Vec<RaftMessage> {
    use prost::Message as _;

    let new_batch = || RaftMessage {
        group_id,
        from_replica: Some(from_replica),
        to_replica: Some(to_replica),
        messages: Vec::new(),
    };
    let mut batches = Vec::new();
    let mut batch = new_batch();
    let mut batch_len = batch.encoded_len();
    // A protobuf length delimiter is at most ten bytes for usize-sized values.
    // Reserving that full width makes this bound independent of the eventual
    // encoded message length.
    let max_message_len =
        max_batch_len.saturating_sub(batch_len).saturating_sub(1).saturating_sub(10);

    for message in
        messages.into_iter().flat_map(|message| split_append_message(message, max_message_len))
    {
        let message_len = message.encoded_len();
        // `messages` is field 4, whose protobuf tag occupies one byte.
        let field_len = 1 + prost::length_delimiter_len(message_len) + message_len;
        if !batch.messages.is_empty() && batch_len.saturating_add(field_len) > max_batch_len {
            batches.push(batch);
            batch = new_batch();
            batch_len = batch.encoded_len();
        }
        batch_len = batch_len.saturating_add(field_len);
        batch.messages.push(message);
    }

    if !batch.messages.is_empty() {
        if batch_len > max_batch_len {
            warn!(
                "raft message exceeds transport limit. group={group_id}, from={}, to={}, size={batch_len}",
                from_replica.id, to_replica.id
            );
        }
        batches.push(batch);
    }
    batches
}

fn split_append_message(mut message: Message, max_encoded_len: usize) -> Vec<Message> {
    use prost::Message as _;

    if message.get_msg_type() != MessageType::MsgAppend
        || message.entries.len() <= 1
        || message.encoded_len() <= max_encoded_len
    {
        return vec![message];
    }

    let entries = message.take_entries();
    let mut chunks = Vec::new();
    let mut chunk = message.clone();
    let mut chunk_len = chunk.encoded_len();

    for entry in entries {
        // `entries` is field 7 in eraftpb.Message, so its protobuf tag is one byte.
        let entry_len = entry.encoded_len();
        let field_len = 1 + prost::length_delimiter_len(entry_len) + entry_len;
        if !chunk.entries.is_empty() && chunk_len.saturating_add(field_len) > max_encoded_len {
            let previous = chunk.entries.last().expect("non-empty append chunk");
            let previous_index = previous.index;
            let previous_term = previous.term;
            chunks.push(chunk);

            chunk = message.clone();
            chunk.index = previous_index;
            chunk.log_term = previous_term;
            chunk_len = chunk.encoded_len();
        }
        chunk_len = chunk_len.saturating_add(field_len);
        chunk.entries.push(entry);
    }
    if !chunk.entries.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

/// A structure wraps raft node execution logics.
pub struct RaftWorker<M: StateMachine>
where
    Self: Send,
{
    cfg: RaftConfig,
    request_sender: mpsc::Sender<Request>,
    request_receiver: mpsc::Receiver<Request>,

    group_id: u64,
    desc: ReplicaDesc,
    raft_node: RaftNode<M>,

    channels: HashMap<u64, Channel>,
    trans_mgr: Arc<ChannelManager>,
    snap_mgr: SnapManager,
    engine: Arc<Engine>,
    observer: Box<dyn StateObserver>,
    replica_cache: ReplicaCache,

    task_group: TaskGroup,
    marker: PhantomData<M>,
}

#[derive(Default)]
struct WorkerContext {
    accumulated_bytes: usize,
    perf_ctx: WorkerPerfContext,
    monitors: Vec<oneshot::Sender<Box<WorkerPerfContext>>>,
}

impl<M> RaftWorker<M>
where
    M: StateMachine,
{
    pub async fn open(
        group_id: u64,
        replica_id: u64,
        node_id: u64,
        state_machine: M,
        raft_mgr: &RaftManager,
        mut observer: Box<dyn StateObserver>,
    ) -> Result<Self> {
        let desc = ReplicaDesc { id: replica_id, node_id, ..Default::default() };
        let mut replica_cache = ReplicaCache::default();
        replica_cache.insert(desc);
        replica_cache.batch_insert(&state_machine.descriptor().replicas);
        let raft_node = RaftNode::new(group_id, replica_id, raft_mgr, state_machine).await?;

        let (mut request_sender, request_receiver) =
            mpsc::channel(raft_mgr.cfg.max_inflight_requests);
        request_sender.send(Request::Start).await.unwrap();

        observer.on_state_updated(
            raft_node.raft().leader_id,
            raft_node.raft().vote,
            raft_node.raft().term,
            RaftRole::Follower,
        );

        Ok(RaftWorker {
            cfg: raft_mgr.cfg.clone(),
            request_sender,
            request_receiver,
            group_id,
            desc,
            raft_node,
            channels: HashMap::new(),
            trans_mgr: raft_mgr.transport_mgr.clone(),
            snap_mgr: raft_mgr.snap_mgr.clone(),
            engine: raft_mgr.engine.clone(),
            observer,
            replica_cache,
            task_group: TaskGroup::default(),
            marker: PhantomData,
        })
    }

    #[inline]
    pub fn request_sender(&self) -> mpsc::Sender<Request> {
        self.request_sender.clone()
    }

    /// Poll requests and messages, forward both to `RaftNode`, and advance
    /// `RaftNode`.
    pub async fn run(mut self, log_writer: LogWriter) -> Result<()> {
        debug!("group {} replica {} raft worker is running", self.group_id, self.desc.id);

        let mut log_writer = log_writer;
        // WARNING: the underlying instant isn't steady.
        let mut interval = interval(Duration::from_millis(self.cfg.tick_interval_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        while !self.request_receiver.is_terminated() {
            let mut ctx = WorkerContext::default();
            self.maintenance(&mut ctx, &mut interval).await?;
            self.consume_requests(&mut ctx)?;
            self.dispatch(&mut ctx, &mut log_writer).await?;
            self.finish_round(ctx);
        }

        debug!("group {} replica {} raft worker is quit", self.group_id, self.desc.id);

        Ok(())
    }

    async fn maintenance(
        &mut self,
        ctx: &mut WorkerContext,
        interval: &mut Interval,
    ) -> Result<()> {
        if !self.raft_node.has_ready() {
            sekas_runtime::select! {
                biased;
                _ = interval.tick().fuse() => {
                    self.on_tick_fire(ctx);
                },
                request = self.request_receiver.next() => {
                    if let Some(req) = request {
                        self.handle_request(ctx, req)?;
                    }
                },
            }
            record_perf_point(&mut ctx.perf_ctx.wake);
        } else {
            // Because the raft node is still in the ready state, there is no need for a
            // block to wait for a new input event, but the tick may be ready at
            // this time, and we need to process it first to avoid the tick
            // delay being too long.
            let mut cx = Context::from_waker(futures::task::noop_waker_ref());
            if interval.poll_tick(&mut cx).is_ready() {
                self.on_tick_fire(ctx);
            }
        }
        Ok(())
    }

    fn on_tick_fire(&mut self, ctx: &mut WorkerContext) {
        self.raft_node.tick();
        self.compact_log(ctx);
    }

    fn consume_requests(&mut self, ctx: &mut WorkerContext) -> Result<()> {
        record_latency!(&RAFTGROUP_WORKER_CONSUME_REQUESTS_DURATION_SECONDS);
        record_perf_point(&mut ctx.perf_ctx.consume_requests);
        while let Ok(request) = self.request_receiver.try_recv() {
            self.handle_request(ctx, request)?;
            if ctx.accumulated_bytes >= self.cfg.max_io_batch_size as usize {
                break;
            }
        }
        Ok(())
    }

    async fn dispatch(&mut self, ctx: &mut WorkerContext, writer: &mut LogWriter) -> Result<()> {
        RAFTGROUP_WORKER_ACCUMULATED_BYTES_SIZE.observe(ctx.accumulated_bytes as f64);
        RAFTGROUP_WORKER_ADVANCE_TOTAL.inc();
        record_latency!(&RAFTGROUP_WORKER_ADVANCE_DURATION_SECONDS);
        let mut template = AdvanceImpl {
            replica_id: self.desc.id,
            group_id: self.group_id,
            desc: self.desc,
            channels: &mut self.channels,
            trans_mgr: &self.trans_mgr,
            snap_mgr: &self.snap_mgr,
            observer: &mut self.observer,
            replica_cache: &mut self.replica_cache,
        };
        if let Some(write_task) = self.raft_node.advance(&mut ctx.perf_ctx.advance, &mut template) {
            let mut batch = LogBatch::default();
            self.raft_node.mut_store().write(&mut batch, &write_task).expect("write log batch");

            let _slow_io_guard = self.cfg.engine_slow_io_threshold_ms.map(SlowIoGuard::new);
            record_perf_point(&mut ctx.perf_ctx.write);
            ctx.perf_ctx.num_writes = write_task.entries.len();
            {
                record_latency!(&RAFTGROUP_WORKER_WRITE_DURATION_SECONDS);
                // TODO(walter) handle io error.
                writer.submit(batch).await.unwrap_or(Ok(())).unwrap();
            }
            let post_ready = write_task.post_ready();
            self.raft_node.post_advance(&mut ctx.perf_ctx.advance, post_ready, &mut template);
        }

        if self.raft_node.mut_store().create_snapshot.get() {
            self.raft_node.mut_store().create_snapshot.set(false);
            let handle = super::snap::dispatch_creating_snap_task(
                self.desc.id,
                self.request_sender.clone(),
                self.raft_node.mut_state_machine(),
                self.snap_mgr.clone(),
            );
            self.task_group.add_task(handle);
        }

        Ok(())
    }

    fn finish_round(&self, mut ctx: WorkerContext) {
        record_perf_point(&mut ctx.perf_ctx.finish);
        ctx.perf_ctx.accumulated_bytes = ctx.accumulated_bytes;
        for sender in ctx.monitors {
            sender.send(Box::new(ctx.perf_ctx.clone())).unwrap_or_default();
        }
    }

    fn handle_request(&mut self, ctx: &mut WorkerContext, request: Request) -> Result<()> {
        ctx.perf_ctx.num_requests += 1;
        match request {
            Request::Propose { eval_result, start, term, sender } => {
                self.handle_proposal(ctx, eval_result, start, term, sender)
            }
            Request::Read { policy, sender } => self.handle_read(policy, sender),
            Request::ChangeConfig { change, sender } => self.handle_conf_change(change, sender),
            Request::CreateSnapshotFinished => {
                self.raft_node.mut_store().is_creating_snapshot.set(false);
            }
            Request::Transfer { transferee: target_id } => {
                self.raft_node.transfer_leader(target_id);
            }
            Request::Message(msg) => {
                self.handle_msg(ctx, msg)?;
            }
            Request::Unreachable { target_id } => {
                self.raft_node.report_unreachable(target_id);
            }
            Request::RejectSnapshot { msg: input } => {
                let mut msg = Message::default();
                msg.set_msg_type(MessageType::MsgSnapStatus);
                msg.from = input.to;
                msg.to = input.from;
                msg.reject = true;

                if let Some(to_replica) = self.replica_cache.get(input.from) {
                    self.channels
                        .entry(input.from)
                        .or_insert_with(|| Channel::new(self.trans_mgr.clone()))
                        .send_message(RaftMessage {
                            group_id: self.group_id,
                            from_replica: Some(self.desc),
                            to_replica: Some(to_replica),
                            messages: vec![msg],
                        });
                }
            }
            Request::InstallSnapshot { msg } => {
                self.raft_node.step(msg)?;
            }
            Request::State(sender) => {
                let store = self.raft_node.mut_store();
                let first_index = store.first_index().unwrap();
                let last_index = store.last_index().unwrap();
                sender.send(self.raft_group_state(first_index, last_index)).unwrap_or_default();
            }
            Request::Monitor(sender) => {
                ctx.monitors.push(sender);
            }
            Request::Start => {}
        }
        Ok(())
    }

    fn handle_msg(&mut self, ctx: &mut WorkerContext, raft_msg: RaftMessage) -> Result<()> {
        let from_replica = raft_msg.from_replica.unwrap();
        self.replica_cache.insert(from_replica);
        for msg in raft_msg.messages {
            if msg.get_msg_type() == MessageType::MsgSnapshot {
                // TODO(walter) In order to avoid useless downloads, should check whether this
                // snapshot will be accept.
                let handle = super::snap::dispatch_downloading_snap_task(
                    self.desc.id,
                    self.request_sender.clone(),
                    self.snap_mgr.clone(),
                    self.trans_mgr.clone(),
                    from_replica,
                    msg,
                );
                self.task_group.add_task(handle);
            } else {
                ctx.accumulated_bytes += msg.entries.iter().map(|e| e.data.len()).sum::<usize>();
                ctx.perf_ctx.num_step_msg += 1;
                self.raft_node.step(msg)?;
            }
        }
        Ok(())
    }

    fn handle_proposal(
        &mut self,
        ctx: &mut WorkerContext,
        eval_result: EvalResult,
        start: Instant,
        term: Option<u64>,
        sender: oneshot::Sender<Result<()>>,
    ) {
        use prost::Message;

        let data = eval_result.encode_to_vec();
        ctx.accumulated_bytes += data.len();
        ctx.perf_ctx.num_proposal += 1;
        self.raft_node.propose(data, vec![], term, sender);
        RAFTGROUP_WORKER_REQUEST_IN_QUEUE_DURATION_SECONDS.observe(elapsed_seconds(start));
    }

    fn handle_conf_change(&mut self, change: ChangeReplicas, sender: oneshot::Sender<Result<()>>) {
        info!("group {} replica {} handle conf change {change:?}", self.group_id, self.desc.id);
        let cc = super::encode_to_conf_change(change);
        self.raft_node.propose_conf_change(vec![], cc, sender);
    }

    fn handle_read(&mut self, policy: ReadPolicy, sender: oneshot::Sender<Result<()>>) {
        match policy {
            ReadPolicy::Relaxed => {
                panic!("not support");
            }
            ReadPolicy::LeaseRead => {
                self.raft_node.lease_read(sender);
            }
            ReadPolicy::ReadIndex => {
                self.raft_node.read_index(sender);
            }
        }
    }

    fn compact_log(&mut self, ctx: &mut WorkerContext) {
        record_latency!(&RAFTGROUP_WORKER_COMPACT_LOG_DURATION_SECONDS);
        record_perf_point(&mut ctx.perf_ctx.compact_log);
        let mut to = self.raft_node.mut_state_machine().flushed_index();

        let status = self.raft_node.raft_status();
        if status.ss.raft_state == StateRole::Leader {
            if let Some(min_matched_index) =
                status.progress.and_then(|p| p.iter().map(|(_, p)| p.matched).min())
            {
                to = std::cmp::min(min_matched_index, to);
            }
        }

        let store = self.raft_node.mut_store();
        if store.first_index().unwrap() < to {
            let mut lb = store.compact_to(to);
            self.engine.write(&mut lb, false).unwrap();
        }

        self.snap_mgr.recycle_snapshots(self.desc.id, RecycleSnapMode::RequiredIndex(to));
    }

    fn raft_group_state(&self, first_index: u64, last_index: u64) -> RaftGroupState {
        let status = self.raft_node.raft_status();

        let mut peer_states = HashMap::new();
        if let Some(tracker) = status.progress {
            for (id, progress) in tracker.iter() {
                let state = PeerState {
                    matched: progress.matched,
                    next_idx: progress.next_idx,
                    committed_index: progress.next_idx,
                    might_lost: progress.might_lost,
                };
                peer_states.insert(*id, state);
            }
        }

        RaftGroupState {
            hs: status.hs,
            ss: status.ss,
            applied: status.applied,
            committed: self.raft_node.committed_index(),
            first_index,
            last_index,
            peers: peer_states,
        }
    }
}

impl SlowIoGuard {
    fn new(threshold: u64) -> Self {
        SlowIoGuard { threshold, start: Instant::now() }
    }
}

impl Drop for SlowIoGuard {
    fn drop(&mut self) {
        if self.start.elapsed() >= Duration::from_millis(self.threshold) {
            warn!("raft engine slow io: {:?}", raft_engine::get_perf_context());
        }
    }
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;

    #[test]
    fn raft_transport_batches_stay_within_grpc_message_limit() {
        let from = ReplicaDesc { id: 1, node_id: 1, ..Default::default() };
        let to = ReplicaDesc { id: 2, node_id: 2, ..Default::default() };
        let messages = (0..100)
            .map(|index| Message { index, context: vec![0; 64 * 1024], ..Default::default() })
            .collect::<Vec<_>>();

        let max_batch_len = 1024 * 1024;
        let batches = batch_raft_messages_with_limit(7, from, to, messages, max_batch_len);
        assert!(batches.len() > 1);
        assert!(batches.iter().all(|batch| batch.encoded_len() <= max_batch_len));
        assert_eq!(
            batches
                .into_iter()
                .flat_map(|batch| batch.messages)
                .map(|message| message.index)
                .collect::<Vec<_>>(),
            (0..100).collect::<Vec<_>>()
        );
    }

    #[test]
    fn oversized_append_is_split_on_entry_boundaries() {
        let from = ReplicaDesc { id: 1, node_id: 1, ..Default::default() };
        let to = ReplicaDesc { id: 2, node_id: 2, ..Default::default() };
        let entries = (11..15)
            .map(|index| Entry {
                index,
                term: if index < 13 { 2 } else { 3 },
                data: vec![0; 64 * 1024],
                ..Default::default()
            })
            .collect();
        let message = Message {
            msg_type: MessageType::MsgAppend.into(),
            to: 2,
            from: 1,
            term: 3,
            log_term: 2,
            index: 10,
            commit: 14,
            entries,
            ..Default::default()
        };

        let max_batch_len = 128 * 1024;
        let batches = batch_raft_messages_with_limit(7, from, to, vec![message], max_batch_len);
        assert!(batches.iter().all(|batch| batch.encoded_len() <= max_batch_len));
        let chunks = batches.into_iter().flat_map(|batch| batch.messages).collect::<Vec<_>>();
        assert!(chunks.len() > 1);

        let mut previous_index = 10;
        let mut previous_term = 2;
        let mut entry_indices = Vec::new();
        for chunk in &chunks {
            assert_eq!(chunk.get_msg_type(), MessageType::MsgAppend);
            assert_eq!(chunk.index, previous_index);
            assert_eq!(chunk.log_term, previous_term);
            assert_eq!((chunk.from, chunk.to, chunk.term, chunk.commit), (1, 2, 3, 14));
            entry_indices.extend(chunk.entries.iter().map(|entry| entry.index));
            let last = chunk.entries.last().unwrap();
            previous_index = last.index;
            previous_term = last.term;
        }
        assert_eq!(entry_indices, vec![11, 12, 13, 14]);
    }
}
