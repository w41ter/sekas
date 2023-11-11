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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project::pin_project;

use crate::ExecutorConfig;

enum TaskState {
    First(Instant),
    Polled(Duration),
}

pub type JoinError = tokio::task::JoinError;

/// A handle that awaits the result of a task.
///
/// Dropping a [`DispatchHandle`] will abort the underlying task.
#[must_use = "Drop this `JoinHandle` will abort the underlying task"]
#[derive(Debug)]
pub struct JoinHandle<T> {
    inner: tokio::task::JoinHandle<T>,
}

pub struct ExecutorOwner {
    runtime: tokio::runtime::Runtime,
}

/// An execution service.
#[derive(Clone)]
pub struct Executor
where
    Self: Send + Sync,
{
    handle: tokio::runtime::Handle,
}

#[pin_project]
struct FutureWrapper<F: Future> {
    #[pin]
    inner: F,
    state: TaskState,
}

impl ExecutorOwner {
    /// New executor and setup the underlying threads, scheduler.
    pub fn new(num_threads: usize) -> Self {
        Self::with_config(num_threads, ExecutorConfig::default())
    }

    pub fn with_config(num_threads: usize, cfg: ExecutorConfig) -> Self {
        use tokio::runtime::Builder;
        let runtime = Builder::new_multi_thread()
            .worker_threads(num_threads)
            .enable_all()
            .event_interval(cfg.event_interval.unwrap_or(61))
            .global_queue_interval(cfg.global_event_interval.unwrap_or(64))
            .max_blocking_threads(cfg.max_blocking_threads.unwrap_or(2))
            .thread_keep_alive(Duration::from_secs(60))
            .build()
            .expect("build tokio runtime");
        ExecutorOwner { runtime }
    }

    pub fn executor(&self) -> Executor {
        Executor { handle: self.runtime.handle().clone() }
    }
}

impl Executor {
    /// Spawns a task.
    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        JoinHandle { inner: self.handle.spawn(FutureWrapper::new(future)) }
    }

    /// Runs a future to completion on the executor. This is the executor’s
    /// entry point.
    #[inline]
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send + 'static,
    {
        self.handle.block_on(future)
    }

    #[inline]
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        JoinHandle { inner: self.handle.spawn_blocking(func) }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx)
    }
}

impl<T> JoinHandle<T> {
    /// Checks if the task associated with this `JoinHandle` has finished.
    #[inline]
    pub fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        self.inner.abort();
    }
}

impl<F: Future> FutureWrapper<F> {
    fn new(inner: F) -> Self {
        FutureWrapper { state: TaskState::First(Instant::now()), inner }
    }
}

impl<F: Future> Future for FutureWrapper<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut duration = match this.state {
            TaskState::First(_) => Duration::ZERO,
            TaskState::Polled(duration) => *duration,
        };

        let start = Instant::now();
        let output = Pin::new(&mut this.inner).poll(cx);
        let elapsed = start.elapsed();
        if !should_skip_slow_log::<F>() && elapsed >= Duration::from_micros(1000) {
            tracing::warn!(
                "future poll() execute total {elapsed:?}: {}",
                std::any::type_name::<F>(),
            );
        }

        duration += elapsed;
        *this.state = TaskState::Polled(duration);

        output
    }
}

/// Returns a `Executor` view over the currently running `ExecutorOwner`.
///
/// # Panics
///
/// This will panic if called outside the context of a runtime.
#[inline]
pub fn current() -> Executor {
    Executor { handle: tokio::runtime::Handle::current() }
}

/// Spawns a task with current `Executor`.
///
/// # Panics
///
/// This will panic if called outside the context of a runtime.
#[inline]
pub fn spawn<F, T>(future: F) -> JoinHandle<F::Output>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    current().spawn(future)
}

#[inline]
pub fn spawn_blocking<F, R>(func: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    current().spawn_blocking(func)
}

#[inline]
const fn should_skip_slow_log<F: Future>() -> bool {
    const_str::contains!(std::any::type_name::<F>(), "start_raft_group")
}
