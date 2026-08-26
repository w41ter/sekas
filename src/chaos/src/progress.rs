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

use std::sync::atomic::{AtomicU64, Ordering};

/// A snapshot of a writer's published state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressSnapshot {
    /// Number of operations whose post-state has been reconciled.
    pub completed: u64,
    /// False while the writer may be changing database state.
    pub stable: bool,
    raw: u64,
}

/// A single-atomic seqlock used to publish a writer's stable prefix.
///
/// Even values encode a stable completed count. Odd values mean the writer is
/// executing or reconciling the next operation. A reader may judge its reads
/// only when snapshots taken before and after the reads are the same stable
/// value.
#[derive(Debug, Default)]
pub struct WriterProgress {
    state: AtomicU64,
}

impl WriterProgress {
    const WRITING_BIT: u64 = 1;
    const MAX_COMPLETED: u64 = u64::MAX >> 1;

    pub const fn new() -> Self {
        Self { state: AtomicU64::new(0) }
    }

    pub fn load(&self) -> ProgressSnapshot {
        Self::decode(self.state.load(Ordering::Acquire))
    }

    /// Marks the next operation as in progress.
    ///
    /// There must be exactly one writer for this progress instance.
    pub fn begin(&self) -> ProgressSnapshot {
        let current = self.state.load(Ordering::Relaxed);
        let snapshot = Self::decode(current);
        assert!(snapshot.stable, "writer operation is already in progress");
        self.state.store(current | Self::WRITING_BIT, Ordering::Release);
        Self::decode(current | Self::WRITING_BIT)
    }

    /// Publishes one more reconciled operation.
    pub fn finish(&self) -> ProgressSnapshot {
        let current = self.state.load(Ordering::Relaxed);
        let snapshot = Self::decode(current);
        assert!(!snapshot.stable, "writer operation has not started");
        assert!(snapshot.completed < Self::MAX_COMPLETED, "writer progress overflow");
        let next = (snapshot.completed + 1) << 1;
        self.state.store(next, Ordering::Release);
        Self::decode(next)
    }

    /// Returns to the previous stable state after proving that the in-flight
    /// operation did not take effect. This does not advance progress.
    pub fn abort(&self) -> ProgressSnapshot {
        let current = self.state.load(Ordering::Relaxed);
        let snapshot = Self::decode(current);
        assert!(!snapshot.stable, "writer operation has not started");
        let stable = snapshot.completed << 1;
        self.state.store(stable, Ordering::Release);
        Self::decode(stable)
    }

    /// Returns true when a reader's observations were made against one stable
    /// writer state.
    pub fn unchanged_since(&self, before: ProgressSnapshot) -> bool {
        before.stable && self.state.load(Ordering::Acquire) == before.raw
    }

    const fn decode(raw: u64) -> ProgressSnapshot {
        ProgressSnapshot { completed: raw >> 1, stable: raw & Self::WRITING_BIT == 0, raw }
    }
}

#[cfg(test)]
mod tests {
    use super::WriterProgress;

    #[test]
    fn only_unchanged_stable_progress_can_be_checked() {
        let progress = WriterProgress::new();
        let initial = progress.load();
        assert!(progress.unchanged_since(initial));

        let writing = progress.begin();
        assert!(!writing.stable);
        assert!(!progress.unchanged_since(initial));
        assert!(!progress.unchanged_since(writing));

        let completed = progress.finish();
        assert_eq!(completed.completed, 1);
        assert!(completed.stable);
        assert!(progress.unchanged_since(completed));
    }
}
