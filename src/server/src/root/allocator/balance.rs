// Copyright 2026-present The Sekas Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use super::{BalanceDecision, BalanceThresholds};
use crate::BalanceDimension;

const EPSILON: f64 = 1e-9;

pub(super) struct Axis {
    pub kind: BalanceDimension,
    loads: HashMap<u64, f64>,
    ready: bool,
    thresholds: BalanceThresholds,
    deadline: Option<f64>,
}

impl Axis {
    pub fn new(
        kind: BalanceDimension,
        loads: HashMap<u64, f64>,
        expected: usize,
        min_tolerance: f64,
        relative_tolerance: f64,
        deadline: Option<f64>,
    ) -> Self {
        let mut values: Vec<_> = loads.iter().collect();
        values.sort_unstable_by_key(|&(&id, _)| id);
        let mean =
            values.into_iter().map(|(_, &value)| value).sum::<f64>() / expected.max(1) as f64;
        let tolerance = min_tolerance.max(mean * relative_tolerance);
        let max = loads.values().copied().fold(0., f64::max);
        Self {
            kind,
            ready: loads.len() == expected,
            loads,
            thresholds: BalanceThresholds::new(mean, tolerance, max),
            deadline,
        }
    }

    pub fn may_donate(&self, id: u64) -> bool {
        self.ready
            && self.loads.get(&id).is_some_and(|&load| {
                load > self.thresholds.high || self.deadline.is_some_and(|limit| load > limit)
            })
    }

    pub fn report(&self, result: &str) -> BalanceDecision {
        if !self.ready {
            return BalanceDecision::Deferred("waiting for current load samples".into());
        }
        BalanceDecision::NoAction(result.to_owned())
    }

    pub fn thresholds(&self) -> Option<BalanceThresholds> {
        self.ready.then_some(self.thresholds)
    }

    fn penalty(&self, value: f64) -> f64 {
        (value - self.thresholds.high).max(0.) + (self.thresholds.low - value).max(0.)
    }

    pub fn effect(&self, change: &Change) -> Option<Effect> {
        let mut effect = Effect { preserves_balance: true, ..Default::default() };
        if change.unknown && self.deadline.is_some() {
            return None;
        }
        for delta in &change.deltas {
            let Some(&before) = self.loads.get(&delta.id) else {
                if self.deadline.is_some() && (delta.steady > 0. || delta.incoming > 0.) {
                    return None;
                }
                continue;
            };
            let after = (before + delta.steady).max(0.);
            if let Some(limit) = self.deadline {
                // Incoming copies can use resources before their old copies are
                // removed, including when both groups share a replica node.
                let peak = before + delta.incoming.max(delta.steady).max(0.);
                if peak > before.max(limit) + EPSILON {
                    return None;
                }
                effect.relief += (before - limit).max(0.) - (after - limit).max(0.);
            }
            let improvement = self.penalty(before) - self.penalty(after);
            effect.improvement += improvement;
            effect.preserves_balance &= improvement >= -EPSILON;
            effect.donates |= delta.steady < 0. && before > self.thresholds.high + EPSILON;
            effect.receives_below_low |= delta.steady > 0. && before <= self.thresholds.low;
        }
        effect.known = change.deltas.is_empty() && !change.unknown || self.ready && !change.unknown;
        Some(effect)
    }
}

#[derive(Default)]
pub(super) struct Effect {
    relief: f64,
    improvement: f64,
    donates: bool,
    receives_below_low: bool,
    known: bool,
    preserves_balance: bool,
}

impl Effect {
    pub fn score(&self) -> Option<(f64, f64, bool)> {
        (self.known && (self.relief > EPSILON || self.donates && self.improvement > EPSILON))
            .then_some((self.relief, self.improvement, self.receives_below_low))
    }

    pub fn preserves_balance(&self) -> bool {
        self.known && self.preserves_balance
    }
}

#[derive(Default)]
pub(super) struct Change {
    pub unknown: bool,
    deltas: Vec<Delta>,
}

struct Delta {
    id: u64,
    steady: f64,
    incoming: f64,
}

impl Change {
    pub fn add(&mut self, id: u64, steady: f64, incoming: f64) {
        if steady == 0. && incoming == 0. {
            return;
        }
        if let Some(delta) = self.deltas.iter_mut().find(|delta| delta.id == id) {
            delta.steady += steady;
            delta.incoming += incoming;
        } else {
            self.deltas.push(Delta { id, steady, incoming });
        }
    }

    pub fn transfer(&mut self, source: u64, target: u64, amount: Option<f64>) {
        if source == target {
            return;
        }
        if let Some(amount) = amount {
            self.add(source, -amount, 0.);
            self.add(target, amount, 0.);
        } else {
            self.unknown = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_deadline_at_copy_peak_and_allows_repair() {
        let disk =
            Axis::new(BalanceDimension::Disk, [(1, 95.), (2, 84.)].into(), 2, 5., 0., Some(90.));
        let mut movement = Change::default();
        movement.transfer(1, 2, Some(5.));
        assert_eq!(disk.effect(&movement).unwrap().score().unwrap().0, 5.);
        movement.add(2, 0., 7.);
        assert!(disk.effect(&movement).is_none());

        let mut overlapping_copy = Change::default();
        overlapping_copy.add(1, 0., 1.);
        assert!(disk.effect(&overlapping_copy).is_none());
    }

    #[test]
    fn deadband_blocks_churn_and_loss_of_higher_priority_balance() {
        let axis =
            Axis::new(BalanceDimension::ReadUnits, [(1, 100.), (2, 110.)].into(), 2, 1., 0.1, None);
        let mut small = Change::default();
        small.transfer(2, 1, Some(3.));
        let effect = axis.effect(&small).unwrap();
        assert!(effect.score().is_none());
        assert!(effect.preserves_balance());

        let mut large = Change::default();
        large.transfer(1, 2, Some(20.));
        assert!(!axis.effect(&large).unwrap().preserves_balance());
    }
}
