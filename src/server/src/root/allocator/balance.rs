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
    observed_thresholds: Option<BalanceThresholds>,
    copy_peaks: HashMap<u64, f64>,
    deadline: Option<f64>,
    weights: Option<HashMap<u64, f64>>,
    count_targets: HashMap<u64, (f64, f64)>,
    soft_balance: bool,
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
        let ready = loads.len() == expected;
        let thresholds = Self::limits(&loads, None, min_tolerance, relative_tolerance);
        Self {
            kind,
            ready,
            thresholds,
            loads,
            observed_thresholds: ready.then_some(thresholds),
            copy_peaks: HashMap::new(),
            deadline,
            weights: None,
            count_targets: HashMap::new(),
            soft_balance: !matches!(
                kind,
                BalanceDimension::LeaderCount
                    | BalanceDimension::ReplicaCount
                    | BalanceDimension::ShardCount
            ),
        }
    }

    fn limits(
        loads: &HashMap<u64, f64>,
        weights: Option<&HashMap<u64, f64>>,
        floor: f64,
        relative: f64,
    ) -> BalanceThresholds {
        let mut ids: Vec<_> = weights.unwrap_or(loads).keys().copied().collect();
        ids.sort_unstable();
        let capacity =
            weights.map_or(ids.len() as f64, |weights| ids.iter().map(|id| weights[id]).sum());
        let total: f64 = ids
            .iter()
            .filter_map(|id| loads.get(id).map(|load| load * weights.map_or(1., |w| w[id])))
            .sum();
        let mean = if capacity > 0. { total / capacity } else { 0. };
        let max = loads.values().copied().fold(0., f64::max);
        BalanceThresholds::new(mean, floor.max(mean * relative), max)
    }

    pub fn with_pending(mut self, pending: &Change, floor: f64, relative: f64) -> Self {
        if self.deadline.is_some() {
            self.copy_peaks.extend(pending.unknown_receivers.iter().map(|&id| (id, f64::INFINITY)));
        }
        if pending.deltas.is_empty() {
            return self;
        }
        for delta in &pending.deltas {
            if let Some(load) = self.loads.get_mut(&delta.id) {
                let weight = self.weights.as_ref().map_or(1., |weights| weights[&delta.id]);
                if self.deadline.is_some() {
                    // Reserve incoming load before assuming any source has released it.
                    self.copy_peaks.entry(delta.id).or_insert(*load + delta.incoming / weight);
                }
                *load = (*load + delta.steady / weight).max(0.);
            }
        }
        self.thresholds = Self::limits(&self.loads, self.weights.as_ref(), floor, relative);
        self
    }

    pub fn with_observed_thresholds(mut self, thresholds: Option<BalanceThresholds>) -> Self {
        self.observed_thresholds = thresholds;
        self
    }

    pub fn with_soft_balance(mut self, enabled: bool) -> Self {
        self.soft_balance = enabled;
        self
    }

    pub fn with_count_targets(mut self, weights: HashMap<u64, f64>) -> Self {
        let total: f64 = self.loads.values().sum();
        let capacity: f64 = weights.values().sum();
        if capacity > 0. {
            self.count_targets = weights
                .into_iter()
                .map(|(id, weight)| {
                    let target = total * (weight / capacity);
                    (id, (target.floor(), target.ceil()))
                })
                .collect();
        }
        self
    }

    pub fn requests(
        kind: BalanceDimension,
        totals: HashMap<u64, f64>,
        weights: HashMap<u64, f64>,
        ready: bool,
    ) -> Self {
        let loads: HashMap<_, _> =
            totals.into_iter().map(|(id, load)| (id, load / weights[&id])).collect();
        let thresholds = Self::limits(&loads, Some(&weights), 1., 0.1);
        Self {
            kind,
            ready,
            thresholds,
            observed_thresholds: ready.then_some(thresholds),
            copy_peaks: HashMap::new(),
            loads,
            deadline: None,
            weights: Some(weights),
            count_targets: HashMap::new(),
            soft_balance: true,
        }
    }

    pub fn may_donate(&self, id: u64) -> bool {
        self.ready
            && self.drives_placement()
            && self.loads.get(&id).is_some_and(|&load| {
                load > self.bounds(id).1 || self.deadline.is_some_and(|limit| load > limit)
            })
    }

    pub fn has_undersupplied_node(&self) -> bool {
        self.ready && self.loads.values().any(|&load| load < self.thresholds.low)
    }

    pub fn has_imbalance(&self) -> bool {
        self.ready
            && self.loads.iter().any(|(&id, &load)| {
                let (low, high) = self.bounds(id);
                load < low || load > high
            })
    }

    pub fn drives_placement(&self) -> bool {
        self.deadline.is_some() || self.soft_balance
    }

    pub fn score(&self, effect: &Effect) -> Option<(f64, f64, bool)> {
        let score = effect.score()?;
        (self.ready && (self.soft_balance || score.0 > EPSILON)).then_some(score)
    }

    pub fn report(&self, result: &str) -> BalanceDecision {
        if !self.ready {
            return BalanceDecision::Deferred("waiting for current load samples".into());
        }
        BalanceDecision::NoAction(result.to_owned())
    }

    pub fn thresholds(&self) -> Option<BalanceThresholds> {
        self.observed_thresholds
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn relative_gain(&self, effect: &Effect) -> f64 {
        if !effect.known {
            return 0.;
        }
        let scale = self.thresholds.avg.max(1.);
        if self.weights.is_some() {
            effect.improvement / scale.powi(2)
        } else {
            effect.improvement / scale
        }
    }

    fn bounds(&self, id: u64) -> (f64, f64) {
        self.count_targets.get(&id).copied().unwrap_or((self.thresholds.low, self.thresholds.high))
    }

    fn penalty(&self, id: u64, value: f64, weight: Option<f64>) -> f64 {
        let (low, high) = self.bounds(id);
        let deviation = (value - high).max(0.) + (low - value).max(0.);
        weight.map_or(deviation, |weight| weight * deviation.powi(2))
    }

    pub fn effect(&self, change: &Change) -> Option<Effect> {
        let mut effect =
            Effect { known: !change.unknown, preserves_balance: true, ..Default::default() };
        if change.unknown && self.deadline.is_some() {
            return None;
        }
        if self.weights.is_some() && !self.ready && !change.deltas.is_empty() {
            return self.deadline.is_none().then_some(Effect::default());
        }
        for delta in &change.deltas {
            let Some(&before) = self.loads.get(&delta.id) else {
                effect.known = false;
                if self.deadline.is_some() && (delta.steady > 0. || delta.incoming > 0.) {
                    return None;
                }
                continue;
            };
            let weight = self.weights.as_ref().map(|weights| weights[&delta.id]);
            let steady = delta.steady / weight.unwrap_or(1.);
            let incoming = delta.incoming / weight.unwrap_or(1.);
            let after = (before + steady).max(0.);
            if let Some(limit) = self.deadline {
                // Incoming copies can use resources before their old copies are
                // removed, including when both groups share a replica node.
                let pending_peak = self.copy_peaks.get(&delta.id).copied().unwrap_or(before);
                let added = incoming.max(steady).max(0.);
                if added > EPSILON
                    && (!pending_peak.is_finite()
                        || pending_peak + added > pending_peak.max(limit) + EPSILON)
                {
                    return None;
                }
                effect.relief += (before - limit).max(0.) - (after - limit).max(0.);
            }
            let before_penalty = self.penalty(delta.id, before, weight);
            let improvement = before_penalty - self.penalty(delta.id, after, weight);
            effect.improvement += improvement;
            effect.previous_penalty += before_penalty;
            effect.preserves_balance &= improvement >= -EPSILON;
            let (low, high) = self.bounds(delta.id);
            effect.donates |= delta.steady < 0. && before > high + EPSILON;
            effect.receives_below_low |= delta.steady > 0. && before <= low;
        }
        if self.weights.is_some() {
            effect.preserves_balance = effect.improvement >= -EPSILON;
            effect.donates |= effect.receives_below_low;
            // Ignore improvements smaller than the noise of a single action.
            if effect.improvement < 0.01 * effect.previous_penalty {
                effect.donates = false;
            }
        }
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
    previous_penalty: f64,
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
    unknown_receivers: Vec<u64>,
}

struct Delta {
    id: u64,
    steady: f64,
    incoming: f64,
}

impl Change {
    pub fn steady_delta(&self, id: u64) -> f64 {
        self.deltas.iter().find(|delta| delta.id == id).map_or(0., |delta| delta.steady)
    }

    pub fn include_pending(&mut self, change: Change) {
        self.unknown_receivers.extend(change.unknown_receivers);
        for delta in change.deltas {
            self.add(delta.id, delta.steady, delta.incoming.max(delta.steady).max(0.));
        }
    }

    pub fn mark_unknown_receiver(&mut self, id: u64) {
        self.unknown = true;
        self.unknown_receivers.push(id);
    }

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
            self.mark_unknown_receiver(target);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_resource_relief_cannot_pay_for_an_earlier_incoming_copy() {
        let mut outgoing = Change::default();
        outgoing.transfer(1, 2, Some(20.));
        let mut pending = Change::default();
        pending.include_pending(outgoing);
        pending.mark_unknown_receiver(3);
        let disk = Axis::new(
            BalanceDimension::Disk,
            [(1, 85.), (2, 50.), (3, 50.), (4, 50.)].into(),
            4,
            5.,
            0.,
            Some(90.),
        )
        .with_pending(&pending, 5., 0.);
        assert_eq!(disk.thresholds().unwrap().max, 85.);
        assert_eq!(disk.loads[&1], 65.);

        let mut candidate = Change::default();
        candidate.transfer(4, 1, Some(6.));
        assert!(disk.effect(&candidate).is_none());
        let mut candidate = Change::default();
        candidate.transfer(4, 3, Some(1.));
        assert!(disk.effect(&candidate).is_none());
        let mut candidate = Change::default();
        candidate.transfer(4, 2, Some(20.));
        assert!(disk.effect(&candidate).is_some());
    }

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
