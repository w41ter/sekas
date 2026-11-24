// Copyright 2023-present The Sekas Authors.
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

use std::collections::HashSet;

use sekas_api::server::v1::{
    GroupDesc, MoveReplicasRequest, ReplicaDesc, ReplicaReplacement, ReplicaRole,
};

use crate::{Error, Result};

pub(crate) enum ReplacementStep {
    Wait,
    Complete,
    Cancel,
    AddLearner,
    CatchUpAndReplace,
    LeaveJoint,
    RemoveLearner(u64),
}

/// Derive progress from committed membership. Root uses the terminal decisions
/// and the Group scheduler uses the same rules before acting on a request.
pub(crate) fn replacement_step(
    group: &GroupDesc,
    intent: ReplicaReplacement,
    target_active: bool,
) -> Result<ReplacementStep> {
    use ReplacementStep::*;
    let incoming = group.replicas.iter().find(|replica| replica.id == intent.incoming_id);
    let outgoing = group.replicas.iter().find(|replica| replica.id == intent.outgoing_id);
    if intent.incoming_id == intent.outgoing_id
        || incoming.is_some_and(|replica| replica.node_id != intent.target_node)
    {
        return Err(Error::InvalidArgument("invalid replacement replica".into()));
    }

    let incoming_role = incoming.map(|replica| replica.role());
    let outgoing_role = outgoing.map(|replica| replica.role());
    if group.replicas.iter().any(|replica| {
        matches!(replica.role(), ReplicaRole::IncomingVoter | ReplicaRole::DemotingVoter)
    }) {
        return Ok(
            if (incoming_role, outgoing_role)
                == (Some(ReplicaRole::IncomingVoter), Some(ReplicaRole::DemotingVoter))
            {
                LeaveJoint
            } else {
                Wait
            },
        );
    }

    let eligible = outgoing_role == Some(ReplicaRole::Voter)
        && !group.replicas.iter().any(|replica| {
            replica.id != intent.incoming_id && replica.node_id == intent.target_node
        });
    Ok(match incoming_role {
        Some(ReplicaRole::Voter) => match outgoing_role {
            None => Complete,
            Some(ReplicaRole::Learner) => RemoveLearner(intent.outgoing_id),
            _ => Cancel,
        },
        Some(ReplicaRole::Learner) if eligible && target_active => CatchUpAndReplace,
        Some(ReplicaRole::Learner) => RemoveLearner(intent.incoming_id),
        None if !eligible || group.epoch != intent.add_epoch => Cancel,
        // An earlier add request can still reach the original epoch.
        None if !target_active => Wait,
        None => AddLearner,
        _ => unreachable!("joint roles are handled above"),
    })
}

pub(crate) fn validate_move_replicas(
    current_replicas: &[ReplicaDesc],
    request: &MoveReplicasRequest,
) -> Result<(), Error> {
    let incoming_replicas = &request.incoming_voters;
    let outgoing_replicas = &request.outgoing_voters;
    if let Some(intent) = request.replacement {
        if !incoming_replicas.is_empty()
            || !outgoing_replicas.is_empty()
            || intent.incoming_id == intent.outgoing_id
        {
            return Err(Error::InvalidArgument("invalid replica replacement request".into()));
        }
        return Ok(());
    }
    if incoming_replicas.is_empty() && outgoing_replicas.is_empty() {
        return Err(Error::InvalidArgument("empty MoveReplicas request".to_owned()));
    }

    let current_ids = current_replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
    let current_nodes = current_replicas.iter().map(|r| r.node_id).collect::<HashSet<_>>();
    let outgoing_ids = outgoing_replicas.iter().map(|r| r.id).collect::<HashSet<_>>();
    let mut incoming_ids = HashSet::new();
    let mut incoming_nodes = HashSet::new();
    let mut seen_outgoing_ids = HashSet::new();

    for replica in incoming_replicas {
        if !incoming_ids.insert(replica.id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated incoming replica {}",
                replica.id
            )));
        }
        if !incoming_nodes.insert(replica.node_id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated incoming node {}",
                replica.node_id
            )));
        }
        if current_ids.contains(&replica.id) || outgoing_ids.contains(&replica.id) {
            return Err(Error::AlreadyExists(format!("replica {}", replica.id)));
        }
        if current_nodes.contains(&replica.node_id) {
            return Err(Error::AlreadyExists(format!("replica on node {}", replica.node_id)));
        }
    }

    for replica in outgoing_replicas {
        if !seen_outgoing_ids.insert(replica.id) {
            return Err(Error::InvalidArgument(format!(
                "duplicated outgoing replica {}",
                replica.id
            )));
        }
        if !current_ids.contains(&replica.id) {
            return Err(Error::InvalidArgument(format!("replica {} not found", replica.id)));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waits_until_late_learner_admission_is_fenced() {
        let intent =
            ReplicaReplacement { incoming_id: 12, outgoing_id: 11, target_node: 2, add_epoch: 5 };
        let mut group = GroupDesc {
            epoch: intent.add_epoch,
            replicas: vec![ReplicaDesc { id: 11, node_id: 1, role: ReplicaRole::Voter.into() }],
            ..Default::default()
        };
        assert!(matches!(replacement_step(&group, intent, true), Ok(ReplacementStep::AddLearner)));
        assert!(matches!(replacement_step(&group, intent, false), Ok(ReplacementStep::Wait)));

        group.epoch += 1;
        assert!(matches!(replacement_step(&group, intent, false), Ok(ReplacementStep::Cancel)));
        group.replicas.push(ReplicaDesc { id: 12, node_id: 2, role: ReplicaRole::Learner.into() });
        assert!(matches!(
            replacement_step(&group, intent, false),
            Ok(ReplacementStep::RemoveLearner(12))
        ));
    }
}
