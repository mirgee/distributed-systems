use std::collections::BTreeSet;

use super::identifiers::NodeId;

#[derive(Default, Debug)]
pub struct FollowerState {
    pub(super) leader: Option<NodeId>,
    pub(super) election_countdown: u32,
    pub(super) election_countdown_starting_point: u32,
}

#[derive(Default, Debug)]
pub struct CandidateState {
    pub(super) votes_granted: BTreeSet<NodeId>,
    pub(super) election_countdown: u32,
    pub(super) election_countdown_starting_point: u32,
}

#[derive(Default, Debug)]
pub struct LeaderState {
    pub(super) heartbeat_countdown: u32,
}

#[derive(Debug)]
pub enum RoleState {
    LeaderState(LeaderState),
    FollowerState(FollowerState),
    CandidateState(CandidateState),
}
