use std::collections::BTreeSet;

pub type TermId = u64;
pub type NodeId = u64;

#[derive(Default)]
pub struct FollowerState {
    pub leader: Option<NodeId>,
    pub election_countdown: u32,
    pub election_countdown_starting_point: u32,
}

#[derive(Default)]
pub struct CandidateState {
    pub votes_granted: BTreeSet<NodeId>,
    pub election_countdown: u32,
    pub election_countdown_starting_point: u32,
}

#[derive(Default)]
pub struct LeaderState {
    pub heartbeat_countdown: u32,
}

pub enum RoleState {
    LeaderState(LeaderState),
    FollowerState(FollowerState),
    CandidateState(CandidateState),
}
