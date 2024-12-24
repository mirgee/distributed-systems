use std::collections::BTreeSet;

use crate::log::LogState;

pub type TermId = u64;
pub type NodeId = u64;

pub struct RaftConfig {
    pub heartbeat_interval: u32,
    pub min_election_countdown: u32,
    pub max_election_countdown: u32,
}

// TODO: Separate the state and processing layer from client-facing messaging layer, where node subsumes both
// TODO: The fields should not be all public, implement getters
pub struct RaftNode<Log, Random> {
    pub node_id: NodeId,
    pub current_term: TermId,
    pub voted_for: Option<NodeId>,
    pub peers: BTreeSet<NodeId>, // TODO: Map to a peer state
    pub log_state: LogState<Log>,
    pub role_state: RoleState,
    pub config: RaftConfig,
    pub rng: Random,
}

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
