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
pub struct RaftNode<Log, Random> {
    pub(super) node_id: NodeId,
    pub(super) current_term: TermId,
    pub(super) voted_for: Option<NodeId>,
    pub(super) peers: BTreeSet<NodeId>, // TODO: Map to a peer state
    pub(super) log_state: LogState<Log>,
    pub(super) role_state: RoleState,
    pub(super) config: RaftConfig,
    pub(super) rng: Random,
}

#[derive(Default)]
pub struct FollowerState {
    pub(super) leader: Option<NodeId>,
    pub(super) election_countdown: u32,
    pub(super) election_countdown_starting_point: u32,
}

#[derive(Default)]
pub struct CandidateState {
    pub(super) votes_granted: BTreeSet<NodeId>,
    pub(super) election_countdown: u32,
    pub(super) election_countdown_starting_point: u32,
}

#[derive(Default)]
pub struct LeaderState {
    pub(super) heartbeat_countdown: u32,
}

pub enum RoleState {
    LeaderState(LeaderState),
    FollowerState(FollowerState),
    CandidateState(CandidateState),
}
