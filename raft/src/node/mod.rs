mod handlers;
mod identifiers;
mod messages;
mod state;
mod tick;
mod transitions;

use std::collections::BTreeSet;

pub use identifiers::{NodeId, TermId};
use rand_core::RngCore;
pub use state::{CandidateState, FollowerState, LeaderState, RoleState};

use crate::{
    log::{LogState, RaftLog},
    utils::random_election_countdown,
};

// TODO: Separate the state and processing layer from client-facing messaging layer, where node subsumes both
// TODO: The fields should not be all public, implement getters
pub struct RaftNode<Log, Random> {
    node_id: NodeId,
    current_term: TermId,
    voted_for: Option<NodeId>,
    peers: BTreeSet<NodeId>, // TODO: Map to a peer state
    log_state: LogState<Log>,
    role_state: RoleState,
    config: RaftConfig,
    rng: Random,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub heartbeat_interval: u32,
    pub min_election_countdown: u32,
    pub max_election_countdown: u32,
}

impl<Log, Random> RaftNode<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub fn new(
        node_id: NodeId,
        peers: BTreeSet<NodeId>,
        log: Log,
        config: RaftConfig,
        mut rng: Random,
    ) -> Self {
        let election_countdown = random_election_countdown(
            &mut rng,
            config.min_election_countdown,
            config.max_election_countdown,
        );
        Self {
            node_id,
            peers,
            log_state: LogState::new(log),
            rng,
            current_term: Default::default(),
            voted_for: Default::default(),
            role_state: RoleState::FollowerState(FollowerState {
                leader: None,
                election_countdown,
                election_countdown_starting_point: election_countdown,
            }),
            config,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role_state, RoleState::LeaderState(_))
    }

    pub fn current_term(&self) -> TermId {
        self.current_term
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }

    pub fn role_state(&self) -> &RoleState {
        &self.role_state
    }

    pub fn voted_for(&self) -> &Option<NodeId> {
        &self.voted_for
    }

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn random_election_countdown(&mut self) -> u32 {
        random_election_countdown(
            &mut self.rng,
            self.config.min_election_countdown,
            self.config.max_election_countdown,
        )
    }

    fn maybe_become_leader(&mut self) {
        if let RoleState::CandidateState(candidate_state) = &self.role_state {
            if candidate_state.votes_granted.len() >= self.quorum() {
                self.transition_to_leader_state();
            }
        }
    }
}
