use super::{
    RaftNode,
    identifiers::NodeId,
    state::{CandidateState, FollowerState, LeaderState, RoleState},
};
use rand_core::RngCore;

use crate::log::RaftLog;

impl<Log, Random> RaftNode<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub(super) fn transition_to_candidate_state(
        &mut self,
        votes_granted: impl IntoIterator<Item = NodeId>,
    ) {
        let election_countdown = self.random_election_countdown();
        self.role_state = RoleState::CandidateState(CandidateState {
            votes_granted: votes_granted.into_iter().collect(),
            election_countdown,
            election_countdown_starting_point: election_countdown,
        });
    }

    pub(super) fn transition_to_leader_state(&mut self) {
        self.role_state = RoleState::LeaderState(LeaderState {
            heartbeat_countdown: self.config.heartbeat_interval,
        })
    }

    pub(super) fn transition_to_follower_state(&mut self, leader: Option<NodeId>) {
        let election_countdown = self.random_election_countdown();
        self.role_state = RoleState::FollowerState(FollowerState {
            // TODO: Can we assume the sender is the leader? Probably not, because this may be
            // just vote request
            leader,
            // TODO: Do we create new random countdown or reset the previous one?
            // Does it depend on context?
            election_countdown,
            election_countdown_starting_point: election_countdown,
        });
    }
}
