use std::iter;

use super::{state::{CandidateState, FollowerState, LeaderState, RoleState}, RaftNode};
use rand_core::RngCore;

use crate::{log::RaftLog, messages::RaftMessageEnvelope};

impl<Log, Random> RaftNode<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub fn tick(&mut self) -> Option<RaftMessageEnvelope> {
        match &mut self.role_state {
            RoleState::LeaderState(LeaderState {
                heartbeat_countdown,
                ..
            }) => {
                *heartbeat_countdown = heartbeat_countdown.saturating_sub(1);
                if *heartbeat_countdown == 0 {
                    *heartbeat_countdown = self.config.heartbeat_interval;
                    Some(self.create_append_entries_message(iter::empty()))
                } else {
                    None
                }
            }
            RoleState::FollowerState(FollowerState {
                election_countdown, ..
            })
            | RoleState::CandidateState(CandidateState {
                election_countdown, ..
            }) => {
                *election_countdown = election_countdown.saturating_sub(1);
                if *election_countdown == 0 {
                    self.election_timeout()
                } else {
                    None
                }
            }
        }
    }

    fn election_timeout(&mut self) -> Option<RaftMessageEnvelope> {
        match self.role_state {
            RoleState::LeaderState(_) => None,
            RoleState::FollowerState(_) | RoleState::CandidateState(_) => {
                self.current_term += 1;
                self.transition_to_candidate_state(iter::once(self.node_id));
                self.maybe_become_leader();
                Some(self.create_request_vote_message())
            }
        }
    }
}
