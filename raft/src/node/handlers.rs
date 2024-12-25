use super::{identifiers::{NodeId, TermId}, state::{FollowerState, RoleState}};
use rand_core::RngCore;

use crate::{
    log::RaftLog,
    messages::{
        RaftMessageEnvelope,
        append_entries::{AppendEntries, AppendEntriesResponse},
        request_vote::{RequestVote, RequestVoteResponse},
        rpc::Rpc,
    },
};

use super::RaftNode;

impl<Log, Random> RaftNode<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub fn receive_message(&mut self, message: RaftMessageEnvelope) -> Option<RaftMessageEnvelope> {
        // TODO: We should be resetting the timer only when we receive append entries!
        self.update_term(&message);

        let response = match message.msg.rpc {
            Rpc::RequestVote(request_vote) => self.handle_request_vote(request_vote, message.from),
            Rpc::RequestVoteResponse(request_vote_response)
                if message.msg.term >= self.current_term() =>
            {
                self.handle_request_vote_response(request_vote_response, message.from)
            }
            Rpc::AppendEntries(append_entries) => {
                self.handle_append_entries(append_entries, message.from, message.msg.term)
            }
            Rpc::AppendEntriesResponse(append_entries_response)
                if message.msg.term >= self.current_term() =>
            {
                self.handle_append_entries_response(append_entries_response)
            }
            _ => None,
        };

        self.maybe_become_leader();
        response
    }

    fn handle_request_vote(
        &mut self,
        request_vote: RequestVote,
        from: NodeId,
    ) -> Option<RaftMessageEnvelope> {
        let vote_granted = (request_vote.last_log_term > self.current_term)
            || (request_vote.last_log_term == self.current_term
                && request_vote.last_log_index
                    >= self.log_state.log.get_last_index().unwrap().unwrap_or(0))
                && self.voted_for.map(|vote| vote == from).unwrap_or(true);
        // TODO: Can we vote for a leader if their term is higher than our current term?
        if vote_granted {
            // TODO: Make sure we reset our timer after granting a vote
            // TODO: Make sure is being reset appropriately
            self.voted_for = Some(from);
        }

        Some(self.create_request_vote_response_message(vote_granted))
    }

    fn handle_request_vote_response(
        &mut self,
        request_vote_response: RequestVoteResponse,
        from: NodeId,
    ) -> Option<RaftMessageEnvelope> {
        if let RoleState::CandidateState(candidate_state) = &mut self.role_state {
            if request_vote_response.vote_granted {
                candidate_state.votes_granted.insert(from);
            }
        }
        None
    }

    fn handle_append_entries(
        &mut self,
        append_entries: AppendEntries,
        from: NodeId,
        term: TermId,
    ) -> Option<RaftMessageEnvelope> {
        if term >= self.current_term {
            match &mut self.role_state {
                RoleState::LeaderState(leader_state) => unreachable!("shouldn't happen"),
                RoleState::FollowerState(follower_state) => {
                    follower_state.leader = Some(from);
                    follower_state.election_countdown =
                        follower_state.election_countdown_starting_point;
                }
                RoleState::CandidateState(_) => {
                    let election_countdown = self.random_election_countdown();
                    self.role_state = RoleState::FollowerState(FollowerState {
                        leader: Some(from),
                        election_countdown,
                        election_countdown_starting_point: election_countdown,
                    });
                }
            }
            Some(self.create_append_entries_response_message(true, from))
        } else {
            Some(self.create_append_entries_response_message(false, from))
        }
    }

    fn handle_append_entries_response(
        &mut self,
        append_entries_response: AppendEntriesResponse,
    ) -> Option<RaftMessageEnvelope> {
        // TODO: If a leader receives rejection on heartbeat rejection, shouldn't they step down? I
        // think so, but can't find that mentioned in the paper.
        None
    }

    pub fn update_term(&mut self, message: &RaftMessageEnvelope) {
        if message.msg.term > self.current_term {
            self.current_term = message.msg.term;
            self.transition_to_follower_state(Some(message.from));
        }
    }
}
