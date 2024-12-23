use super::state::{FollowerState, NodeId, RoleState, TermId};
use rand_core::RngCore;

use crate::{
    log::RaftLog,
    messages::{
        RaftMessage,
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
    pub(super) fn handle_request_vote(
        &mut self,
        request_vote: RequestVote,
        from: NodeId,
    ) -> Option<RaftMessage> {
        let vote_granted = (request_vote.last_log_term > self.current_term)
            || (request_vote.last_log_term == self.current_term
                && request_vote.last_log_index >= self.log_state.log.get_last_index().unwrap());
        // TODO: Can we vote for a leader if their term is higher than our current term?
        if vote_granted {
            // TODO: Make sure we reset our timer after granting a vote
            self.voted_for = Some(from);
        }

        Some(RaftMessage {
            term: self.current_term,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted }),
        })
    }

    pub(super) fn handle_request_vote_response(
        &mut self,
        request_vote_response: RequestVoteResponse,
        from: NodeId,
    ) -> Option<RaftMessage> {
        if let RoleState::CandidateState(candidate_state) = &mut self.role_state {
            if request_vote_response.vote_granted {
                candidate_state.votes_granted.insert(from);
            }
        }
        None
    }

    pub(super) fn handle_append_entries(
        &mut self,
        append_entries: AppendEntries,
        from: NodeId,
        term: TermId,
    ) -> Option<RaftMessage> {
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
            Some(RaftMessage {
                term,
                rpc: Rpc::AppendEntriesResponse(AppendEntriesResponse { success: true }),
            })
        } else {
            Some(RaftMessage {
                term,
                rpc: Rpc::AppendEntriesResponse(AppendEntriesResponse { success: false }),
            })
        }
    }

    pub(super) fn handle_append_entries_response(
        &mut self,
        append_entries_response: AppendEntriesResponse,
    ) -> Option<RaftMessage> {
        // TODO: If a leader receives rejection on heartbeat rejection, shouldn't they step down? I
        // think so, but can't find that mentioned in the paper.
        None
    }
}
