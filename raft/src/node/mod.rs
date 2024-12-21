pub mod state;
mod utils;

use std::collections::BTreeSet;

use rand::RngCore;
use state::{CandidateState, FollowerState, LeaderState, NodeId, RoleState, TermId};
use utils::random_election_countdown;

use crate::{
    log::{LogState, RaftLog},
    messages::{
        RaftMessage,
        request_vote::{RequestVote, RequestVoteResponse},
        rpc::Rpc,
    },
};

pub struct Config {
    heartbeat_interval: u32,
    min_election_countdown: u32,
    max_election_countdown: u32,
}

pub struct Node<Log, Random> {
    node_id: NodeId,
    current_term: TermId,
    voted_for: Option<NodeId>,
    peers: BTreeSet<NodeId>, // TODO: Map to a peer state
    log_state: LogState<Log>,
    role_state: RoleState,
    config: Config,
    rng: Random,
}

impl<Log, Random> Node<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub fn new(
        node_id: NodeId,
        peers: BTreeSet<NodeId>,
        log: Log,
        config: Config,
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
            }),
            config,
        }
    }

    // TODO: Adapt to one node setting
    fn election_timeout(&mut self) -> Option<RaftMessage> {
        match self.role_state {
            RoleState::LeaderState(_) => None,
            RoleState::FollowerState(_) | RoleState::CandidateState(_) => {
                self.current_term += 1;
                let votes_granted = {
                    let mut vg = BTreeSet::new();
                    vg.insert(self.node_id);
                    vg
                };
                self.role_state = RoleState::CandidateState(CandidateState {
                    votes_granted,
                    election_countdown: self.random_election_countdown(),
                });
                Some(RaftMessage {
                    term: self.current_term,
                    rpc: Rpc::RequestVote(RequestVote {
                        last_log_index: self.log_state.log.get_last_term().unwrap(),
                        last_log_term: self.log_state.log.get_last_term().unwrap(),
                    }),
                })
            }
        }
    }

    pub fn tick(&mut self) -> Option<RaftMessage> {
        match &mut self.role_state {
            RoleState::LeaderState(LeaderState {
                heartbeat_countdown: ticks_to_heartbeat, ..
            }) => {
                *ticks_to_heartbeat = ticks_to_heartbeat.saturating_sub(1);
                if *ticks_to_heartbeat == 0 {
                    *ticks_to_heartbeat = self.config.heartbeat_interval;
                    Some(RaftMessage {
                        term: self.current_term,
                        rpc: Rpc::AppendEntries,
                    })
                } else {
                    None
                }
            }
            RoleState::FollowerState(FollowerState {
                election_countdown: ticks_to_election,
                ..
            })
            // TODO: Count our votes and potentially convert to a leader?
            | RoleState::CandidateState(CandidateState {
                election_countdown: ticks_to_election,
                ..
            }) => {
                *ticks_to_election = ticks_to_election.saturating_sub(1);
                if *ticks_to_election == 0 {
                    self.election_timeout()
                } else {
                    None
                }
            }
        }
    }

    // TODO: We should send and respond with message envelope
    fn receive_message(&mut self, message: RaftMessage, from: NodeId) -> Option<RaftMessage> {
        if message.term > self.current_term {
            self.current_term = message.term;
            self.role_state = RoleState::FollowerState(FollowerState {
                // TODO: Can we assume the sender is the leader
                leader: Some(from),
                // TODO: Do we create new random countdown or reset the previous one?
                // Does it depend on context?
                election_countdown: self.random_election_countdown(),
            });
        }

        let response = match message.rpc {
            Rpc::RequestVote(request_vote) => self.handle_request_vote(request_vote, from),
            Rpc::RequestVoteResponse(request_vote_response) => {
                self.handle_request_vote_response(request_vote_response)
            }
            Rpc::AppendEntries | Rpc::AppendEntriesResponse => todo!(),
        };

        if let RoleState::CandidateState(candidate_state) = &self.role_state {
            if candidate_state.votes_granted.len() >= self.majority_size() {
                self.role_state = RoleState::LeaderState(LeaderState {
                    heartbeat_countdown: self.config.heartbeat_interval,
                })
            }
        }

        response
    }

    fn handle_request_vote(&mut self, request_vote: RequestVote, from: NodeId) -> Option<RaftMessage> {
        let vote_granted = (request_vote.last_log_term > self.current_term)
            || (request_vote.last_log_term == self.current_term
                && request_vote.last_log_index >= self.log_state.log.get_last_index().unwrap());
        // TODO: Can we vote for a leader if their term is higher than our current term?
        if vote_granted {
            self.voted_for = Some(from);
        }

        Some(RaftMessage {
            term: self.current_term,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse {
                vote_granted 
            })
        })
    }

    fn handle_request_vote_response(
        &mut self,
        request_vote_response: RequestVoteResponse,
    ) -> Option<RaftMessage> {
        todo!()
    }

    fn random_election_countdown(&mut self) -> u32 {
        random_election_countdown(
            &mut self.rng,
            self.config.min_election_countdown,
            self.config.max_election_countdown,
        )
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role_state, RoleState::LeaderState(_))
    }

    fn majority_size(&self) -> usize {
        self.peers.len() / 2 + 1
    }
}
