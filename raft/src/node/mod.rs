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
    peers: BTreeSet<NodeId>,
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
                ticks_to_heartbeat, ..
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

    fn receive_message(&mut self, message: RaftMessage, from: NodeId) -> Option<RaftMessage> {
        self.update_term(&message, from);

        match message.rpc {
            Rpc::RequestVote(request_vote) => self.handle_request_vote(request_vote),
            Rpc::RequestVoteResponse(request_vote_response) => {
                self.handle_request_vote_response(request_vote_response)
            }
            Rpc::AppendEntries | Rpc::AppendEntriesResponse => todo!(),
        }
    }

    fn handle_request_vote(&mut self, request_vote: RequestVote) -> Option<RaftMessage> {
        todo!()
    }

    fn handle_request_vote_response(
        &mut self,
        request_vote_response: RequestVoteResponse,
    ) -> Option<RaftMessage> {
        todo!()
    }

    fn update_term(&mut self, message: &RaftMessage, from: NodeId) {
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
    }

    fn random_election_countdown(&mut self) -> u32 {
        random_election_countdown(
            &mut self.rng,
            self.config.min_election_countdown,
            self.config.max_election_countdown,
        )
    }
}
