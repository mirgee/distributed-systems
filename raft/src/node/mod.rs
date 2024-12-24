mod handlers;
pub mod state;

use std::collections::BTreeSet;

use rand_core::RngCore;
use state::{
    CandidateState, FollowerState, LeaderState, NodeId, RaftConfig, RaftNode, RoleState, TermId,
};

use crate::{
    log::{LogState, RaftLog},
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope, append_entries::AppendEntries,
        request_vote::RequestVote, rpc::Rpc,
    },
    utils::random_election_countdown,
};

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

    pub fn tick(&mut self) -> Option<RaftMessageEnvelope> {
        match &mut self.role_state {
            RoleState::LeaderState(LeaderState {
                heartbeat_countdown: ticks_to_heartbeat,
                ..
            }) => {
                *ticks_to_heartbeat = ticks_to_heartbeat.saturating_sub(1);
                if *ticks_to_heartbeat == 0 {
                    *ticks_to_heartbeat = self.config.heartbeat_interval;
                    Some(RaftMessageEnvelope {
                        msg: RaftMessage {
                            term: self.current_term,
                            rpc: Rpc::AppendEntries(AppendEntries {
                                entries: Vec::new(),
                            }),
                        },
                        src: self.node_id,
                        dst: MessageDestination::Broadcast,
                    })
                } else {
                    None
                }
            }
            RoleState::FollowerState(FollowerState {
                election_countdown: ticks_to_election,
                ..
            })
            | RoleState::CandidateState(CandidateState {
                election_countdown: ticks_to_election,
                ..
            }) => {
                *ticks_to_election = ticks_to_election.saturating_sub(1);
                if *ticks_to_election == 0 {
                    self.election_timeout()
                } else {
                    self.become_leader();
                    None
                }
            }
        }
    }

    pub fn receive_message(&mut self, message: RaftMessageEnvelope) -> Option<RaftMessageEnvelope> {
        // TODO: We should be resetting the timer only when we receive append entries!
        self.update_term(&message);

        let response = match message.msg.rpc {
            Rpc::RequestVote(request_vote) => self.handle_request_vote(request_vote, message.src),
            Rpc::RequestVoteResponse(request_vote_response)
                if message.msg.term >= self.current_term() =>
            {
                self.handle_request_vote_response(request_vote_response, message.src)
            }
            Rpc::AppendEntries(append_entries) => {
                self.handle_append_entries(append_entries, message.src, message.msg.term)
            }
            Rpc::AppendEntriesResponse(append_entries_response)
                if message.msg.term >= self.current_term() =>
            {
                self.handle_append_entries_response(append_entries_response)
            }
            _ => None,
        };

        self.become_leader();
        response
    }

    fn become_leader(&mut self) {
        if let RoleState::CandidateState(candidate_state) = &self.role_state {
            if candidate_state.votes_granted.len() >= self.majority_size() {
                self.role_state = RoleState::LeaderState(LeaderState {
                    heartbeat_countdown: self.config.heartbeat_interval,
                })
            }
        }
    }

    fn update_term(&mut self, message: &RaftMessageEnvelope) {
        if message.msg.term > self.current_term {
            self.current_term = message.msg.term;
            let election_countdown = self.random_election_countdown();
            self.role_state = RoleState::FollowerState(FollowerState {
                // TODO: Can we assume the sender is the leader? Probably not, because this may be
                // just vote request
                leader: Some(message.src),
                // TODO: Do we create new random countdown or reset the previous one?
                // Does it depend on context?
                election_countdown,
                election_countdown_starting_point: election_countdown,
            });
        }
    }

    // TODO: Adapt to one node setting
    fn election_timeout(&mut self) -> Option<RaftMessageEnvelope> {
        match self.role_state {
            RoleState::LeaderState(_) => None,
            RoleState::FollowerState(_) | RoleState::CandidateState(_) => {
                self.current_term += 1;
                let votes_granted = {
                    let mut vg = BTreeSet::new();
                    vg.insert(self.node_id);
                    vg
                };
                let election_countdown = self.random_election_countdown();
                self.role_state = RoleState::CandidateState(CandidateState {
                    votes_granted,
                    election_countdown,
                    election_countdown_starting_point: election_countdown,
                });
                Some(RaftMessageEnvelope {
                    msg: RaftMessage {
                        term: self.current_term,
                        rpc: Rpc::RequestVote(RequestVote {
                            last_log_index: self
                                .log_state
                                .log
                                .get_last_term()
                                .unwrap()
                                .unwrap_or(0),
                            last_log_term: self.log_state.log.get_last_term().unwrap().unwrap_or(0),
                        }),
                    },
                    src: self.node_id,
                    dst: MessageDestination::Broadcast,
                })
            }
        }
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
        (self.peers.len() + 1) / 2 + 1
    }

    pub fn current_term(&self) -> TermId {
        self.current_term
    }

    pub fn config(&self) -> &RaftConfig {
        &self.config
    }
}
