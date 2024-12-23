use std::collections::BTreeSet;

use raft::{
    log::in_memory::RaftLogInMemory,
    messages::{RaftMessage, request_vote::RequestVoteResponse, rpc::Rpc},
    node::{RaftConfig, RaftNode},
};
use rand_core::OsRng;

#[test]
pub fn win_election_single_node() {
    let config = RaftConfig {
        heartbeat_interval: 1,
        min_election_countdown: 2,
        max_election_countdown: 3,
    };
    let log = RaftLogInMemory::new();
    let rng = OsRng;
    let mut node = RaftNode::new(0, BTreeSet::new(), log, config, rng);
    let mut is_leader = node.is_leader();
    assert!(!is_leader);
    while !is_leader {
        node.tick();
        is_leader = node.is_leader();
    }
}

#[test]
pub fn win_election_majority_vote() {
    let config = RaftConfig {
        heartbeat_interval: 1,
        min_election_countdown: 2,
        max_election_countdown: 3,
    };
    let log = RaftLogInMemory::new();
    let rng = OsRng;
    let mut node = RaftNode::new(0, BTreeSet::from_iter(vec![1, 2]), log, config, rng);
    assert!(!node.is_leader());
    let mut msg = None;
    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().rpc, Rpc::RequestVote(_)));
    let response = RaftMessage {
        term: node.current_term(),
        rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
    };
    node.receive_message(response.clone(), 1);
    node.receive_message(response, 2);
    assert!(node.is_leader());
}

// TODO: Duplicated or outdated messages
// TODO: Convert from candidate to follower
