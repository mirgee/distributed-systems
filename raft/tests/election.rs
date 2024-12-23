use std::collections::BTreeSet;

use raft::{
    log::in_memory::RaftLogInMemory,
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope, request_vote::RequestVoteResponse,
        rpc::Rpc,
    },
    node::state::{NodeId, RaftConfig, RaftNode},
};
use rand_core::OsRng;

fn create_node(peers: Vec<NodeId>) -> RaftNode<RaftLogInMemory, OsRng> {
    let config = RaftConfig {
        heartbeat_interval: 1,
        min_election_countdown: 2,
        max_election_countdown: 3,
    };
    RaftNode::new(
        0,
        BTreeSet::from_iter(peers),
        RaftLogInMemory::new(),
        config,
        OsRng,
    )
}

#[test]
pub fn win_election_single_node() {
    let mut node = create_node(vec![]);
    let is_leader = node.is_leader();
    assert!(!is_leader);
    for _ in 0..=node.config().max_election_countdown {
        node.tick();
    }
    assert!(node.is_leader());
}

#[test]
pub fn win_election_majority_vote() {
    let mut node = create_node(vec![1, 2]);
    assert!(!node.is_leader());
    let mut msg = None;
    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));
    let response = RaftMessage {
        term: node.current_term(),
        rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
    };
    let response1 = RaftMessageEnvelope {
        msg: response.clone(),
        src: 1,
        dst: MessageDestination::To(0),
    };
    let response2 = RaftMessageEnvelope {
        msg: response,
        src: 2,
        dst: MessageDestination::To(0),
    };
    node.receive_message(response1);
    node.receive_message(response2);
    assert!(node.is_leader());
}

// TODO: Duplicated or outdated messages
// TODO: Convert from candidate to follower
