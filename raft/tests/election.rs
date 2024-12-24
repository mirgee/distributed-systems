use std::collections::BTreeSet;

use raft::{
    log::in_memory::RaftLogInMemory,
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope,
        append_entries::AppendEntries,
        request_vote::{RequestVote, RequestVoteResponse},
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
pub fn candidate_wins_election_as_single_node() {
    let mut node = create_node(vec![]);
    let is_leader = node.is_leader();
    assert!(!is_leader);
    for _ in 0..=node.config().max_election_countdown {
        node.tick();
    }
    assert!(node.is_leader());
}

#[test]
pub fn candidate_wins_election_by_majority_vote() {
    let mut node = create_node(vec![1, 2, 3]);
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
    assert!(!node.is_leader());
    node.receive_message(response2);
    assert!(node.is_leader());
}

#[test]
pub fn candidate_ignores_duplicate_votes() {
    let mut node = create_node(vec![1, 2, 3]);
    assert!(!node.is_leader());
    let mut msg = None;
    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));
    let response = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term(),
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(response.clone());
    assert!(!node.is_leader());
    node.receive_message(response.clone());
    assert!(!node.is_leader());
    node.receive_message(response);
    assert!(!node.is_leader());
}

#[test]
pub fn candidate_ignores_outdated_votes() {
    let mut node = create_node(vec![1, 2, 3]);
    assert!(!node.is_leader());
    let term1 = node.current_term();
    let mut msg = None;
    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));
    let mut msg = None;
    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));
    let term2 = node.current_term();
    assert!(term2 > term1);
    let response1 = RaftMessageEnvelope {
        msg: RaftMessage {
            term: term1,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    let response2 = RaftMessageEnvelope {
        msg: RaftMessage {
            term: term1,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
        },
        src: 2,
        dst: MessageDestination::To(0),
    };
    let response3 = RaftMessageEnvelope {
        msg: RaftMessage {
            term: term1,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
        },
        src: 3,
        dst: MessageDestination::To(0),
    };
    node.receive_message(response1);
    node.receive_message(response2);
    node.receive_message(response3);
    assert!(!node.is_leader());
}

#[test]
pub fn follower_resets_election_timer_on_append_entries() {
    let mut node = create_node(vec![1]);
    assert!(!node.is_leader());

    for _ in 0..node.config().min_election_countdown - 1 {
        node.tick();
    }
    let msg = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term(),
            rpc: Rpc::AppendEntries(AppendEntries {
                entries: Vec::new(),
            }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(msg);

    for _ in 0..node.config().min_election_countdown {
        node.tick();
    }

    assert!(!node.is_leader());
}

#[test]
pub fn candidate_steps_down_on_higher_term() {
    let mut node = create_node(vec![1]);
    let mut msg = None;

    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));

    let higher_term_msg = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term() + 1,
            rpc: Rpc::AppendEntries(AppendEntries {
                entries: Vec::new(),
            }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(higher_term_msg);

    assert!(!node.is_leader());
    assert!(matches!(
        node.role_state,
        raft::node::state::RoleState::FollowerState(_)
    ));
}

#[test]
pub fn candidate_fails_to_win_due_to_lack_of_majority() {
    let mut node = create_node(vec![1, 2, 3]);
    let mut msg = None;

    while msg.is_none() {
        msg = node.tick();
    }
    let term = node.current_term();
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));

    let response = RaftMessageEnvelope {
        msg: RaftMessage {
            term,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted: true }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(response);

    for _ in 0..node.config().max_election_countdown {
        node.tick();
    }

    assert!(matches!(
        node.role_state,
        raft::node::state::RoleState::CandidateState(_)
    ));
    assert!(node.current_term() > term);
}

#[test]
pub fn candidate_rejects_second_request_vote_in_same_term() {
    let mut node = create_node(vec![1]);
    let mut msg = None;

    while msg.is_none() {
        msg = node.tick();
    }
    assert!(matches!(msg.unwrap().msg.rpc, Rpc::RequestVote(_)));

    let vote_request = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term(),
            rpc: Rpc::RequestVote(RequestVote {
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(vote_request);

    assert!(matches!(
        node.role_state,
        raft::node::state::RoleState::CandidateState(_)
    ));
    assert!(node.voted_for.is_none());
}

#[test]
pub fn follower_rejects_second_vote_in_same_term() {
    let mut node = create_node(vec![1, 2]);
    assert!(!node.is_leader());

    let vote_request1 = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term(),
            rpc: Rpc::RequestVote(RequestVote {
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        src: 1,
        dst: MessageDestination::To(0),
    };
    node.receive_message(vote_request1);

    let vote_request2 = RaftMessageEnvelope {
        msg: RaftMessage {
            term: node.current_term(),
            rpc: Rpc::RequestVote(RequestVote {
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        src: 2,
        dst: MessageDestination::To(0),
    };
    node.receive_message(vote_request2);

    assert_eq!(node.voted_for.unwrap(), 1);
}
