mod utils;

use raft::{
    log::in_memory::RaftLogInMemory,
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope,
        append_entries::AppendEntries,
        request_vote::{RequestVote, RequestVoteResponse},
        rpc::Rpc,
    },
    node::{NodeId, RaftConfig, RaftNode, RoleState},
};
use utils::{
    messages::{append_entries, vote_request, vote_response},
    test_cluster::TestRaftCluster,
};

#[test]
fn candidate_wins_election_as_single_node() {
    let mut cluster = TestRaftCluster::new(1);
    assert!(!cluster.node(0).is_leader());

    cluster.run_until(10, |c| c.node(0).is_leader());

    assert!(cluster.node(0).is_leader());
}

#[test]
fn candidate_wins_election_by_majority_vote() {
    let mut cluster = TestRaftCluster::new(4);
    assert!(!cluster.has_leader());

    cluster.run_until(10, |c| {
        let node_state = &c.node(0).role_state();
        matches!(node_state, RoleState::CandidateState(_))
    });

    let term = cluster.node(0).current_term();
    let vote_granted_msg_1 = vote_response(1.into(), 0.into(), term, true);
    let vote_granted_msg_2 = vote_response(2.into(), 0.into(), term, true);

    cluster.send_message(0, vote_granted_msg_1);
    cluster.send_message(0, vote_granted_msg_2);

    cluster.run_until(5, |c| c.node(0).is_leader());

    assert!(cluster.node(0).is_leader());
}

#[test]
pub fn candidate_ignores_duplicate_votes() {
    let mut cluster = TestRaftCluster::new(4);
    assert!(!cluster.has_leader());

    cluster.run_until(10, |c| {
        matches!(c.node(0).role_state(), RoleState::CandidateState(_))
    });

    let term = cluster.node(0).current_term();

    let vote_response = vote_response(1.into(), 0.into(), term, true);

    cluster.send_message(0, vote_response.clone());
    cluster.send_message(0, vote_response.clone());
    cluster.send_message(0, vote_response);

    cluster.run(5);
    assert!(!cluster.node(0).is_leader());
}

#[test]
pub fn candidate_ignores_outdated_votes() {
    let mut cluster = TestRaftCluster::new(4);
    cluster.run_until(10, |c| {
        matches!(c.node(0).role_state(), RoleState::CandidateState(_))
    });

    let term1 = cluster.node(0).current_term();
    cluster.run_until(10, |c| c.node(0).current_term() > term1);

    let outdated_votes = vec![
        vote_response(1.into(), 0.into(), term1, true),
        vote_response(2.into(), 0.into(), term1, true),
        vote_response(3.into(), 0.into(), term1, true),
    ];

    for vote in outdated_votes {
        cluster.send_message(0, vote);
    }

    assert!(!cluster.node(0).is_leader());
}

#[test]
pub fn follower_resets_election_timer_on_append_entries() {
    let mut cluster = TestRaftCluster::new(2);
    cluster.run(1);

    let append_msg = append_entries(
        1.into(),
        0.into(),
        cluster.node(0).current_term(),
        Vec::new(),
    );
    cluster.send_message(0, append_msg);

    cluster.run(10);
    assert!(!cluster.node(0).is_leader());
}

#[test]
pub fn candidate_steps_down_on_higher_term() {
    let mut cluster = TestRaftCluster::new(2);
    cluster.run_until(10, |c| {
        matches!(c.node(0).role_state(), RoleState::CandidateState(_))
    });

    let higher_term_msg = append_entries(
        1.into(),
        0.into(),
        cluster.node(0).current_term() + 1,
        Vec::new(),
    );
    cluster.send_message(0, higher_term_msg);

    cluster.run(5);
    assert!(!cluster.node(0).is_leader());
    assert!(matches!(
        cluster.node(0).role_state(),
        RoleState::FollowerState(_)
    ));
}

#[test]
pub fn candidate_fails_to_win_due_to_lack_of_majority() {
    let mut cluster = TestRaftCluster::new(4);
    cluster.run_until(10, |c| {
        matches!(c.node(0).role_state(), RoleState::CandidateState(_))
    });

    let vote = vote_response(1.into(), 0.into(), cluster.node(0).current_term(), true);
    cluster.send_message(0, vote);

    cluster.run(10);
    assert!(matches!(
        cluster.node(0).role_state(),
        RoleState::CandidateState(_)
    ));
    assert!(cluster.node(0).current_term() > 0.into());
}

#[test]
pub fn candidate_rejects_second_request_vote_in_same_term() {
    let mut cluster = TestRaftCluster::new(2);
    cluster.run_until(10, |c| {
        matches!(c.node(0).role_state(), RoleState::CandidateState(_))
    });

    let request_vote_msg = vote_request(1.into(), 0.into(), cluster.node(0).current_term());
    cluster.send_message(0, request_vote_msg);

    assert!(matches!(
        cluster.node(0).role_state(),
        RoleState::CandidateState(_)
    ));
    assert!(cluster.node(0).voted_for().is_none());
}

#[test]
pub fn follower_rejects_second_vote_in_same_term() {
    let mut cluster = TestRaftCluster::new(3);

    let first_vote = vote_request(1.into(), 0.into(), cluster.node(0).current_term());
    let second_vote = vote_request(2.into(), 0.into(), cluster.node(0).current_term());

    cluster.send_message(0, first_vote);
    cluster.send_message(0, second_vote);

    assert_eq!(cluster.node(0).voted_for().unwrap(), 1.into());
}
