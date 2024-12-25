use std::collections::BTreeSet;

use rand_core::RngCore;

use crate::{
    log::RaftLog,
    messages::RaftMessageEnvelope,
    node::state::{NodeId, RaftNode},
};

use super::RaftRole;

pub struct Candidate {
    votes_granted: BTreeSet<NodeId>,
    election_countdown: u32,
    election_countdown_starting_point: u32,
}

impl<Log, Random> RaftRole<Log, Random> for Candidate
where
    Log: RaftLog,
    Random: RngCore,
{
    fn tick(&mut self, raft: &mut RaftNode<Log, Random>) -> Option<RaftMessageEnvelope> {
        self.election_countdown = self.election_countdown.saturating_sub(1);
        if self.election_countdown == 0 {
            raft.election_timeout()
        } else {
            None
        }
    }

    fn handle_message(
        &mut self,
        raft: &mut RaftNode<Log, Random>,
        message: RaftMessageEnvelope,
    ) -> Option<RaftMessageEnvelope> {
        todo!()
    }
}
