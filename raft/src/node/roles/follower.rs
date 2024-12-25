use rand_core::RngCore;

use crate::{log::RaftLog, messages::RaftMessageEnvelope, node::state::{NodeId, RaftNode}};

use super::RaftRole;

pub struct Follower {
    leader: Option<NodeId>,
    election_countdown: u32,
    election_countdown_starting_point: u32,
}

impl<Log, Random> RaftRole<Log, Random> for Follower
where
    Log: RaftLog,
    Random: RngCore,
{
    fn tick(&mut self, raft: &mut RaftNode<Log, Random>) -> Option<RaftMessageEnvelope> {
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
