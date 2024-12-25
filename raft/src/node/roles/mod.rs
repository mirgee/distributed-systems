pub mod candidate;
pub mod follower;
pub mod leader;

use rand_core::RngCore;

use crate::{log::RaftLog, messages::RaftMessageEnvelope};

use super::state::RaftNode;

pub trait RaftRole<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    fn tick(&mut self, raft: &mut RaftNode<Log, Random>) -> Option<RaftMessageEnvelope>;
    fn handle_message(
        &mut self,
        raft: &mut RaftNode<Log, Random>,
        message: RaftMessageEnvelope,
    ) -> Option<RaftMessageEnvelope>;
}
