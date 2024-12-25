use rand_core::RngCore;

use crate::{
    log::RaftLog,
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope, append_entries::AppendEntries,
        rpc::Rpc,
    },
    node::state::RaftNode,
};

use super::RaftRole;

pub struct Leader {
    heartbeat_countdown: u32,
}

impl<Log, Random> RaftRole<Log, Random> for Leader
where
    Log: RaftLog,
    Random: RngCore,
{
    fn tick(&mut self, raft: &mut RaftNode<Log, Random>) -> Option<RaftMessageEnvelope> {
        self.heartbeat_countdown = self.heartbeat_countdown.saturating_sub(1);
        if self.heartbeat_countdown == 0 {
            self.heartbeat_countdown = raft.config.heartbeat_interval;
            Some(RaftMessageEnvelope {
                msg: RaftMessage {
                    term: raft.current_term,
                    rpc: Rpc::AppendEntries(AppendEntries {
                        entries: Vec::new(),
                    }),
                },
                from: raft.node_id,
                to: MessageDestination::Broadcast,
            })
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
