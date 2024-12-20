pub mod append_entries;
pub mod request_vote;
pub mod rpc;

use rpc::Rpc;

use crate::node::state::{NodeId, TermId};

pub struct RaftMessage {
    pub term: TermId,
    pub rpc: Rpc,
}

pub struct MessageEnvelope {
    pub msg: RaftMessage,
    pub dst: MessageDestination,
    pub src: NodeId,
}

pub enum MessageDestination {
    Broadcast,
    To(NodeId),
}
