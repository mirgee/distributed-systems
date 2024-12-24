pub mod append_entries;
pub mod request_vote;
pub mod rpc;

use rpc::Rpc;

use crate::node::state::{NodeId, TermId};

#[derive(Debug, Clone)]
pub struct RaftMessage {
    pub term: TermId,
    pub rpc: Rpc,
}

#[derive(Debug, Clone)]
pub struct RaftMessageEnvelope {
    pub msg: RaftMessage,
    pub dst: MessageDestination,
    pub src: NodeId,
}

#[derive(Debug, Clone)]
pub enum MessageDestination {
    Broadcast,
    To(NodeId),
}
