use raft::{
    log::LogEntry,
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope,
        append_entries::AppendEntries,
        request_vote::{RequestVote, RequestVoteResponse},
        rpc::Rpc,
    },
    node::{NodeId, TermId},
};

pub fn vote_request(from: NodeId, to: NodeId, term: TermId) -> RaftMessageEnvelope {
    RaftMessageEnvelope {
        msg: RaftMessage {
            term,
            rpc: Rpc::RequestVote(RequestVote {
                last_log_index: 0,
                last_log_term: 0.into(),
            }),
        },
        from,
        to: MessageDestination::To(to),
    }
}

pub fn vote_response(from: NodeId, to: NodeId, term: TermId, granted: bool) -> RaftMessageEnvelope {
    RaftMessageEnvelope {
        msg: RaftMessage {
            term,
            rpc: Rpc::RequestVoteResponse(RequestVoteResponse {
                vote_granted: granted,
            }),
        },
        from,
        to: MessageDestination::To(to),
    }
}

pub fn append_entries(
    from: NodeId,
    to: NodeId,
    term: TermId,
    entries: Vec<LogEntry>,
) -> RaftMessageEnvelope {
    RaftMessageEnvelope {
        msg: RaftMessage {
            term,
            rpc: Rpc::AppendEntries(AppendEntries { entries }),
        },
        from,
        to: MessageDestination::To(to),
    }
}
