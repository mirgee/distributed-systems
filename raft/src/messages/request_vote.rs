use crate::{log::LogId, node::TermId};

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub last_log_index: LogId,
    pub last_log_term: TermId,
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub vote_granted: bool,
}
