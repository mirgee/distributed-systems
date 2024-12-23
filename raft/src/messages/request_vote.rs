use crate::{log::LogIndex, node::state::TermId};

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub last_log_index: LogIndex,
    pub last_log_term: TermId
}

#[derive(Debug, Clone)]
pub struct RequestVoteResponse {
    pub vote_granted: bool,
}
