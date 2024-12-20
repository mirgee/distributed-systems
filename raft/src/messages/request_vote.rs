use crate::{log::LogIndex, node::state::TermId};

pub struct RequestVote {
    pub last_log_index: LogIndex,
    pub last_log_term: TermId
}

pub struct RequestVoteResponse {
    pub vote_granted: bool,
}
