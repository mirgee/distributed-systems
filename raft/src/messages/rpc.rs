use super::request_vote::{RequestVote, RequestVoteResponse};

pub enum Rpc {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries,
    AppendEntriesResponse,
}
