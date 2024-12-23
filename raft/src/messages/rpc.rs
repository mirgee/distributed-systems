use super::{append_entries::{AppendEntries, AppendEntriesResponse}, request_vote::{RequestVote, RequestVoteResponse}};

#[derive(Debug, Clone)]
pub enum Rpc {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}
