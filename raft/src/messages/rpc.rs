use super::{append_entries::{AppendEntries, AppendEntriesResponse}, request_vote::{RequestVote, RequestVoteResponse}};

pub enum Rpc {
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
}
