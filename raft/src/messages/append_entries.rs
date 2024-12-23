use crate::{log::{LogEntry, LogIndex}, node::state::TermId};

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub prev_log_index: LogIndex,
    pub prev_log_term: TermId,
    pub entries: Vec<LogEntry>
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub success: bool
}
