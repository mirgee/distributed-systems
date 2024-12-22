use crate::{log::{LogEntry, LogIndex}, node::state::TermId};

pub struct AppendEntries {
    pub prev_log_index: LogIndex,
    pub prev_log_term: TermId,
    pub entries: Vec<LogEntry>
}

pub struct AppendEntriesResponse {
    pub success: bool
}
