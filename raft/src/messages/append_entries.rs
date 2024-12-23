use crate::log::LogEntry;

#[derive(Debug, Clone)]
pub struct AppendEntries {
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesResponse {
    pub success: bool,
}
