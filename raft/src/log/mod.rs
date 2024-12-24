pub mod in_memory;

use std::fmt::Debug;

use crate::node::state::TermId;

#[derive(Debug, Clone)]
pub struct LogEntry {
    term_id: TermId,
    data: Vec<u8>
}

pub type LogId = u64;

pub trait RaftLog {
    type Error: Debug;

    fn append(&mut self, entry: LogEntry) -> Result<(), Self::Error>;
    fn get(&self, index: LogId) -> Result<Option<LogEntry>, Self::Error>;
    fn get_last_index(&self) -> Result<Option<LogId>, Self::Error>;
    fn get_last_term(&self) -> Result<Option<TermId>, Self::Error>;
}

#[derive(Debug)]
pub struct LogState<Log> {
    pub log: Log,
    pub commit_index: LogId,
    pub last_applied: LogId
}

impl<Log> LogState<Log> {
    pub fn new(log: Log) -> Self {
        Self {
            log,
            commit_index: Default::default(),
            last_applied: Default::default()
        }
    }
}
