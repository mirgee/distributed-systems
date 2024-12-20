use std::fmt::Debug;

use crate::node::state::TermId;

pub struct LogEntry;

pub type LogIndex = u64;

pub trait RaftLog {
    type Error: Debug;

    fn append(&mut self, entry: LogEntry) -> Result<(), Self::Error>;
    fn get(&self, index: LogIndex) -> Result<LogEntry, Self::Error>;
    fn get_last_index(&self) -> Result<LogIndex, Self::Error>;
    fn get_last_term(&self) -> Result<TermId, Self::Error>;
}

#[derive(Debug)]
pub struct LogState<Log> {
    pub log: Log,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex
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
