use crate::node::state::TermId;

use super::{LogEntry, LogIndex, RaftLog};

pub struct RaftLogInMemory {
    entries: Vec<LogEntry>,
}

impl RaftLogInMemory {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
}

impl RaftLog for RaftLogInMemory {
    type Error = ();

    fn append(&mut self, entry: LogEntry) -> Result<(), Self::Error> {
        todo!()
    }

    fn get(&self, index: LogIndex) -> Result<LogEntry, Self::Error> {
        todo!()
    }

    fn get_last_index(&self) -> Result<LogIndex, Self::Error> {
        todo!()
    }

    fn get_last_term(&self) -> Result<TermId, Self::Error> {
        todo!()
    }
}
