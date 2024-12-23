use crate::node::state::TermId;

use super::{LogEntry, LogId, RaftLog};

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
        self.entries.push(entry);
        Ok(())
    }

    fn get(&self, index: LogId) -> Result<Option<LogEntry>, Self::Error> {
        Ok(self.entries.get(index as usize).cloned())
    }

    fn get_last_index(&self) -> Result<LogId, Self::Error> {
        Ok((self.entries.len() - 1) as LogId)
    }

    fn get_last_term(&self) -> Result<Option<TermId>, Self::Error> {
        Ok(self.entries.last().map(|entry| entry.term_id))
    }
}
