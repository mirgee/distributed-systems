use super::{identifiers::NodeId, RaftNode};
use rand_core::RngCore;

use crate::{
    log::{LogEntry, RaftLog},
    messages::{
        MessageDestination, RaftMessage, RaftMessageEnvelope,
        append_entries::{AppendEntries, AppendEntriesResponse},
        request_vote::{RequestVote, RequestVoteResponse},
        rpc::Rpc,
    },
};

// TODO: Return Options based on state
impl<Log, Random> RaftNode<Log, Random>
where
    Log: RaftLog,
    Random: RngCore,
{
    pub(super) fn create_append_entries_message(
        &self,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> RaftMessageEnvelope {
        RaftMessageEnvelope {
            msg: RaftMessage {
                term: self.current_term,
                rpc: Rpc::AppendEntries(AppendEntries {
                    entries: entries.into_iter().collect(),
                }),
            },
            from: self.node_id,
            to: MessageDestination::Broadcast,
        }
    }

    pub(super) fn create_append_entries_response_message(
        &self,
        success: bool,
        from: NodeId,
    ) -> RaftMessageEnvelope {
        RaftMessageEnvelope {
            msg: RaftMessage {
                term: self.current_term,
                rpc: Rpc::AppendEntriesResponse(AppendEntriesResponse { success }),
            },
            to: MessageDestination::To(from),
            from: self.node_id,
        }
    }

    pub(super) fn create_request_vote_message(&self) -> RaftMessageEnvelope {
        let log = &self.log_state.log;
        RaftMessageEnvelope {
            msg: RaftMessage {
                term: self.current_term,
                rpc: Rpc::RequestVote(RequestVote {
                    last_log_index: log.get_last_index().unwrap().unwrap_or_default(),
                    last_log_term: log.get_last_term().unwrap().unwrap_or_default(),
                }),
            },
            from: self.node_id,
            to: MessageDestination::Broadcast,
        }
    }

    pub(super) fn create_request_vote_response_message(
        &self,
        vote_granted: bool,
    ) -> RaftMessageEnvelope {
        RaftMessageEnvelope {
            msg: RaftMessage {
                term: self.current_term,
                rpc: Rpc::RequestVoteResponse(RequestVoteResponse { vote_granted }),
            },
            to: MessageDestination::Broadcast,
            from: self.node_id,
        }
    }
}
