use std::sync::atomic::{AtomicU32, Ordering};

use serde_json::Value;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageType {
    ClientRequest,          // Request a transaction from the coordinator
    CoordinatorPropose,     // Coordinator sends propose work to participants
    ParticipantVoteCommit,  // Participant votes to commit in phase 1
    ParticipantVoteAbort,   // Participant votes to abort in phase 1
    CoordinatorAbort,       // Coordinator aborts in phase 2
    CoordinatorCommit,      // Coordinator commits phase 2
    ClientResultCommit,     // result (success/fail) communicated to client
    ClientResultAbort,      // result (success/fail) communicated to client
    CoordinatorExit,        // Coordinator telling client/participant about shut down
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestStatus {
    Committed,              // Request succeeded
    Aborted,                // Request explicitly aborted
    Unknown,                // Request status unknown (typically timed out)
}

/// generator for unique ids of messages
static COUNTER: AtomicU32 = AtomicU32::new(1);

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ProtocolMessage {
    pub mtype: MessageType,  //
    pub uid: u32,            // Unique ID relative to the current process
    pub txid: String,        // Transaction ID from the client (unique relative to other transactions)
    pub senderid: String,    // Sender ID (unique across all senders)
    pub opid: u32,           // Operation ID (relative to the original client who started this transaction)
}

impl ProtocolMessage {
    pub fn generate(t: MessageType, tid: String, sid: String, oid: u32) -> ProtocolMessage {
        ProtocolMessage {
            mtype: t,
            uid: COUNTER.fetch_add(1, Ordering::SeqCst),
            txid: tid,
            senderid: sid,
            opid: oid,
        }
    }
    pub fn instantiate(t: MessageType, u: u32, tid: String, sid: String, oid: u32) -> ProtocolMessage {
        ProtocolMessage {
            mtype: t,
            uid: u,
            txid: tid,
            senderid: sid,
            opid: oid,
        }
    }
    pub fn from_string(line: &String) -> ProtocolMessage {
        let data: Value = serde_json::from_str(&line.to_string()).unwrap();
        let pm: ProtocolMessage = serde_json::from_value(data).unwrap();
        pm
    }

}
