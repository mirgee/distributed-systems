use std::collections::HashMap;

use crate::message::{self, ProtocolMessage, MessageType};
use crate::oplog::OpLog;

fn check_participant(
    participant: &String,
    num_commit: usize,
    num_abort: usize,
    coord_committed: &HashMap<u32, ProtocolMessage>,
    participant_log: &HashMap<u32, ProtocolMessage>,
) -> bool {
    let mut result = true;

    // Filter the participant log for Global Commits, Local Commits, and Aborted
    let participant_commit_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let participant_local_commit_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::ParticipantVoteCommit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let participant_abort_map: HashMap<u32, message::ProtocolMessage> = participant_log
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let num_participant_commit = participant_commit_map.len();
    let num_participant_local_commit = participant_local_commit_map.len();
    let num_participant_abort = participant_abort_map.len();

    info!("{participant}: C {num_participant_commit}; LC {num_participant_local_commit}; A {num_participant_abort}");

    result &= num_participant_commit <= num_commit;
    result &= num_participant_local_commit >= num_commit;
    result &= num_participant_abort <= num_abort;

    trace!("{num_participant_commit} <= {num_commit}");
    assert!(num_participant_commit <= num_commit);

    trace!("{num_commit} <= {num_participant_local_commit}");
    assert!(num_commit <= num_participant_local_commit);

    trace!("{num_abort} >= {num_participant_abort}");
    assert!(num_abort >= num_participant_abort);

    for (_, coord_msg) in coord_committed.iter() {
        let txid = coord_msg.txid.clone();
        let mut _found_txid = 0;
        let mut found_local_txid = 0;
        for (_, participant_msg) in participant_commit_map.iter() {
            if participant_msg.txid == txid {
                _found_txid += 1;
            }
        }

        for (_, participant_msg) in participant_local_commit_map.iter() {
            // Handle the case where the participant simply doesn't get the
            // global commit message from the coordinator. If the coordinator
            // committed the transaction, the participant has to have voted in
            // favor. Namely, when _found_txid != found_local_txid.
            if participant_msg.txid == txid {
                found_local_txid += 1;
            }
        }

        // Exactly one commit of txid per participant
        result &= found_local_txid == 1;
        assert!(found_local_txid == 1);
    }
    println!(
        "{} OK: Committed: {} == {} (Committed-global), Aborted: {} <= {} (Aborted-global)",
        participant.clone(),
        num_participant_commit,
        num_commit,
        num_participant_abort,
        num_abort
    );
    result
}

pub fn check_last_run(
    num_clients: u32,
    num_requests: u32,
    num_participants: u32,
    log_path: &String,
) {
    info!(
        "Checking 2PC run:  {} requests * {} clients, {} participants",
        num_requests, num_clients, num_participants
    );

    let coord_log_path = format!("{}//{}", log_path, "coordinator.log");
    let coord_log = OpLog::from_file(coord_log_path);

    let lock = coord_log.arc();
    let coord_map = lock.lock().unwrap();

    // Filter coordinator logs for Commit and Abort
    let committed: HashMap<u32, message::ProtocolMessage> = coord_map
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorCommit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let aborted: HashMap<u32, message::ProtocolMessage> = coord_map
        .iter()
        .filter(|e| (*e.1).mtype == MessageType::CoordinatorAbort)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let num_commit = committed.len();
    let num_abort = aborted.len();

    info!("CG {num_commit:?}; AG {num_abort:?}");

    // Iterate and check each participant
    for pid in 0..num_participants {
        info!("Checking participant number {pid:?}");
        let participant_id_str = format!("participant_{}", pid);
        let participant_log_path = format!("{}//{}.log", log_path, participant_id_str);
        let participant_oplog = OpLog::from_file(participant_log_path);
        let participant_lock = participant_oplog.arc();
        let participant_log = participant_lock.lock().unwrap();
        check_participant(
            &participant_id_str,
            num_commit,
            num_abort,
            &committed,
            &participant_log,
        );
    }
}
