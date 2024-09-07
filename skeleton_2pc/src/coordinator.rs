extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

use crate::Stats;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision,
}

#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    stats: Stats,
    participants: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    clients: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
}

impl Coordinator {
    pub fn new(log_path: String, r: &Arc<AtomicBool>) -> Coordinator {
        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            stats: Stats::default(),
            participants: HashMap::new(),
            clients: HashMap::new(),
        }
    }

    pub fn participant_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.participants.insert(name.clone(), (tx, rx));
    }

    pub fn client_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.clients.insert(name.clone(), (tx, rx));
    }

    pub fn report_status(&mut self) {
        println!(
            "Coordinator    :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.stats.committed, self.stats.aborted, self.stats.unknown,
        );
    }

    pub fn protocol(&mut self) {
        println!("Starting coordinator protocol");
        let sid = "Coordinator".to_string();

        for (client_name, (client_tx, client_rx)) in self.clients.iter().cycle() {
            if let Ok(client_request) = client_rx.try_recv() {
                if client_request.mtype == MessageType::ClientRequest {
                    for (participant_name, (participant_tx, participant_rx)) in
                        self.participants.iter()
                    {
                        let pm = ProtocolMessage::generate(
                            MessageType::CoordinatorPropose,
                            client_request.txid.clone(),
                            sid.clone(),
                            client_request.opid,
                        );
                        self.log
                            .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);
                        participant_tx.send(pm).unwrap();
                    }

                    let mut responses = vec![];
                    for (participant_name, (participant_tx, participant_rx)) in
                        self.participants.iter()
                    {
                        let participant_response = participant_rx.recv().unwrap();
                        responses.push(participant_response);
                    }

                    let cont = responses
                        .iter()
                        .all(|r| r.mtype == MessageType::ParticipantVoteCommit);
                    let (coordinator_participant_response, coordinator_client_response) = if cont {
                        (
                            ProtocolMessage::generate(
                                MessageType::CoordinatorCommit,
                                client_request.txid.clone(),
                                sid.clone(),
                                client_request.opid,
                            ),
                            ProtocolMessage::generate(
                                MessageType::ClientResultCommit,
                                client_request.txid.clone(),
                                sid.clone(),
                                client_request.opid,
                            ),
                        )
                    } else {
                        (
                            ProtocolMessage::generate(
                                MessageType::CoordinatorAbort,
                                client_request.txid.clone(),
                                sid.clone(),
                                client_request.opid,
                            ),
                            ProtocolMessage::generate(
                                MessageType::ClientResultAbort,
                                client_request.txid.clone(),
                                sid.clone(),
                                client_request.opid,
                            ),
                        )
                    };

                    for (participant_name, (participant_tx, participant_rx)) in
                        self.participants.iter()
                    {
                        self.log.append(
                            coordinator_participant_response.mtype,
                            coordinator_participant_response.txid.clone(),
                            coordinator_participant_response.senderid.clone(),
                            coordinator_participant_response.opid,
                        );
                        participant_tx
                            .send(coordinator_participant_response.clone())
                            .unwrap();
                    }

                    self.log.append(
                        coordinator_client_response.mtype,
                        coordinator_client_response.txid.clone(),
                        coordinator_client_response.senderid.clone(),
                        coordinator_client_response.opid,
                    );
                    client_tx.send(coordinator_client_response).unwrap();
                }
            } else {
                if !self.running.load(Ordering::SeqCst) {
                    trace!("{sid}::Exiting");
                    break;
                }
            }
        }

        self.report_status();
    }
}
