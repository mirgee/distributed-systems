extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

use crate::Stats;

static SID: &str = "Coordinator";

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
    log: Arc<Mutex<oplog::OpLog>>,
    stats: Stats,
    participants: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    clients: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
}

impl Coordinator {
    pub fn new(log_path: String, r: &Arc<AtomicBool>) -> Coordinator {
        Coordinator {
            state: CoordinatorState::Quiescent,
            log: Arc::new(Mutex::new(oplog::OpLog::new(log_path))),
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

    fn report_status(&mut self) {
        println!(
            "Coordinator    :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.stats.committed, self.stats.aborted, self.stats.unknown,
        );
    }

    fn collect_participant_responses(&self) -> Vec<ProtocolMessage> {
        let mut responses = vec![];
        for (_, participant_rx) in self.participants.values() {
            let participant_response = participant_rx.recv().unwrap();
            self.log.lock().unwrap().append(
                participant_response.mtype,
                participant_response.txid.clone(),
                participant_response.senderid.clone(),
                participant_response.opid,
            );
            responses.push(participant_response);
        }

        responses
    }

    fn propose_to_participants(&self, client_request: &ProtocolMessage) {
        assert_eq!(client_request.mtype, MessageType::ClientRequest);
        let pm = ProtocolMessage::generate(
            MessageType::CoordinatorPropose,
            client_request.txid.clone(),
            SID.to_string(),
            client_request.opid,
        );
        self.log
            .lock()
            .unwrap()
            .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);
        for (participant_tx, _) in self.participants.values() {
            participant_tx.send(pm.clone()).unwrap();
        }
    }

    fn prepare_participant_response(
        &self,
        client_request: &ProtocolMessage,
        cont: bool,
    ) -> ProtocolMessage {
        if cont {
            ProtocolMessage::generate(
                MessageType::CoordinatorCommit,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        } else {
            ProtocolMessage::generate(
                MessageType::CoordinatorAbort,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        }
    }

    fn prepare_client_response(
        &self,
        client_request: &ProtocolMessage,
        cont: bool,
    ) -> ProtocolMessage {
        if cont {
            ProtocolMessage::generate(
                MessageType::ClientResultCommit,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        } else {
            ProtocolMessage::generate(
                MessageType::ClientResultAbort,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        }
    }

    fn send_client_response(
        &self,
        coordinator_client_response: ProtocolMessage,
        client_tx: &Sender<ProtocolMessage>,
    ) {
        self.log.lock().unwrap().append(
            coordinator_client_response.mtype,
            coordinator_client_response.txid.clone(),
            coordinator_client_response.senderid.clone(),
            coordinator_client_response.opid,
        );
        client_tx.send(coordinator_client_response).unwrap();
    }

    fn distribute_participant_responses(&self, coordinator_participant_response: ProtocolMessage) {
        self.log.lock().unwrap().append(
            coordinator_participant_response.mtype,
            coordinator_participant_response.txid.clone(),
            coordinator_participant_response.senderid.clone(),
            coordinator_participant_response.opid,
        );
        for (participant_tx, _) in self.participants.values() {
            participant_tx
                .send(coordinator_participant_response.clone())
                .unwrap();
        }
    }

    fn prepare_final_decision(&self, responses: Vec<ProtocolMessage>) -> bool {
        responses
            .iter()
            .all(|r| r.mtype == MessageType::ParticipantVoteCommit)
    }

    fn handle_client_request(&self, client_request: ProtocolMessage, client_tx: &Sender<ProtocolMessage>) {
        self.log.lock().unwrap().append(
            client_request.mtype,
            client_request.txid.clone(),
            client_request.senderid.clone(),
            client_request.opid,
        );
        if client_request.mtype == MessageType::ClientRequest {
            self.propose_to_participants(&client_request);

            let responses = self.collect_participant_responses();

            let cont = self.prepare_final_decision(responses);
            let coordinator_participant_response =
                self.prepare_participant_response(&client_request, cont);
            let coordinator_client_response = self.prepare_client_response(&client_request, cont);

            self.distribute_participant_responses(coordinator_participant_response);
            self.send_client_response(coordinator_client_response, client_tx);
        }
    }

    pub fn protocol(&mut self) {
        println!("Starting coordinator protocol");
        for (client_tx, client_rx) in self.clients.values().cycle() {
            if let Ok(client_request) = client_rx.try_recv() {
                self.handle_client_request(client_request, &client_tx);
            } else {
                if !self.running.load(Ordering::SeqCst) {
                    trace!("{SID}::Exiting");
                    break;
                }
            }
        }

        self.report_status();
    }
}
