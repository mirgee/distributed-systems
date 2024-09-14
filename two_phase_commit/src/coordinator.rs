use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;

use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcSender as Sender;

use crate::message::MessageType;
use crate::message::ProtocolMessage;
use crate::oplog;

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

#[derive(Debug, Clone)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: Arc<Mutex<oplog::OpLog>>,
    stats: Arc<Mutex<Stats>>,
    participants: Arc<Mutex<HashMap<String, (Sender<ProtocolMessage>, Arc<Mutex<Receiver<ProtocolMessage>>>)>>>,
    clients: Arc<Mutex<HashMap<String, (Sender<ProtocolMessage>, Arc<Mutex<Receiver<ProtocolMessage>>>)>>>,
}

impl Coordinator {
    pub fn new(log_path: String, r: &Arc<AtomicBool>) -> Coordinator {
        Coordinator {
            state: CoordinatorState::Quiescent,
            log: Arc::new(Mutex::new(oplog::OpLog::new(log_path))),
            running: r.clone(),
            stats: Default::default(),
            participants: Default::default(),
            clients: Default::default(),
        }
    }

    pub fn participant_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.participants.lock().unwrap().insert(name.clone(), (tx, Arc::new(Mutex::new(rx))));
    }

    pub fn client_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.clients.lock().unwrap().insert(name.clone(), (tx, Arc::new(Mutex::new(rx))));
    }

    fn report_status(&mut self) {
        println!(
            "Coordinator    :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.stats.lock().unwrap().committed, self.stats.lock().unwrap().aborted, self.stats.lock().unwrap().unknown,
        );
    }

    async fn collect_participant_responses(
        &self,
        mut rx: tokio::sync::mpsc::Receiver<(String, ProtocolMessage)>,
    ) -> Vec<ProtocolMessage> {
        let mut responses = Vec::new();
        while let Some((participant_name, response)) = rx.recv().await {
            self.log.lock().unwrap().append(
                response.mtype,
                response.txid.clone(),
                response.senderid.clone(),
                response.opid,
            );
            responses.push(response);

            if responses.len() == self.participants.lock().unwrap().len() {
                break;
            }
        }

        responses
    }

    async fn propose_to_participants(&self, client_request: &ProtocolMessage) {
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

        let mut tasks = tokio::task::JoinSet::new();
        for (participant_tx, _) in self.participants.lock().unwrap().values() {
            let participant_tx = participant_tx.clone();
            let pm_clone = pm.clone();
            tasks.spawn(async move {
                participant_tx.send(pm_clone).unwrap();
            });
        }
        tasks.join_all().await;
    }

    fn spawn_blocking_listener(
        name: String,
        rx: Arc<Mutex<Receiver<ProtocolMessage>>>,
        sender: tokio::sync::mpsc::Sender<(String, ProtocolMessage)>,
    ) {
        std::thread::spawn(move || loop {
            match rx.lock().unwrap().recv() {
                Ok(received_message) => {
                    sender
                        .blocking_send((name.clone(), received_message))
                        .unwrap();
                }
                Err(_) => {
                    break;
                }
            }
        });
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

    fn prepare_final_decision(&self, responses: Vec<ProtocolMessage>) -> bool {
        responses
            .iter()
            .all(|r| r.mtype == MessageType::ParticipantVoteCommit)
    }

    async fn handle_client_request(
        &mut self,
        client_name: String,
        client_request: ProtocolMessage,
    ) {
        self.log.lock().unwrap().append(
            client_request.mtype,
            client_request.txid.clone(),
            client_request.senderid.clone(),
            client_request.opid,
        );

        if client_request.mtype == MessageType::ClientRequest {
            self.propose_to_participants(&client_request).await;

            let (tx, rx) = tokio::sync::mpsc::channel(self.participants.lock().unwrap().len());
            for (participant_name, (_, participant_rx)) in self.participants.lock().unwrap().iter() {
                let tx_clone = tx.clone();
                Self::spawn_blocking_listener(
                    participant_name.clone(),
                    participant_rx.clone(),
                    tx_clone,
                );
            }
            let responses = self.collect_participant_responses(rx).await;

            let cont = self.prepare_final_decision(responses);
            let client_response = self.prepare_client_response(&client_request, cont);
            self.log.lock().unwrap().append(
                client_response.mtype,
                client_response.txid.clone(),
                client_response.senderid.clone(),
                client_response.opid,
            );
            let client_tx = self.clients.lock().unwrap().get(&client_name).unwrap().0.clone();
            client_tx.send(client_response).unwrap();
        }
    }

    pub async fn protocol(&mut self) {
        let (tx, mut rx) = tokio::sync::mpsc::channel(32);

        for (client_name, (_, client_rx)) in self.clients.lock().unwrap().iter() {
            let tx_clone = tx.clone();
            Self::spawn_blocking_listener(client_name.clone(), client_rx.clone(), tx_clone);
        }

        while let Some((client_name, client_request)) = rx.recv().await {
            let mut coordinator = self.clone();
            tokio::spawn(async move {
                coordinator.handle_client_request(client_name, client_request).await;
            });
        }
    }
}
