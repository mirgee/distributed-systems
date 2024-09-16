use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

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
    // TODO: Makre RwLock
    participants: Arc<
        Mutex<
            HashMap<
                String,
                (
                    Sender<ProtocolMessage>,
                    Arc<Mutex<Receiver<ProtocolMessage>>>,
                ),
            >,
        >,
    >,
    // TODO: Makre RwLock
    clients: Arc<
        Mutex<
            HashMap<
                String,
                (
                    Sender<ProtocolMessage>,
                    Arc<Mutex<Receiver<ProtocolMessage>>>,
                ),
            >,
        >,
    >,
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

    pub async fn participant_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.participants
            .lock()
            .await
            .insert(name.clone(), (tx, Arc::new(Mutex::new(rx))));
    }

    pub async fn client_join(
        &mut self,
        name: &String,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.clients
            .lock()
            .await
            .insert(name.clone(), (tx, Arc::new(Mutex::new(rx))));
    }

    async fn report_status(&mut self) {
        println!(
            "Coordinator    :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.stats.lock().await.committed,
            self.stats.lock().await.aborted,
            self.stats.lock().await.unknown,
        );
    }

    async fn collect_participant_responses(
        &self,
        mut rx: tokio::sync::mpsc::Receiver<(String, ProtocolMessage)>,
    ) -> Vec<ProtocolMessage> {
        let mut responses = Vec::new();
        while let Some((participant_name, response)) = rx.recv().await {
            self.log.lock().await.append(
                response.mtype,
                response.txid.clone(),
                response.senderid.clone(),
                response.opid,
            );
            responses.push(response);

            if responses.len() == self.participants.lock().await.len() {
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
            .await
            .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);

        let mut tasks = tokio::task::JoinSet::new();
        for (participant_tx, _) in self.participants.lock().await.values() {
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
        running_process: Arc<AtomicBool>,
    ) {
        std::thread::spawn(move || {
            while running_process.load(Ordering::SeqCst) {
                match rx
                    .blocking_lock()
                    .try_recv_timeout(Duration::from_millis(10000))
                {
                    Ok(received_message) => {
                        if let Err(err) = sender.blocking_send((name.clone(), received_message)) {
                            error!("{SID}::Error sending received message from {name}: {err}");
                        }
                    }
                    Err(err) => {
                        error!("{SID}::Error receiving message from {name}: {err}");
                        break;
                    }
                }
            }
        });
    }

    fn spawn_oneshot_listener(
        name: String,
        rx: Arc<Mutex<Receiver<ProtocolMessage>>>,
        sender: tokio::sync::mpsc::Sender<(String, ProtocolMessage)>,
    ) {
        std::thread::spawn(move || {
            match rx
                .blocking_lock()
                .try_recv_timeout(Duration::from_millis(10000))
            {
                Ok(received_message) => {
                    if let Err(err) = sender.blocking_send((name.clone(), received_message)) {
                        error!("{SID}::Error sending received message from {name}: {err}");
                    }
                }
                Err(err) => {
                    error!("{SID}::Error receiving message from {name}: {err}");
                }
            }
        });
    }

    async fn prepare_client_response(
        &self,
        client_request: &ProtocolMessage,
        cont: bool,
    ) -> ProtocolMessage {
        if cont {
            {
                let mut lock = self.stats.lock().await;
                lock.unknown -= 1;
                lock.committed += 1;
            }
            ProtocolMessage::generate(
                MessageType::ClientResultCommit,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        } else {
            {
                let mut lock = self.stats.lock().await;
                lock.unknown -= 1;
                lock.aborted += 1;
            }
            ProtocolMessage::generate(
                MessageType::ClientResultAbort,
                client_request.txid.clone(),
                SID.to_string(),
                client_request.opid,
            )
        }
    }

    async fn prepare_participant_response(
        &self,
        participant_message: &ProtocolMessage,
        cont: bool,
    ) -> ProtocolMessage {
        if cont {
            ProtocolMessage::generate(
                MessageType::CoordinatorCommit,
                participant_message.txid.clone(),
                SID.to_string(),
                participant_message.opid,
            )
        } else {
            ProtocolMessage::generate(
                MessageType::CoordinatorAbort,
                participant_message.txid.clone(),
                SID.to_string(),
                participant_message.opid,
            )
        }
    }
    fn prepare_final_decision(&self, responses: Vec<ProtocolMessage>) -> bool {
        responses
            .iter()
            .all(|r| r.mtype == MessageType::ParticipantVoteCommit)
    }

    async fn send_message_to_participants(&self, message: ProtocolMessage) {
        let mut tasks = tokio::task::JoinSet::new();
        for (participant_tx, _) in self.participants.lock().await.values() {
            let participant_tx = participant_tx.clone();
            let message = message.clone();
            tasks.spawn(async move {
                participant_tx.send(message).unwrap();
            });
        }
        tasks.join_all().await;
    }

    async fn handle_client_request(
        &mut self,
        client_name: String,
        client_request: ProtocolMessage,
    ) {
        {
            let mut lock = self.stats.lock().await;
            lock.unknown += 1;
        }
        self.log.lock().await.append(
            client_request.mtype,
            client_request.txid.clone(),
            client_request.senderid.clone(),
            client_request.opid,
        );

        if client_request.mtype == MessageType::ClientRequest {
            self.propose_to_participants(&client_request).await;

            let (ptx, prx) = tokio::sync::mpsc::channel(self.participants.lock().await.len());
            for (participant_name, (_, participant_rx)) in self.participants.lock().await.iter() {
                let ptx_clone = ptx.clone();
                Self::spawn_oneshot_listener(
                    participant_name.clone(),
                    participant_rx.clone(),
                    ptx_clone,
                );
            }
            let responses = self.collect_participant_responses(prx).await;

            let cont = self.prepare_final_decision(responses);
            
            let participant_response = self.prepare_participant_response(&client_request, cont).await;
            self.send_message_to_participants(participant_response).await;

            let client_response = self.prepare_client_response(&client_request, cont).await;
            self.log.lock().await.append(
                client_response.mtype,
                client_response.txid.clone(),
                client_response.senderid.clone(),
                client_response.opid,
            );
            let client_tx = self
                .clients
                .lock()
                .await
                .get(&client_name)
                .unwrap()
                .0
                .clone();
            trace!("{SID}::Sending message {client_response:?}");
            client_tx.send(client_response).unwrap();
        }
    }

    pub async fn protocol(&mut self) {
        let (ctx, mut crx) = tokio::sync::mpsc::channel(32);
        for (client_name, (_, client_rx)) in self.clients.lock().await.iter() {
            let ctx_clone = ctx.clone();
            Self::spawn_blocking_listener(
                client_name.clone(),
                client_rx.clone(),
                ctx_clone,
                self.running.clone(),
            );
        }

        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                    Some((client_name, client_request)) = crx.recv() => {
                        let mut coordinator = self.clone();
                        tokio::spawn(async move {
                            coordinator.handle_client_request(client_name, client_request).await;
                        });
                    },
                _ = async {
                    while self.running.load(Ordering::SeqCst) {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                } => {
                    trace!("{SID} exiting gracefully");
                    break;
                }
            }
        }

        self.report_status().await;
    }
}
