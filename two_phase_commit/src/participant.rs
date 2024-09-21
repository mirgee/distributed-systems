use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ipc_channel::ipc::TryRecvError;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcSender as Sender;
use rand::prelude::*;

use crate::message::MessageType;
use crate::message::ProtocolMessage;
use crate::oplog;

use crate::Stats;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

#[derive(Debug)]
struct Transaction {
    state: TransactionState,
}

#[derive(Debug)]
pub struct Participant {
    id_str: String,
    // TODO: Manage transaction states protperly
    transactions: HashMap<String, Transaction>,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    stats: Stats,
}

impl Participant {
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Participant {
        Participant {
            id_str,
            transactions: HashMap::new(),
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob,
            operation_success_prob,
            rx,
            tx,
            stats: Stats::default(),
        }
    }

    fn send(&mut self, pm: ProtocolMessage) {
        if random::<f64>() <= self.send_success_prob {
            trace!("{}::Sent message {:?}", self.id_str.clone(), pm);
            self.log
                .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);
            self.tx.send(pm).unwrap();
        } else {
            trace!("{}::Failed to send message", self.id_str.clone());
            // Optionally handle send failure, e.g., retry or log
        }
    }

    fn receive(&mut self) -> Result<ProtocolMessage, TryRecvError> {
        match self.rx.try_recv() {
            Ok(message) => {
                self.log.append(
                    message.mtype,
                    message.txid.clone(),
                    message.senderid.clone(),
                    message.opid,
                );
                Ok(message)
            }
            Err(err) => Err(err),
        }
    }

    fn handle_propose(&mut self, message: &ProtocolMessage) {
        let txid = message.txid.clone();
        let transaction = self.transactions.entry(txid.clone()).or_insert(Transaction {
            state: TransactionState::Quiescent,
        });

        assert_eq!(transaction.state, TransactionState::Quiescent);
        self.stats.unknown += 1;
        transaction.state = TransactionState::ReceivedP1;

        let response = if random::<f64>() <= self.operation_success_prob {
            transaction.state = TransactionState::VotedCommit;
            ProtocolMessage::generate(
                MessageType::ParticipantVoteCommit,
                txid,
                self.id_str.clone(),
                message.opid,
            )
        } else {
            transaction.state = TransactionState::VotedAbort;
            ProtocolMessage::generate(
                MessageType::ParticipantVoteAbort,
                txid,
                self.id_str.clone(),
                message.opid,
            )
        };

        transaction.state = TransactionState::AwaitingGlobalDecision;
        self.send(response);
    }

    fn handle_commit(&mut self, message: &ProtocolMessage) {
        let txid = message.txid.clone();
        if let Some(transaction) = self.transactions.get_mut(&txid) {
            assert_eq!(transaction.state, TransactionState::AwaitingGlobalDecision);
            self.stats.unknown -= 1;
            self.stats.committed += 1;
            self.transactions.remove(&txid);
        } else {
            trace!("{}::Received commit for unknown transaction {}", self.id_str, txid);
        }
    }

    fn handle_abort(&mut self, message: &ProtocolMessage) {
        let txid = message.txid.clone();
        if let Some(transaction) = self.transactions.get_mut(&txid) {
            assert_eq!(transaction.state, TransactionState::AwaitingGlobalDecision);
            self.stats.unknown -= 1;
            self.stats.aborted += 1;
            self.transactions.remove(&txid);
        } else {
            trace!("{}::Received abort for unknown transaction {}", self.id_str, txid);
        }
    }

    fn perform_operation(&mut self, message: &ProtocolMessage) {
        trace!("{}::Received message {message:?}", self.id_str);
        match message.mtype {
            MessageType::CoordinatorPropose => self.handle_propose(message),
            MessageType::CoordinatorAbort => self.handle_abort(message),
            MessageType::CoordinatorCommit => self.handle_commit(message),
            MessageType::CoordinatorExit => self.running.store(false, Ordering::SeqCst),
            _ => panic!("{}::Unexpected message type {:?}", self.id_str, message.mtype),
        }
    }

    fn report_status(&mut self) {
        println!(
            "{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id_str.clone(),
            self.stats.committed,
            self.stats.aborted,
            self.stats.unknown,
        );
    }

    // TODO: Support handling multiple transactions in parallel?
    pub fn protocol(&mut self) {
        info!("{}::Beginning protocol", self.id_str.clone());

        // TODO: Refactor to use async code
        while self.running.load(Ordering::SeqCst) {
            match self.receive() {
                Ok(message) => {
                    self.perform_operation(&message);
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }

        self.report_status();
    }
}
