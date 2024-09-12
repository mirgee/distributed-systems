use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
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
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
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
            state: ParticipantState::Quiescent,
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
            trace!("{}::Sent message", self.id_str.clone());
            self.log
                .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);
            self.tx.send(pm).unwrap();
        } else {
            trace!("{}::Failed to send message", self.id_str.clone());
            self.transition_to(ParticipantState::Quiescent)
        }
    }

    fn transition_to(&mut self, state: ParticipantState) {
        trace!(
            "{}::Moving state from {:?} to {:?}",
            self.id_str.clone(),
            self.state,
            state
        );
        self.state = state;
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

    fn handle_propose(&mut self, request: &ProtocolMessage) {
        assert_eq!(self.state, ParticipantState::Quiescent);
        self.stats.unknown += 1;
        self.transition_to(ParticipantState::ReceivedP1);
        let response = if random::<f64>() <= self.operation_success_prob {
            self.transition_to(ParticipantState::VotedCommit);
            ProtocolMessage::generate(
                MessageType::ParticipantVoteCommit,
                request.txid.clone(),
                self.id_str.clone(),
                request.opid,
            )
        } else {
            self.transition_to(ParticipantState::VotedAbort);
            ProtocolMessage::generate(
                MessageType::ParticipantVoteAbort,
                request.txid.clone(),
                self.id_str.clone(),
                request.opid,
            )
        };

        self.send(response);
        self.transition_to(ParticipantState::AwaitingGlobalDecision);
    }

    fn handle_abort(&mut self, message: &ProtocolMessage) {
        assert_eq!(self.state, ParticipantState::AwaitingGlobalDecision);
        assert_eq!(message.mtype, MessageType::CoordinatorAbort);
        self.stats.unknown -= 1;
        self.stats.aborted += 1;
        self.state = ParticipantState::Quiescent;
    }

    fn handle_commit(&mut self, message: &ProtocolMessage) {
        assert_eq!(self.state, ParticipantState::AwaitingGlobalDecision);
        assert_eq!(message.mtype, MessageType::CoordinatorCommit);
        self.stats.unknown -= 1;
        self.stats.committed += 1;
        self.state = ParticipantState::Quiescent;
    }

    fn perform_operation(&mut self, request: &ProtocolMessage) {
        // TODO: We should write the transaction to a WAL after receiving propose
        trace!(
            "{}::Performing operation on message {:?}",
            self.id_str.clone(),
            request
        );
        match request.mtype {
            MessageType::CoordinatorPropose => self.handle_propose(request),
            MessageType::CoordinatorAbort => self.handle_abort(request),
            MessageType::CoordinatorCommit => self.handle_commit(request),
            MessageType::CoordinatorExit => self.running.store(false, Ordering::SeqCst),
            _ => panic!(),
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

    fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            sleep(Duration::from_secs(1));
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            match self.receive() {
                Ok(message) => {
                    self.perform_operation(&message);
                }
                Err(_) => {
                    sleep(Duration::from_millis(100));
                }
            }
        }

        self.wait_for_exit_signal();
        self.report_status();
    }
}
