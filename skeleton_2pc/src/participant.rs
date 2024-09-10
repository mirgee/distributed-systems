extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::IpcSender as Sender;
use participant::rand::prelude::*;

use message::MessageType;
use message::ProtocolMessage;
use oplog;

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

    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            trace!("{}::Sent message", self.id_str.clone());
            self.log
                .append(pm.mtype, pm.txid.clone(), pm.senderid.clone(), pm.opid);
            self.tx.send(pm).unwrap();
        } else {
            trace!("{}::Failed to send message", self.id_str.clone());
            self.move_state(ParticipantState::Quiescent)
        }
    }

    fn move_state(&mut self, state: ParticipantState) {
        trace!(
            "{}::Moving state from {:?} to {:?}",
            self.id_str.clone(),
            self.state,
            state
        );
        self.state = state;
    }

    pub fn perform_operation(&mut self, request: &ProtocolMessage) {
        // TODO: We should write the transaction to a WAL after receiving propose
        trace!(
            "{}::Performing operation on message {:?}",
            self.id_str.clone(),
            request
        );
        let x: f64 = random();
        match request.mtype {
            MessageType::CoordinatorPropose => {
                assert_eq!(self.state, ParticipantState::Quiescent);
                self.stats.unknown += 1;
                self.move_state(ParticipantState::ReceivedP1);
                let response = if x <= self.operation_success_prob {
                    self.move_state(ParticipantState::VotedCommit);
                    ProtocolMessage::generate(
                        MessageType::ParticipantVoteCommit,
                        request.txid.clone(),
                        self.id_str.clone(),
                        request.opid,
                    )
                } else {
                    self.move_state(ParticipantState::VotedAbort);
                    ProtocolMessage::generate(
                        MessageType::ParticipantVoteAbort,
                        request.txid.clone(),
                        self.id_str.clone(),
                        request.opid,
                    )
                };

                self.send(response);
                self.move_state(ParticipantState::AwaitingGlobalDecision);
            }
            MessageType::CoordinatorAbort => {
                assert_eq!(self.state, ParticipantState::AwaitingGlobalDecision);
                self.stats.unknown -= 1;
                self.stats.aborted += 1;
                self.state = ParticipantState::Quiescent;
            }
            MessageType::CoordinatorCommit => {
                assert_eq!(self.state, ParticipantState::AwaitingGlobalDecision);
                self.stats.unknown -= 1;
                self.stats.committed += 1;
                self.state = ParticipantState::Quiescent;
            }
            MessageType::CoordinatorExit => {
                self.running.store(false, Ordering::SeqCst);
            }
            _ => panic!(),
        }
    }

    pub fn report_status(&mut self) {
        println!(
            "{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id_str.clone(),
            self.stats.committed,
            self.stats.aborted,
            self.stats.unknown,
        );
    }

    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            sleep(Duration::from_secs(1));
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    pub fn protocol(&mut self) {
        trace!("{}::Beginning protocol", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            match self.rx.try_recv() {
                Ok(message) => {
                    self.log.append(
                        message.mtype,
                        message.txid.clone(),
                        message.senderid.clone(),
                        message.opid,
                    );
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
