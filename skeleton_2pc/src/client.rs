extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;

use crate::message::ProtocolMessage;
use crate::Stats;

#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    pub tx: Sender<ProtocolMessage>,
    pub rx: Receiver<ProtocolMessage>,
    pub stats: Stats,
}

impl Client {
    pub fn new(
        id_str: String,
        running: Arc<AtomicBool>,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>,
    ) -> Client {
        Client {
            id_str,
            running,
            num_requests: 0,
            tx,
            rx,
            stats: Stats::default(),
        }
    }

    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            sleep(Duration::from_secs(1));
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    pub fn send_next_operation(&mut self) {
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        let pm = ProtocolMessage::generate(
            MessageType::ClientRequest,
            txid.clone(),
            self.id_str.clone(),
            self.num_requests,
        );

        info!(
            "{}::Sending operation #{}",
            self.id_str.clone(),
            self.num_requests
        );

        self.tx.send(pm).unwrap();

        trace!(
            "{}::Sent operation #{}",
            self.id_str.clone(),
            self.num_requests
        );
    }

    pub fn recv_result(&mut self) {
        info!("{}::Receiving Coordinator Result", self.id_str.clone());

        match self.rx.recv().unwrap().mtype {
            MessageType::ClientResultCommit => {
                self.stats.committed += 1;
            }
            MessageType::ClientResultAbort => {
                self.stats.aborted += 1;
            }
            MessageType::CoordinatorExit => {
                self.running.store(false, Ordering::SeqCst);
            }
            _ => {
                self.stats.unknown += 1;
            }
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

    pub fn protocol(&mut self, n_requests: u32) {
        for _ in 0..n_requests {
            self.send_next_operation();
            self.recv_result();
        }
        self.wait_for_exit_signal();
        self.report_status();
    }
}
