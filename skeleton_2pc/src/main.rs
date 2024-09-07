#[macro_use]
extern crate log;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
extern crate stderrlog;
use client::Client;
use coordinator::Coordinator;
use ipc_channel::ipc::channel;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcSender as Sender;
use participant::Participant;
use std::env;
use std::fs;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
pub mod checker;
pub mod client;
pub mod coordinator;
pub mod message;
pub mod oplog;
pub mod participant;
pub mod tpcoptions;
use message::ProtocolMessage;

#[derive(Debug, Default)]
pub struct Stats {
    pub committed: u64,
    pub aborted: u64,
    pub unknown: u64,
}

fn spawn_child_and_connect(
    child_opts: &tpcoptions::TPCOptions,
    mode: &str,
    num: u32
) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (server, server_name) =
        IpcOneShotServer::<(Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>::new().unwrap();
    let mut child_opts = child_opts.clone();
    child_opts.ipc_path = server_name;
    child_opts.mode = mode.to_string();
    child_opts.num = num;
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    let (sender_server_client, receiver_server_client): (
        Sender<ProtocolMessage>,
        Receiver<ProtocolMessage>,
    ) = server.accept().unwrap().1;

    (child, sender_server_client, receiver_server_client)
}

fn connect_to_coordinator(
    opts: &tpcoptions::TPCOptions,
) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (sender_server_client, receiver_client_server) = channel().unwrap();
    let (sender_client_server, receiver_server_client) = channel().unwrap();

    Sender::connect(opts.ipc_path.clone())
        .unwrap()
        .send((sender_server_client.clone(), receiver_server_client))
        .unwrap();

    (sender_client_server, receiver_client_server)
}

fn run(opts: &tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");
    let mut coordinator = Coordinator::new(coord_log_path, &running);
    let mut children = vec![];

    for client_num in 0..opts.num_clients {
        let (child, sender_server_client, receiver_server_client) =
            spawn_child_and_connect(opts, "client", client_num);
        coordinator.client_join(
            &format!("client_{client_num}"),
            sender_server_client,
            receiver_server_client,
        );
        children.push(child);
    }

    for participant_num in 0..opts.num_participants {
        let (child, sender_server_client, receiver_server_client) =
            spawn_child_and_connect(opts, "participant", participant_num);
        coordinator.participant_join(
            &format!("participant_{participant_num}"),
            sender_server_client,
            receiver_server_client,
        );
        children.push(child);
    }

    coordinator.protocol();

    for child in children.iter_mut() {
        child.wait().unwrap();
    }
}

fn run_client(opts: &tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let (sender_client_server, receiver_client_server) = connect_to_coordinator(opts);
    let mut client = Client::new(
        format!("client_{}", opts.num),
        running,
        sender_client_server,
        receiver_client_server,
    );
    client.protocol(opts.num_requests);
}

fn run_participant(opts: &tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);
    let (sender_server_client, receiver_server_client) = connect_to_coordinator(opts);

    let mut participant = Participant::new(
        format!("participant_{}", opts.num),
        participant_log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        sender_server_client,
        receiver_server_client,
    );

    participant.protocol();
}

fn main() {
    let opts = tpcoptions::TPCOptions::new();

    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .timestamp(stderrlog::Timestamp::Millisecond)
        .verbosity(opts.verbosity)
        .init()
        .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!(
            "Failed to create log_path: \"{:?}\". Error \"{:?}\"",
            opts.log_path, e
        ),
        _ => (),
    }

    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    })
    .expect("Error setting signal handler!");

    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(
            opts.num_clients,
            opts.num_requests,
            opts.num_participants,
            &opts.log_path,
        ),
        _ => panic!("Unknown mode"),
    }
}
