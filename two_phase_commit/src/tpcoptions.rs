use clap::{value_parser, Arg, Command};

#[derive(Clone, Debug)]
pub struct TPCOptions {
    pub send_success_probability: f64,
    pub operation_success_probability: f64,
    pub num_clients: u32,
    pub num_requests: u32,
    pub num_participants: u32,
    pub verbosity: usize,
    pub mode: String,
    pub log_path: String,
    pub ipc_path: String,
    pub num: u32,
}

impl TPCOptions {
    pub fn new() -> TPCOptions {
        let default_send_success_probability = "1.0";
        let default_operation_success_probability = "1.0";
        let default_num_participants = "3";
        let default_num_clients = "3";
        let default_num_requests = "15";
        let default_verbosity = "0";
        let default_mode = "run";
        let default_log_path = "./logs/";
        let default_ipc_path = "none";
        let default_num = "0";

        let matches = Command::new("two-phase-commit")
            .version("0.1.0")
            .author("Miroslav Kovar")
            .about("Two-phase commit protocol")
            .arg(Arg::new("send_success_probability")
                    .short('S')
                    .value_parser(value_parser!(f64))
                    .help("Probability participants successfully send messages"))
            .arg(Arg::new("operation_success_probability")
                    .short('s')
                    .value_parser(value_parser!(f64))
                    .help("Probability participants successfully execute requests"))
            .arg(Arg::new("num_clients")
                    .short('c')
                    .value_parser(value_parser!(u32))
                    .help("Number of clients making requests"))
            .arg(Arg::new("num_participants")
                    .short('p')
                    .value_parser(value_parser!(u32))
                    .help("Number of participants in protocol"))
            .arg(Arg::new("num_requests")
                    .short('r')
                    .value_parser(value_parser!(u32))
                    .help("Number of requests made per client"))
            .arg(Arg::new("verbosity")
                    .short('v')
                    .value_parser(value_parser!(usize))
                    .help("Output verbosity: 0->No Output, 5->Output Everything"))
            .arg(Arg::new("log_path")
                    .short('l')
                    .value_parser(value_parser!(String))
                    .help("Specifies path to directory where logs are stored"))
            .arg(Arg::new("mode")
                    .short('m')
                    .value_parser(value_parser!(String))
                    .help("Mode: \"run\" starts 2PC, \"client\" starts a client process, \"participant\" starts a participant process, \"check\" checks logs produced by previous run"))
            .arg(Arg::new("ipc_path")
                    .long("ipc_path")
                    .value_parser(value_parser!(String))
                    .help("Path for IPC socket for communication"))
            .arg(Arg::new("num")
                    .long("num")
                    .value_parser(value_parser!(u32))
                    .help("Participant / Client number for naming the log files. Ranges from 0 to num_clients - 1 or num_participants - 1"))
            .get_matches();

        let mode = matches
            .get_one::<String>("mode")
            .unwrap_or(&default_mode.to_string())
            .to_string();
        let operation_success_probability = *matches
            .get_one::<f64>("operation_success_probability")
            .unwrap_or(
                &default_operation_success_probability
                    .parse::<f64>()
                    .unwrap(),
            );
        let send_success_probability = *matches
            .get_one::<f64>("send_success_probability")
            .unwrap_or(&default_send_success_probability.parse::<f64>().unwrap());
        let num_clients = *matches
            .get_one::<u32>("num_clients")
            .unwrap_or(&default_num_clients.parse::<u32>().unwrap());
        let num_participants = *matches
            .get_one::<u32>("num_participants")
            .unwrap_or(&default_num_participants.parse::<u32>().unwrap());
        let num_requests = *matches
            .get_one::<u32>("num_requests")
            .unwrap_or(&default_num_requests.parse::<u32>().unwrap());
        let verbosity = *matches
            .get_one::<usize>("verbosity")
            .unwrap_or(&default_verbosity.parse::<usize>().unwrap());
        let log_path = matches
            .get_one::<String>("log_path")
            .unwrap_or(&default_log_path.to_string())
            .to_string();
        let ipc_path = matches
            .get_one::<String>("ipc_path")
            .unwrap_or(&default_ipc_path.to_string())
            .to_string();
        let num = *matches
            .get_one::<u32>("num")
            .unwrap_or(&default_num.parse::<u32>().unwrap());

        match mode.as_ref() {
            "run" => {}
            "client" => {
                if ipc_path == default_ipc_path {
                    panic!("No ipc_path specified for client mode");
                }
            }
            "participant" => {
                if ipc_path == default_ipc_path {
                    panic!("No ipc_path specified for participant mode");
                }
            }
            "check" => {}
            _ => panic!("unknown execution mode requested!"),
        }

        TPCOptions {
            send_success_probability,
            operation_success_probability,
            num_clients,
            num_participants,
            num_requests,
            verbosity,
            mode,
            log_path,
            ipc_path,
            num,
        }
    }

    pub fn as_vec(&self) -> Vec<String> {
        vec![
            format!("-S{}", self.send_success_probability),
            format!("-s{}", self.operation_success_probability),
            format!("-c{}", self.num_clients),
            format!("-r{}", self.num_requests),
            format!("-p{}", self.num_participants),
            format!("-v{}", self.verbosity),
            format!("-m{}", self.mode),
            format!("-l{}", self.log_path),
            format!("--ipc_path={}", self.ipc_path),
            format!("--num={}", self.num),
        ]
    }
}
