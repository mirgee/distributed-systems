use raft::{
    log::in_memory::RaftLogInMemory,
    messages::RaftMessageEnvelope,
    node::{NodeId, RaftConfig, RaftNode},
};
use rand_core::OsRng;
use std::collections::BTreeSet;

pub struct TestRaftCluster {
    nodes: Vec<RaftNode<RaftLogInMemory, OsRng>>,
    tick_count: u64,
}

impl TestRaftCluster {
    pub fn new(size: usize) -> Self {
        let config = RaftConfig {
            heartbeat_interval: 1,
            min_election_countdown: 2,
            max_election_countdown: 3,
        };

        let all_ids: BTreeSet<_> = (0..size as u64).map(NodeId).collect();
        let mut nodes = vec![];
        for id in 0..size as u64 {
            let node_peers = all_ids
                .difference(&[NodeId(id)].into_iter().collect())
                .cloned()
                .collect();

            nodes.push(RaftNode::new(
                NodeId(id),
                node_peers,
                RaftLogInMemory::new(),
                config.clone(),
                OsRng,
            ));
        }

        Self {
            nodes,
            tick_count: 0,
        }
    }

    pub fn node(&self, idx: usize) -> &RaftNode<RaftLogInMemory, OsRng> {
        &self.nodes[idx]
    }

    pub fn tick(&mut self) -> Vec<(usize, RaftMessageEnvelope)> {
        self.tick_count += 1;
        let mut all_messages = Vec::new();
        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if let Some(msg) = node.tick() {
                all_messages.push((idx, msg));
            }
        }
        all_messages
    }

    pub fn run_until<F: Fn(&Self) -> bool>(&mut self, max_ticks: u64, predicate: F) {
        let mut ticks_left = max_ticks;
        while ticks_left > 0 {
            self.tick();
            if predicate(self) {
                return;
            }
            ticks_left -= 1;
        }
        panic!("Condition not met within {} ticks!", max_ticks);
    }

    pub fn run(&mut self, max_ticks: u64) {
        self.run_until(max_ticks, |_| true);
    }

    pub fn has_leader(&self) -> bool {
        self.nodes.iter().any(|node| node.is_leader())
    }

    pub fn send_message(&mut self, to_idx: usize, msg: RaftMessageEnvelope) {
        self.nodes[to_idx].receive_message(msg);
    }
}
