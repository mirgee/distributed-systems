# TODO
* Coordinator collecting participant responses should match response to a participant and collect only responses with
relevant txid. Establish IPC channel between coordinator and participant specific to the transaction as part of proposal.
Further on, this can be abstracted behind a channel "topic".
* Improve logging
* Improve error handling
* Implement fault-tolerant write-ahead log
* Implement participant fault and recovery
* Implement coordinator fault and recovery
* Implement parallel transaction handling for participant
* Use tokio::signal::ctrlc instead of ctrlc?
* Use IPC with async support
