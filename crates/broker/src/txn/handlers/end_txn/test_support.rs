//! Fixtures shared by the `EndTxn` unit tests: a coordinator entry in a chosen
//! producer identity and state, the partition set a marker fan-out targets, and
//! an inter-broker client with neither TLS nor SASL configured.

use {
    crate::{
        network::client::InterBrokerClient,
        txn::state::{TopicPartition, TxnEntry, TxnState},
    },
    krabka_ids::PartitionIndex,
    krabka_log::ProducerId,
};

/// Build a `TxnEntry` in a given (pid, epoch, state) for the re-validation
/// tests. Partition sets do not change the decision, so leave them empty.
pub(super) fn entry(pid: i64, epoch: i16, state: TxnState) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid-x".into(), ProducerId(pid), epoch, 60_000, 1);
    e.state = state;
    e
}

pub(super) fn marker_entry() -> TxnEntry {
    TxnEntry::new_empty("tid".to_string(), ProducerId(7), 0, 60_000, 0)
}

pub(super) fn tps() -> Vec<TopicPartition> {
    vec![TopicPartition {
        topic: "t".to_string(),
        partition: PartitionIndex(0),
    }]
}

/// A client with no TLS connector and no SASL creds — fine here, every
/// case fails at the TCP connect (unreachable address) before any
/// handshake would run.
pub(super) fn plaintext_client() -> InterBrokerClient { InterBrokerClient::new(None, None) }
