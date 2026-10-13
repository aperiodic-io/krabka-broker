//! The shape of the `__transaction_state` internal topic.
//!
//! The first `FindCoordinator(TRANSACTION)` creates the topic, with its
//! configured partition count and replication factor
//! ([`crate::auto_topic_creation::AutoTopicCreation`]), as Kafka's
//! `KafkaApis.getCoordinator` does.

use {
    krabka_units::{ByteSize, convert::ByteSizeExt as _},
    std::collections::BTreeMap,
};

pub const TOPIC: &str = "__transaction_state";

/// The topic configs Kafka writes when it creates `__transaction_state`
/// (`TransactionCoordinator.transactionStateTopicConfigs`).
///
/// `segment_bytes` is `transaction.state.log.segment.bytes` and `min_isr` is
/// `transaction.state.log.min.isr`.
pub(crate) fn topic_configs(segment_bytes: ByteSize, min_isr: i32) -> BTreeMap<String, String> {
    use crate::config_keys::{
        CLEANUP_POLICY, COMPRESSION_TYPE, MIN_INSYNC_REPLICAS, SEGMENT_BYTES,
        UNCLEAN_LEADER_ELECTION_ENABLE,
    };
    BTreeMap::from([
        (
            UNCLEAN_LEADER_ELECTION_ENABLE.to_owned(),
            "false".to_owned(),
        ),
        (COMPRESSION_TYPE.to_owned(), "uncompressed".to_owned()),
        (CLEANUP_POLICY.to_owned(), "compact".to_owned()),
        (MIN_INSYNC_REPLICAS.to_owned(), min_isr.to_string()),
        (
            SEGMENT_BYTES.to_owned(),
            segment_bytes.bytes_u64().to_string(),
        ),
    ])
}
