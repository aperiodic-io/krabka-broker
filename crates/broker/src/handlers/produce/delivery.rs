//! The KFC-1 scheduled-delivery gate, which decides whether a batch's
//! delivery time is one the target partition accepts.

use {
    crate::config_keys::{DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, resolve_delivery_max_delay},
    krabka_units::{Time, convert::TimeExt},
};

/// The KFC-1 produce-time delivery settings of one topic.
///
/// [`resolve_delivery_gate`] builds this once per topic, and only for a topic
/// whose `delivery.mode` is `scheduled`. An immediate topic resolves to `None`,
/// so no partition of it reads a clock or looks at a batch timestamp.
///
/// On a scheduled topic a batch's `max_timestamp` is its delivery time. The
/// rejection reads that one v2 header field, which
/// [`krabka_protocol::records::validate_one_v2_batch`] already extracted into
/// `prepare::ValidatedHeader::max_timestamp`. It decodes no record,
/// decompresses no body, and does not change the verbatim-passthrough
/// decision, so a scheduled topic keeps the same zero-copy append an immediate
/// topic gets.
///
/// The gate holds the one KFC-1 rejection that needs no log state.
/// `delivery.schedule.monotonic` is the other one, and it moved into
/// [`krabka_log::Log`]: it is a statement about what the partition already
/// holds, so the test and the append it guards have to be one critical
/// section, and only the log's own lock is that.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct DeliveryGate {
    /// `delivery.max.delay.ms`: the largest delay accepted, measured forward
    /// from produce time. `None` is the `-1` sentinel and removes the bound.
    max_delay: Option<Time>,
}

impl DeliveryGate {
    /// Whether this batch earns `INVALID_TIMESTAMP` (32).
    ///
    /// `delivery_ms` is the batch's `max_timestamp` and `produced_at_ms` is
    /// the broker's clock reading for this produce.
    ///
    /// The bound is one-sided. It limits how far ahead a producer may schedule
    /// a batch and says nothing about a delivery time in the past, which comes
    /// due at once.
    pub(super) fn rejects(self, delivery_ms: i64, produced_at_ms: i64) -> bool {
        self.max_delay
            .is_some_and(|delay| delivery_ms.saturating_sub(produced_at_ms) > delay.millis_i64())
    }
}

/// Resolve a topic's KFC-1 delivery settings from the metadata image.
///
/// `None` means `delivery.mode=immediate`, the default and Kafka's behavior:
/// the produce path then does no delivery work for the topic at all. The
/// bound comes from [`resolve_delivery_max_delay`], which falls back to its
/// default on a corrupt value exactly as the other produce-side config reads
/// do.
pub(super) fn resolve_delivery_gate(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<DeliveryGate> {
    let scheduled = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MODE))
        .map(String::as_str)
        == Some(DELIVERY_MODE_SCHEDULED);
    scheduled.then(|| DeliveryGate {
        max_delay: resolve_delivery_max_delay(image, topic),
    })
}

#[cfg(test)]
mod tests;
