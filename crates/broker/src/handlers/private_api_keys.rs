//! The api keys that only krabka speaks, and the floor that keeps them clear
//! of the Apache Kafka assignments.
//!
//! The barrier, topic write-freeze, and break-glass control planes are krabka
//! features with no Kafka counterpart, so each one takes a number from this
//! private range instead of a Kafka one.

use super::wire_types::ApiKeyCode;

/// Lowest `api_key` in the krabka-private range.
///
/// Apache Kafka assigns api keys upward from 0, so krabka reserves 1000 and
/// above for RPCs that only krabka speaks. A later Kafka assignment cannot
/// reach that far by normal growth. `crates/raft/src/wire.rs` follows the same
/// convention for the controller-only RPCs at 1003 and 1004.
///
/// The broker registers a krabka-private key for dispatch but never advertises
/// it in [`crate::api_catalog::supported_apis`]. An advertised row would print
/// as `UNKNOWN(1010)` in `kafka-broker-api-versions.sh` output, which a real
/// Kafka broker never prints. A client that finds no row negotiates version
/// `(0, 0)`, and every krabka-private api is version 0 only.
pub(crate) const KRABKA_PRIVATE_API_KEY_FLOOR: ApiKeyCode = 1000;

/// `AlterBarrierGroups` (1010): creates, updates, and deletes barrier groups.
pub(crate) const ALTER_BARRIER_GROUPS_API_KEY: ApiKeyCode = 1010;

/// `DescribeBarrierGroups` (1011): reads back the barrier group definitions.
pub(crate) const DESCRIBE_BARRIER_GROUPS_API_KEY: ApiKeyCode = 1011;

/// `TriggerBarrier` (1012): starts one injection for a barrier group.
pub(crate) const TRIGGER_BARRIER_API_KEY: ApiKeyCode = 1012;

/// `ListBarrierCuts` (1013): lists the cuts that a barrier group retains.
pub(crate) const LIST_BARRIER_CUTS_API_KEY: ApiKeyCode = 1013;

/// `WriteBarrierMarkers` (1014): inter-broker append of markers into the
/// partitions that the receiving broker leads.
pub(crate) const WRITE_BARRIER_MARKERS_API_KEY: ApiKeyCode = 1014;

/// `SetTopicFreeze` (1015): sets or clears one topic write-freeze entry.
pub(crate) const SET_TOPIC_FREEZE_API_KEY: ApiKeyCode = 1015;

/// `DescribeTopicFreezes` (1016): reads back the topic write-freeze registry.
pub(crate) const DESCRIBE_TOPIC_FREEZES_API_KEY: ApiKeyCode = 1016;

/// `ProposeBreakGlass` (1017): opens one break-glass proposal.
pub(crate) const PROPOSE_BREAK_GLASS_API_KEY: ApiKeyCode = 1017;

/// `ApproveBreakGlass` (1018): adds one approval to a break-glass proposal.
pub(crate) const APPROVE_BREAK_GLASS_API_KEY: ApiKeyCode = 1018;

/// `DescribeBreakGlass` (1019): lists break-glass proposals and their approvals.
pub(crate) const DESCRIBE_BREAK_GLASS_API_KEY: ApiKeyCode = 1019;

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// Every krabka-private api key, with the name a failure reports.
    const KRABKA_PRIVATE_API_KEYS: [(&str, ApiKeyCode, ApiKeyCode); 10] = [
        ("AlterBarrierGroups", ALTER_BARRIER_GROUPS_API_KEY, 1010),
        (
            "DescribeBarrierGroups",
            DESCRIBE_BARRIER_GROUPS_API_KEY,
            1011,
        ),
        ("TriggerBarrier", TRIGGER_BARRIER_API_KEY, 1012),
        ("ListBarrierCuts", LIST_BARRIER_CUTS_API_KEY, 1013),
        ("WriteBarrierMarkers", WRITE_BARRIER_MARKERS_API_KEY, 1014),
        ("SetTopicFreeze", SET_TOPIC_FREEZE_API_KEY, 1015),
        ("DescribeTopicFreezes", DESCRIBE_TOPIC_FREEZES_API_KEY, 1016),
        ("ProposeBreakGlass", PROPOSE_BREAK_GLASS_API_KEY, 1017),
        ("ApproveBreakGlass", APPROVE_BREAK_GLASS_API_KEY, 1018),
        ("DescribeBreakGlass", DESCRIBE_BREAK_GLASS_API_KEY, 1019),
    ];

    #[test]
    fn krabka_private_api_keys_sit_in_the_private_range() {
        for (name, api_key, want) in KRABKA_PRIVATE_API_KEYS {
            assert!(api_key == want, "{name}");
            assert!(api_key >= KRABKA_PRIVATE_API_KEY_FLOOR, "{name}");
        }
    }

    #[test]
    fn krabka_private_api_keys_are_pairwise_distinct() {
        for (index, (left_name, left, _)) in KRABKA_PRIVATE_API_KEYS.iter().enumerate() {
            for (right_name, right, _) in &KRABKA_PRIVATE_API_KEYS[index + 1..] {
                assert!(left != right, "{left_name} and {right_name}");
            }
        }
    }
}
