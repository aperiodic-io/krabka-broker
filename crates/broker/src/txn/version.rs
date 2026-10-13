//! KIP-890 transaction.version resolution.
//!
//! This module reads the finalized `transaction.version` from the live image
//! and maps it to the behavior the coordinator runs. An unfinalized, that is
//! UNKNOWN, version resolves to `Classic`, the safest behavior for a
//! pre-bootstrap or legacy image. A 4.0-formatted cluster, or a standalone
//! self-bootstrapped one, finalizes `TV_2`, so the common path is `Verified`.
//!
//! The cluster level chooses the `__transaction_state` value format and the
//! rules of a server-initiated abort (the timeout reaper and the fence that
//! `InitProducerId` runs). What a client request may do is a separate,
//! per-request version that only the request's API version decides, see
//! [`TxnVersion::for_end_txn`] and [`TxnVersion::for_add_partitions_to_txn`].
//!
//! Kafka's `TransactionVersion` defines `TV_0` to `TV_2` only. KIP-939
//! two-phase commit is not a transaction version: Kafka gates it on the broker
//! config `transaction.two.phase.commit.enable` and the `TWO_PHASE_COMMIT`
//! ACL, which `crate::handlers::init_producer_id` checks.

use krabka_metadata::MetadataImage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnVersion {
    /// `TV_0`: classic (KIP-98), non-flexible `__transaction_state` records.
    Classic,
    /// `TV_1`: flexible (tagged) `__transaction_state` records.
    Flexible,
    /// `TV_2`: an epoch bump on completion and server-side
    /// `AddPartitionsToTxn` verification. It also uses flexible records.
    Verified,
}

impl TxnVersion {
    /// Flexible `__transaction_state` record format applies at `TV >= 1`.
    pub(crate) fn flexible_records(self) -> bool {
        matches!(self, TxnVersion::Flexible | TxnVersion::Verified)
    }

    /// The epoch bump on completion and the verify-only `AddPartitionsToTxn`
    /// both apply at `TV >= 2`.
    pub(crate) fn verified(self) -> bool { matches!(self, TxnVersion::Verified) }

    /// The `transaction.version` level, which
    /// `TransactionLogValue.ClientTransactionVersion` carries.
    pub(crate) fn level(self) -> i16 {
        match self {
            TxnVersion::Classic => 0,
            TxnVersion::Flexible => 1,
            TxnVersion::Verified => 2,
        }
    }

    /// The transaction version a client speaks on an `EndTxn` request, as
    /// Kafka's `TransactionVersion.transactionVersionForEndTxn` derives it from
    /// the request version alone: v5 and later know the epoch bump (`TV_2`),
    /// every earlier version is `TV_0`. The cluster's finalized level plays no
    /// part, so a v4 client on a `TV_2` cluster keeps the classic rules.
    pub(crate) fn for_end_txn(request_version: i16) -> TxnVersion {
        if request_version > 4 {
            TxnVersion::Verified
        } else {
            TxnVersion::Classic
        }
    }

    /// The transaction version a client speaks on an `AddPartitionsToTxn`
    /// request, as Kafka's `TransactionVersion.transactionVersionForAddPartitionsToTxn`
    /// derives it: v4 and later come from `TV_2` clients or from brokers that
    /// add partitions on a `Produce` or `TxnOffsetCommit`, every earlier
    /// version is `TV_0`.
    pub(crate) fn for_add_partitions_to_txn(request_version: i16) -> TxnVersion {
        if request_version > 3 {
            TxnVersion::Verified
        } else {
            TxnVersion::Classic
        }
    }
}

pub(crate) fn resolve_txn_version(image: &MetadataImage) -> TxnVersion {
    match image.finalized_feature(krabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE)
    {
        Some(2) => TxnVersion::Verified,
        Some(1) => TxnVersion::Flexible,
        _ => TxnVersion::Classic,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_metadata::{FeatureLevelRecord, MetadataRecord},
    };

    fn image_with_tv(level: Option<i16>) -> MetadataImage {
        let mut m = MetadataImage::new(uuid::Uuid::nil());
        if let Some(l) = level {
            m.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "transaction.version".into(),
                level: l,
            }));
        }
        m
    }

    #[test]
    fn resolves_levels() {
        for (level, want) in [
            (None, TxnVersion::Classic),
            (Some(0), TxnVersion::Classic),
            (Some(1), TxnVersion::Flexible),
            (Some(2), TxnVersion::Verified),
        ] {
            assert!(
                resolve_txn_version(&image_with_tv(level)) == want,
                "{level:?}"
            );
        }
    }

    #[test]
    fn level_maps_each_version() {
        for (version, level) in [
            (TxnVersion::Classic, 0),
            (TxnVersion::Flexible, 1),
            (TxnVersion::Verified, 2),
        ] {
            assert!(version.level() == level, "{version:?}");
        }
    }

    #[test]
    fn a_client_version_comes_from_the_request_version_alone() {
        // (request version, EndTxn client version, AddPartitionsToTxn client version)
        for (request_version, end_txn, add_partitions) in [
            (0, TxnVersion::Classic, TxnVersion::Classic),
            (3, TxnVersion::Classic, TxnVersion::Classic),
            (4, TxnVersion::Classic, TxnVersion::Verified),
            (5, TxnVersion::Verified, TxnVersion::Verified),
        ] {
            assert!(
                TxnVersion::for_end_txn(request_version) == end_txn,
                "EndTxn v{request_version}"
            );
            assert!(
                TxnVersion::for_add_partitions_to_txn(request_version) == add_partitions,
                "AddPartitionsToTxn v{request_version}"
            );
        }
    }

    #[test]
    fn behavior_predicates() {
        for (v, want_flexible, want_verified) in [
            (TxnVersion::Classic, false, false),
            (TxnVersion::Flexible, true, false),
            (TxnVersion::Verified, true, true),
        ] {
            assert!(
                v.flexible_records() == want_flexible,
                "{v:?} flexible_records"
            );
            assert!(v.verified() == want_verified, "{v:?} verified");
        }
    }
}
