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

    #[yare::parameterized(
        unfinalized = { None, TxnVersion::Classic },
        classic = { Some(0), TxnVersion::Classic },
        flexible = { Some(1), TxnVersion::Flexible },
        verified = { Some(2), TxnVersion::Verified },
    )]
    fn resolves_levels(level: Option<i16>, want: TxnVersion) {
        assert2::assert!(
            resolve_txn_version(&image_with_tv(level)) == want,
            "{level:?}"
        );
    }

    #[yare::parameterized(
        classic = { TxnVersion::Classic, 0 },
        flexible = { TxnVersion::Flexible, 1 },
        verified = { TxnVersion::Verified, 2 },
    )]
    fn level_maps_each_version(version: TxnVersion, level: i16) {
        assert2::assert!(version.level() == level, "{version:?}");
    }

    #[yare::parameterized(
        version_0 = { 0, TxnVersion::Classic, TxnVersion::Classic },
        version_3 = { 3, TxnVersion::Classic, TxnVersion::Classic },
        version_4 = { 4, TxnVersion::Classic, TxnVersion::Verified },
        version_5 = { 5, TxnVersion::Verified, TxnVersion::Verified },
    )]
    fn a_client_version_comes_from_the_request_version_alone(
        request_version: i16,
        end_txn: TxnVersion,
        add_partitions: TxnVersion,
    ) {
        assert2::assert!(
            TxnVersion::for_end_txn(request_version) == end_txn,
            "EndTxn v{request_version}"
        );
        assert2::assert!(
            TxnVersion::for_add_partitions_to_txn(request_version) == add_partitions,
            "AddPartitionsToTxn v{request_version}"
        );
    }

    #[yare::parameterized(
        classic = { TxnVersion::Classic, false, false },
        flexible = { TxnVersion::Flexible, true, false },
        verified = { TxnVersion::Verified, true, true },
    )]
    fn behavior_predicates(v: TxnVersion, want_flexible: bool, want_verified: bool) {
        assert2::assert!(
            v.flexible_records() == want_flexible,
            "{v:?} flexible_records"
        );
        assert2::assert!(v.verified() == want_verified, "{v:?} verified");
    }
}
