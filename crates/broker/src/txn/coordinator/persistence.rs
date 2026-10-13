//! Persistence and recovery of the coordinator's transaction state.
//!
//! One path appends a `TxnEntry` to its `__transaction_state` partition as a
//! byte-exact Kafka `TransactionLogKey` / `TransactionLogValue` record pair and
//! publishes it to the in-memory map once the record is committed. A second
//! appends a null-valued record under that same key, which is how KIP-98
//! expires a transactional id.
//! The third replays one `__transaction_state` partition, tombstones
//! included, for the load that follows an election.

use {
    super::{TxnCoordinator, pid_index::RecoveredTransactions},
    crate::{
        error::BrokerError,
        partition::Partition,
        txn::{
            log_record::{decode_key, decode_value, encode_key, encode_value},
            state::TxnEntry,
            version::TxnVersion,
        },
    },
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_protocol::records::{Record, RecordBatch},
    std::sync::Arc,
    tokio::sync::Mutex,
};

impl TxnCoordinator {
    pub(crate) async fn lock_state_partition_for(
        &self,
        tid: &str,
    ) -> tokio::sync::MutexGuard<'_, ()> {
        let partition = self.partition_for(tid);
        let index = usize::try_from(partition.get())
            .expect("transaction state partition index must be nonnegative");
        self.state_partition_writes[index].lock().await
    }

    /// Persists `entry` to the matching `__transaction_state` partition log,
    /// then updates the in-memory map. The partition's writer task appends the
    /// batch, in order with all other produce appends, and the map changes
    /// only once the batch is committed (see [`super::commit`]). Returns the
    /// entry as persisted.
    ///
    /// `format_txnv` is the finalized `transaction.version` that the caller
    /// resolved from the live metadata image. It selects the byte-exact Kafka
    /// `TransactionLogValue` format: v0 for `TV_0`, and v1 for `TV >= 1`. It
    /// does not decide what a request may do, and it does not stamp the
    /// record: `entry.client_transaction_version` goes to the log as the caller
    /// set it.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] with the
    /// coordinator error the client gets if the write does not commit in this
    /// broker's loaded term of the partition, and [`BrokerError::Protocol`] if
    /// the transactional id is longer than 32767 bytes, which the log key
    /// cannot carry.
    #[tracing::instrument(
        name = "txn_coordinator_put",
        level = "debug",
        skip_all,
        fields(tid = %entry.transactional_id, producer_id = entry.producer_id.0),
        err,
    )]
    pub(crate) async fn put(
        &self,
        entry: TxnEntry,
        format_txnv: TxnVersion,
    ) -> Result<TxnEntry, BrokerError> {
        let _state_partition_write = self.lock_state_partition_for(&entry.transactional_id).await;
        self.put_under_state_partition_lock(entry, format_txnv)
            .await
    }

    /// Persists one entry while the caller holds its state-partition write
    /// lock, and publishes it. The reaper uses this form to make its exact
    /// recheck and append one serialized operation.
    ///
    /// `entry.client_transaction_version` is the version Kafka records with
    /// the transition (`TransactionLogValue.ClientTransactionVersion`), which
    /// only the transition knows. `AddPartitionsToTxn` and `EndTxn` stamp the
    /// version of their own request, a server-initiated abort stamps `2` on a
    /// `TV_2` cluster and `0` below it, and every other transition, `Complete*`
    /// included,
    /// keeps what the previous record stamped, as Kafka's `TransitionData`
    /// defaults to. The wire format, by contrast, follows the cluster's
    /// current `transaction.version`: completing under a stale, lower format
    /// would drop `TransactionLogValue`'s v1-only tagged fields.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] with the
    /// coordinator error the client gets if the write does not commit in this
    /// broker's loaded term of the partition, and [`BrokerError::Protocol`] if
    /// the transactional id is longer than 32767 bytes, which the log key
    /// cannot carry.
    pub(crate) async fn put_under_state_partition_lock(
        &self,
        entry: TxnEntry,
        format_txnv: TxnVersion,
    ) -> Result<TxnEntry, BrokerError> {
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let term = self.loaded_term(p).await?;
        self.validate_pid_install(&entry)?;

        // Byte-exact Kafka TransactionLogKey(v0) + TransactionLogValue(v0/v1).
        let key = encode_key(&tid)?;
        let value = encode_value(&entry, format_txnv, self.persist_last_producer_epoch);

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        self.append_committed(
            term,
            batch,
            super::commit::transition_timeout(entry.txn_timeout_ms),
        )
        .await?;

        // Kafka's `appendTransactionToLog` callback: an append that ends in a
        // newer coordinator term does not change the cache. The load of that
        // term reads the record from the log.
        let leaders = self.leader_partitions.read().await;
        Self::require_generation(&leaders, p, term.generation)?;
        let _pid_install = self
            .pid_install
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Recheck after the append: another transaction can complete its own
        // durable append while this one awaits I/O, but publication must never
        // overwrite that transaction's PID ownership.
        self.validate_pid_install(&entry)?;
        Self::evict_superseded_pids(&self.pid_to_tid, &entry);
        self.pid_to_tid
            .insert(entry.producer_id, entry.transactional_id.clone());
        if !entry.next_producer_id.is_none() {
            self.pid_to_tid
                .insert(entry.next_producer_id, entry.transactional_id.clone());
        }
        self.state.insert(tid, Arc::new(Mutex::new(entry.clone())));
        drop(leaders);
        Ok(entry)
    }

    /// Appends a `TransactionLogKey` tombstone for `entry`'s transactional id,
    /// then drops that id from the in-memory map and from the producer-id
    /// reverse index.
    ///
    /// The record is a null-valued record under the same byte-exact
    /// `TransactionLogKey(v0)` that [`Self::put`] writes, which is how Kafka
    /// expires a transactional id: compaction reclaims the tid's history, and
    /// [`Self::recover`] already reads a null value as a delete.
    ///
    /// `entry` is the live entry, and the caller **holds its lock**. That is
    /// what makes the append and the in-memory drop one step: every path that
    /// revives a known tid mutates the entry under that same lock, so no
    /// revival can land between them and no reviving record can end up before
    /// this tombstone in the log.
    ///
    /// The id leaves the map only once the tombstone is committed, as Kafka's
    /// `removeFromCacheCallback` removes it only on a complete append. A
    /// failed append leaves the coordinator exactly as it was, and the next
    /// sweep retries.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::TransactionStateWriteUncommitted`] if the
    /// tombstone does not commit in this broker's loaded term of the
    /// partition, and [`BrokerError::Protocol`] if the transactional id is
    /// longer than 32767 bytes, which the log key cannot carry.
    // cargo-mutants: append to a live partition log + live DashMap state
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        name = "txn_coordinator_tombstone",
        level = "debug",
        skip_all,
        fields(tid = %entry.transactional_id),
        err,
    )]
    pub(crate) async fn tombstone(&self, entry: &TxnEntry) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.as_str();
        let p = self.partition_for(tid);
        let term = self.loaded_term(p).await?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(encode_key(tid)?)),
            value: None,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        self.append_committed(term, batch, super::commit::TOMBSTONE_TIMEOUT)
            .await?;

        let leaders = self.leader_partitions.read().await;
        Self::require_generation(&leaders, p, term.generation)?;
        self.state.remove(tid);
        Self::evict_entry_pids(&self.pid_to_tid, entry);
        drop(leaders);
        Ok(())
    }
}

/// Replays `__transaction_state`-`partition` from its log start offset to its
/// log end offset, as Kafka's `TransactionStateManager.loadTransactionMetadata`
/// does.
///
/// A record whose key version or value version Kafka 4.3.1 does not read is
/// logged at WARN and skipped, as Kafka skips the `UnknownKeyVersion` and
/// `UnknownValueVersion` results of `TransactionLog.read`: it can be the
/// leftover of an aborted upgrade. The key version is read first, so a
/// tombstone of an unknown key version is skipped too.
///
/// Every other failure ends the replay but not the load. Kafka's loop sits in
/// a `try` whose `catch` logs `Error loading transactions from transaction log
/// <partition>` at ERROR and returns the transactions it loaded up to the
/// failing record, which the caller then installs and serves. This replay
/// does the same for a read error, a missing key, a key or value that does not
/// decode, and a record that krabka's producer-id index refuses: a misplaced
/// transaction or a producer id that two transactions claim.
///
/// `partition_for` maps a transactional id to its state partition. The replay
/// is a pure fold over the log. It does not touch the coordinator, so a load
/// can run it on the blocking pool.
// cargo-mutants: the log walk itself. `RecoveredTransactions` carries the
// decisions and is mutation-tested on its own.
#[cfg_attr(test, mutants::skip)]
pub(super) fn replay_partition(
    part: &Partition,
    partition: PartitionIndex,
    read: (krabka_units::ByteSize, bool),
    partition_for: impl Fn(&str) -> PartitionIndex,
) -> RecoveredTransactions {
    let mut recovered = RecoveredTransactions::default();
    if let Err(error) = replay_into(&mut recovered, part, partition, read, partition_for) {
        tracing::error!(
            %error,
            "Error loading transactions from transaction log {}-{partition}",
            crate::txn::bootstrap::TOPIC
        );
    }
    recovered
}

/// Folds the log of `partition` into `recovered`, record by record, and stops
/// at the first record that fails.
///
/// # Errors
///
/// Returns [`BrokerError`] if a read or decode fails, a record is misplaced,
/// an offset overflows, or two transactions claim one producer ID. Everything
/// before the failing record stays in `recovered`.
// cargo-mutants: the log walk itself. `RecoveredTransactions` carries the
// decisions and is mutation-tested on its own.
#[cfg_attr(test, mutants::skip)]
fn replay_into(
    recovered: &mut RecoveredTransactions,
    part: &Partition,
    p: PartitionIndex,
    (read_max, last_epoch_tag): (krabka_units::ByteSize, bool),
    partition_for: impl Fn(&str) -> PartitionIndex,
) -> Result<(), BrokerError> {
    let mut offset = part.log_start_offset();
    loop {
        let out = part.read_log(offset, read_max)?;
        if out.batches.is_empty() {
            break;
        }
        for batch in &out.batches {
            if batch.base_offset < offset.0 {
                return Err(BrokerError::Txn(format!(
                    "__transaction_state-{p} replay regressed from {} to {}",
                    offset.0, batch.base_offset
                )));
            }
            for rec in &batch.records {
                // Kafka: `require(record.hasKey, "Transaction state log's key
                // should not be null")`.
                let key_bytes = rec.key.as_ref().ok_or_else(|| {
                    BrokerError::Txn("Transaction state log's key should not be null".into())
                })?;
                if let Some(version) = unknown_version(key_bytes, KEY_VERSIONS) {
                    warn_unknown_version("key", version, p);
                    continue;
                }
                let tid = decode_key(key_bytes)?;
                let partition_matches = partition_for(&tid) == p;
                let Some(value_bytes) = rec.value.as_ref() else {
                    if !partition_matches {
                        return Err(BrokerError::Txn(format!(
                            "transaction {tid} tombstone is in the wrong state partition"
                        )));
                    }
                    recovered.apply_tombstone(&tid);
                    continue;
                };
                if let Some(version) = unknown_version(value_bytes, VALUE_VERSIONS) {
                    warn_unknown_version("value", version, p);
                    continue;
                }
                let entry = decode_value(value_bytes, tid, last_epoch_tag)?;
                recovered.apply_value(entry, partition_matches)?;
            }
            offset = recovery_next_offset(batch.base_offset, batch.last_offset_delta)?;
        }
    }
    Ok(())
}

/// The `TransactionLogKey` versions Kafka 4.3.1 reads: the key's
/// `validVersions` is `"0"`.
const KEY_VERSIONS: std::ops::RangeInclusive<i16> = 0..=0;

/// The `TransactionLogValue` versions Kafka 4.3.1 reads: the value's
/// `validVersions` is `"0-1"`.
const VALUE_VERSIONS: std::ops::RangeInclusive<i16> = 0..=1;

/// Returns the leading `i16` version of `bytes` when it lies outside
/// `known`, which Kafka's `TransactionLog.read` reports as `UnknownKeyVersion`
/// or `UnknownValueVersion`.
///
/// Bytes too short for a version return `None`, so the decoder that follows
/// reports them.
fn unknown_version(bytes: &[u8], known: std::ops::RangeInclusive<i16>) -> Option<i16> {
    bytes
        .first_chunk::<2>()
        .map(|version| i16::from_be_bytes(*version))
        .filter(|version| !known.contains(version))
}

/// Logs a skipped record of an unknown key or value version as Kafka's
/// `TransactionStateManager.loadTransactionMetadata` does, at WARN.
fn warn_unknown_version(version_type: &str, version: i16, partition: PartitionIndex) {
    tracing::warn!(
        version_type,
        version,
        "Unknown message {version_type} with version {version} while loading transaction \
         state from {}-{partition}. Ignoring it. It could be a left over from an aborted \
         upgrade.",
        crate::txn::bootstrap::TOPIC
    );
}

fn recovery_next_offset(base: i64, last_delta: i32) -> Result<Offset, BrokerError> {
    let delta = i64::from(last_delta);
    let next = base
        .checked_add(delta)
        .and_then(|last| last.checked_add(1))
        .filter(|_| delta >= 0)
        .ok_or_else(|| BrokerError::Txn("transaction-state replay offset overflow".into()))?;
    Ok(Offset(next))
}

#[cfg(test)]
mod tests {
    use {
        super::{Offset, recovery_next_offset},
        crate::{
            error::BrokerError,
            txn::{
                bootstrap, coordinator::test_support::live_coordinator, state::TxnEntry,
                version::TxnVersion,
            },
        },
        assert2::{assert, check},
        krabka_ids::PartitionIndex,
        krabka_log::ProducerId,
    };

    #[test]
    fn recovery_offset_advance_is_checked_and_monotonic() {
        assert!(recovery_next_offset(7, 2).unwrap() == Offset(10));
        assert!(recovery_next_offset(7, -1).is_err());
        assert!(recovery_next_offset(i64::MAX, 0).is_err());
    }

    /// The `TransactionLogKey` writes the transactional id with an `int16`
    /// length. An id of 32767 bytes is persisted, and one of 32768 bytes is an
    /// error that appends nothing and keeps no entry, not a panic.
    #[tokio::test]
    async fn a_transactional_id_over_32767_bytes_is_not_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let (coordinator, _data) = live_coordinator(dir.path()).await;
        let log = coordinator
            .partitions
            .get(bootstrap::TOPIC, PartitionIndex(0))
            .expect("the state partition");
        for (producer_id, (length, persisted)) in
            [(32_767, true), (32_768, false)].into_iter().enumerate()
        {
            let tid = "t".repeat(length);
            let producer_id = ProducerId(i64::try_from(producer_id).unwrap() + 7);
            let entry = TxnEntry::new_empty(tid.clone(), producer_id, 0, 60_000, 0);

            let outcome = coordinator
                .put_under_state_partition_lock(entry, TxnVersion::Classic)
                .await;

            assert!(outcome.is_ok() == persisted, "{length} bytes");
            assert!(
                persisted || matches!(outcome, Err(BrokerError::Protocol(_))),
                "{length} bytes is a protocol error"
            );
            assert!(coordinator.state.contains_key(&tid) == persisted);
        }
        assert!(log.log_end_offset().0 == 1);
    }

    /// The replay follows Kafka's `TransactionStateManager.loadTransactionMetadata`.
    /// A record of a key or value version Kafka 4.3.1 does not read is skipped,
    /// as Kafka skips `UnknownKeyVersion` and `UnknownValueVersion`, and the
    /// records around it load. Any other failure ends the replay with the
    /// transactions loaded before it, Kafka's partial load: the record after
    /// it is not loaded. Bytes after a decoded key or value are ignored, as
    /// Kafka's generated readers ignore them.
    #[tokio::test]
    async fn replay_skips_unknown_versions_and_stops_at_the_first_bad_record() {
        use {
            super::{RecoveredTransactions, replay_partition},
            crate::txn::log_record::{encode_key, encode_value},
            bytes::Bytes,
            krabka_protocol::records::{Record, RecordBatch},
        };

        let record = |key: Option<Vec<u8>>, value: Option<Vec<u8>>| RecordBatch {
            records: vec![Record {
                key: key.map(Bytes::from),
                value: value.map(Bytes::from),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        let key = |tid: &str| Some(encode_key(tid).unwrap());
        let value_of = |tid: &str, producer_id: i64| {
            let entry = TxnEntry::new_empty(tid.to_owned(), ProducerId(producer_id), 0, 60_000, 0);
            encode_value(&entry, TxnVersion::Classic, false)
        };
        let with_version = |mut bytes: Vec<u8>, version: i16| {
            bytes[..2].copy_from_slice(&version.to_be_bytes());
            bytes
        };
        let with_trailing_byte = |mut bytes: Vec<u8>| {
            bytes.push(0xff);
            bytes
        };
        // "misplaced" maps to state partition 1, every other id to 0.
        let replay = |batches: Vec<RecordBatch>| async move {
            let dir = tempfile::tempdir().unwrap();
            let (coordinator, _data) = live_coordinator(dir.path()).await;
            let part = coordinator
                .partitions
                .get(bootstrap::TOPIC, PartitionIndex(0))
                .expect("the state partition");
            {
                let mut log = part.log.lock().unwrap();
                for mut batch in batches {
                    log.append(&mut batch).unwrap();
                }
            }
            let RecoveredTransactions { state, pid_to_tid } = replay_partition(
                &part,
                PartitionIndex(0),
                (krabka_units::mebibytes(1), false),
                |tid| PartitionIndex(i32::from(tid == "misplaced")),
            );
            (state, pid_to_tid)
        };
        let a = || record(key("a"), Some(value_of("a", 1)));
        let b = || record(key("b"), Some(value_of("b", 2)));
        let c = || record(key("c"), Some(value_of("c", 3)));
        let only_a = replay(vec![a()]).await;
        let a_and_b = replay(vec![a(), b()]).await;
        let a_c_and_b = replay(vec![a(), c(), b()]).await;
        assert!(only_a.0.len() == 1 && a_and_b.0.len() == 2 && a_c_and_b.0.len() == 3);

        let cases = [
            (
                "key version 1 with a value",
                record(
                    Some(with_version(encode_key("c").unwrap(), 1)),
                    Some(value_of("c", 3)),
                ),
                &a_and_b,
            ),
            (
                "key version -1 as a tombstone",
                record(Some(with_version(encode_key("a").unwrap(), -1)), None),
                &a_and_b,
            ),
            (
                "key version 1 with a key body that does not decode",
                record(Some(vec![0x00, 0x01, 0x7f]), Some(vec![0xff])),
                &a_and_b,
            ),
            (
                "value version 2",
                record(key("c"), Some(with_version(value_of("c", 3), 2))),
                &a_and_b,
            ),
            (
                "value version -1",
                record(key("c"), Some(with_version(value_of("c", 3), -1))),
                &a_and_b,
            ),
            (
                "key and value with trailing bytes",
                record(
                    Some(with_trailing_byte(encode_key("c").unwrap())),
                    Some(with_trailing_byte(value_of("c", 3))),
                ),
                &a_c_and_b,
            ),
            (
                "known value version with a corrupt value",
                record(key("c"), Some(value_of("c", 3)[..9].to_vec())),
                &only_a,
            ),
            (
                "known key version whose key does not decode",
                record(
                    Some(vec![0x00, 0x00, 0x00, 0x05, b'c']),
                    Some(value_of("c", 3)),
                ),
                &only_a,
            ),
            (
                "key shorter than its version",
                record(Some(vec![0x00]), Some(value_of("c", 3))),
                &only_a,
            ),
            (
                "record without a key",
                record(None, Some(value_of("c", 3))),
                &only_a,
            ),
            (
                "transaction in another state partition",
                record(key("misplaced"), Some(value_of("misplaced", 3))),
                &only_a,
            ),
            (
                "producer id another transaction holds",
                record(key("c"), Some(value_of("c", 1))),
                &only_a,
            ),
        ];
        for (name, odd, expected) in cases {
            check!(replay(vec![a(), odd, b()]).await == *expected, "{name}");
        }
    }
}
