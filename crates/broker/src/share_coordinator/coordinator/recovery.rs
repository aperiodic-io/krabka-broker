//! The load of a led `__share_group_state` partition: a replay of its log
//! into the in-memory delivery state.
//!
//! [`ShareCoordinator::refresh_leader_partitions`] starts one load task for
//! each partition that this broker starts to lead. The task folds every
//! `ShareSnapshot`, `ShareUpdate`, and tombstone record of the partition into
//! a private map. It installs the map and marks the partition active only if
//! no newer term of the partition started in the meantime. This path only
//! reads the log, so it lives apart from the write path in `persist`.

use {
    bytes::Bytes,
    krabka_ids::PartitionIndex,
    krabka_log::Offset,
    krabka_protocol::records::RecordBatch,
    std::{collections::HashMap, sync::Arc},
    tokio::sync::Mutex,
    tracing::{info, warn},
};

#[cfg(test)]
mod tests;

use {
    super::{LoadStatus, ShareCoordinator, ShareStateKey3},
    crate::{
        blocking::spawn_blocking,
        error::BrokerError,
        partition::Partition,
        share_coordinator::{
            bootstrap,
            persistence::{
                KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
                ShareUpdateValue, parse_state_key,
            },
            state::SharePartitionState,
        },
    },
};

impl ShareCoordinator {
    crate::task_util::recover_after_refresh! {
        this,
        /// Applies the leadership of `image` and waits until every load that it
        /// started has ended.
        ///
        /// `Broker::start` calls this method, so the partitions that this broker
        /// leads at start serve requests as soon as the broker is up.
        ///
        /// # Errors
        ///
        /// This method does not fail at present. A partition whose replay fails
        /// is logged and stays failed until the next refresh loads it again.
        {
            info!(
                keys_loaded = this.state.len(),
                "ShareCoordinator recovery complete"
            );
            Ok(())
        }
    }

    /// Replays `state_partition` and installs the result for term
    /// `generation`.
    ///
    /// The replay reads the log on the blocking thread pool, because
    /// `Partition::read_log` takes the log mutex and reads from disk.
    pub(super) async fn load_partition(&self, state_partition: PartitionIndex, generation: u64) {
        let read_max = self.config.load_buffer_size;
        let updates_per_snapshot = self.config.snapshot_update_records_per_snapshot;
        let replayed = match self.partitions.get(bootstrap::TOPIC, state_partition) {
            Some(part) => {
                // A timed-out or cancelled append can remain in the writer's
                // mailbox. Reading the log before this barrier would publish
                // stale state even if the previous operation lock is free.
                match tokio::time::timeout(self.config.write_timeout, part.writer_barrier()).await {
                    Ok(Ok(())) => spawn_blocking(move || {
                        replay_partition(&part, state_partition, read_max, updates_per_snapshot)
                    })
                    .await
                    .unwrap_or_else(|error| {
                        Err(BrokerError::Share(format!(
                            "__share_group_state-{state_partition} replay task failed: {error}"
                        )))
                    }),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(BrokerError::Share(format!(
                        "__share_group_state-{state_partition} writer barrier timed out"
                    ))),
                }
            }
            None => Err(BrokerError::Share(format!(
                "__share_group_state-{state_partition} is not open locally"
            ))),
        };
        self.install_load(state_partition, generation, replayed)
            .await;
    }

    /// Ends the load of term `generation` of `state_partition`.
    ///
    /// The install runs under the write guard of the leadership map, and only
    /// while the partition still loads that term. A replayed map goes into the
    /// state map, and the partition becomes active. A failed replay installs
    /// nothing, and the partition becomes failed: it answers
    /// `NOT_COORDINATOR`, as Kafka's `CoordinatorRuntime` answers for a
    /// `FAILED` shard, and the next refresh loads it again. A partial map is
    /// never served.
    pub(super) async fn install_load(
        &self,
        state_partition: PartitionIndex,
        generation: u64,
        replayed: Result<HashMap<ShareStateKey3, SharePartitionState>, BrokerError>,
    ) {
        let mut led = self.leader_partitions.write().await;
        let Some(entry) = led
            .get_mut(&state_partition)
            .filter(|entry| entry.generation == generation && entry.status == LoadStatus::Loading)
        else {
            info!(
                partition = state_partition.get(),
                "__share_group_state load superseded; replayed state dropped"
            );
            return;
        };
        match replayed {
            Ok(replayed) => {
                let keys_loaded = replayed.len();
                for (key, state) in replayed {
                    self.state.insert(key, Arc::new(Mutex::new(state)));
                }
                self.uncertain_appends.remove(&state_partition);
                entry.status = LoadStatus::Active;
                info!(
                    partition = state_partition.get(),
                    keys_loaded, "__share_group_state partition loaded"
                );
            }
            Err(error) => {
                // An indeterminate append must never expose the old live
                // state. Keep requests in LOAD_IN_PROGRESS, and let the next
                // metadata refresh retry the bounded barrier and replay.
                entry.status = if self.uncertain_appends.contains_key(&state_partition) {
                    LoadStatus::Pending
                } else {
                    LoadStatus::Failed
                };
                warn!(
                    partition = state_partition.get(),
                    %error,
                    "__share_group_state load failed; the next refresh loads it again"
                );
            }
        }
    }
}

/// Reads the whole log of `state_partition` and folds it into a new map.
///
/// Kafka loads `__share_group_state` through `CoordinatorLoaderImpl` with a
/// `ShareCoordinatorRecordSerde`, and this replay skips and fails the records
/// that loader does. A record whose type is neither `ShareSnapshot` nor
/// `ShareUpdate` throws `UnknownRecordTypeException`, which the loader logs at
/// WARN and skips, value or tombstone: it can be the leftover of an aborted
/// upgrade. Every other record that does not deserialize fails the load: a
/// missing key, a key too short for its type, a key of a known type that does
/// not decode, an unsupported value version, and a value that does not decode.
/// A control batch is a transaction marker, which the share shard ignores.
///
/// # Errors
///
/// Returns the read error of the log, and the error of the first record that
/// fails the load. The caller must not serve a partial replay.
fn replay_partition(
    part: &Partition,
    state_partition: PartitionIndex,
    read_max: krabka_units::ByteSize,
    updates_per_snapshot: u32,
) -> Result<HashMap<ShareStateKey3, SharePartitionState>, BrokerError> {
    let mut replayed = HashMap::new();
    let mut offset = part.log_start_offset();
    loop {
        let batches = read_replay_batches(part, offset, read_max)?;

        if batches.is_empty() {
            break;
        }

        for batch in &batches {
            if !batch.attributes.is_control_batch() {
                for rec in &batch.records {
                    let rec_offset = Offset(batch.base_offset + i64::from(rec.offset_delta));
                    let Some(key) =
                        parse_loaded_key(rec.key.as_deref(), state_partition, rec_offset)?
                    else {
                        continue;
                    };
                    let map_key = (key.group_id.clone(), key.topic_id, key.partition);

                    // Tombstone: drop the entry.
                    let Some(value) = rec.value.as_ref() else {
                        replayed.remove(&map_key);
                        continue;
                    };

                    replay_value(&mut replayed, map_key, &key, value, ReplayPosition {
                        rec_offset,
                        updates_per_snapshot,
                    })?;
                }
            }
            offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
        }
    }
    Ok(replayed)
}

/// The load budget is a read window, not a maximum record-batch size. The raw
/// reader includes the first eligible batch whole even if it exceeds the
/// window, so a small buffer never looks like the end of the log.
fn read_replay_batches(
    part: &Partition,
    offset: Offset,
    read_max: krabka_units::ByteSize,
) -> Result<Vec<RecordBatch>, BrokerError> {
    let raw = {
        let log = part
            .log
            .lock()
            .map_err(|_| BrokerError::Share("log mutex poisoned".into()))?;
        log.read_raw(offset, log.log_end_offset(), read_max)?
    };
    let mut cursor = raw.bytes.as_ref();
    let mut batches = Vec::new();
    while !cursor.is_empty() {
        batches.push(RecordBatch::decode(&mut cursor).map_err(|error| {
            BrokerError::Share(format!(
                "share-state batch at {offset} does not decode: {error}"
            ))
        })?);
    }
    Ok(batches)
}

/// Reads the key of one record as `CoordinatorRecordSerde.deserialize` does,
/// and returns `None` for a record of an unknown type, which the load skips.
///
/// The record type is read before anything else, so a key of an unknown type
/// is skipped whatever follows its type.
///
/// # Errors
///
/// Returns [`BrokerError::Share`] for a record without a key, and the decode
/// error of a key that is too short or of a known type that does not decode.
fn parse_loaded_key(
    key: Option<&[u8]>,
    partition: PartitionIndex,
    offset: Offset,
) -> Result<Option<ShareStateKey>, BrokerError> {
    let key = key.ok_or_else(|| {
        BrokerError::Share(format!(
            "{}-{partition} record at offset {} has no key",
            bootstrap::TOPIC,
            offset.0
        ))
    })?;
    match key
        .first_chunk::<2>()
        .map(|record_type| i16::from_be_bytes(*record_type))
    {
        Some(record_type)
            if record_type != KEY_SHARE_SNAPSHOT && record_type != KEY_SHARE_UPDATE =>
        {
            warn!(
                record_type,
                offset = offset.0,
                "Unknown record type {record_type} while loading offsets and group metadata from \
                 {}-{partition}. Ignoring it. It could be a left over from an aborted upgrade.",
                bootstrap::TOPIC
            );
            Ok(None)
        }
        _ => parse_state_key(key).map(Some),
    }
}

/// Where a replayed record sits: its log offset, and the snapshot threshold.
#[derive(Clone, Copy)]
struct ReplayPosition {
    rec_offset: Offset,
    updates_per_snapshot: u32,
}

/// Folds one replayed record value into the state of `map_key`, as Kafka's
/// `handleShareSnapshot` and `handleShareUpdate` do.
///
/// A snapshot record replaces the state and records `last_snapshot_offset`.
/// An update record merges into the state, or starts it when the key has
/// none. `position` is the record offset and the snapshot threshold.
///
/// # Errors
///
/// Returns the decode error of the value: an unsupported value version, or
/// bytes that do not decode at that version.
fn replay_value(
    replayed: &mut HashMap<ShareStateKey3, SharePartitionState>,
    map_key: ShareStateKey3,
    key: &ShareStateKey,
    value: &Bytes,
    position: ReplayPosition,
) -> Result<(), BrokerError> {
    let ReplayPosition {
        rec_offset,
        updates_per_snapshot,
    } = position;
    if key.record_type == KEY_SHARE_SNAPSHOT {
        let snap = ShareSnapshotValue::decode(value)?;
        match replayed.get_mut(&map_key) {
            Some(st) => st.apply_snapshot(&snap, rec_offset, updates_per_snapshot),
            None => {
                replayed.insert(
                    map_key,
                    SharePartitionState::from_snapshot(&snap, rec_offset),
                );
            }
        }
    } else {
        let upd = ShareUpdateValue::decode(value)?;
        match replayed.get_mut(&map_key) {
            Some(st) => st.apply_update(&upd),
            None => {
                replayed.insert(map_key, SharePartitionState::from_update(&upd));
            }
        }
    }
    Ok(())
}
