//! Durable-across-process-crash local spool for the AU-5 degraded path.
//!
//! The spool holds exactly the chained audit records that are not yet written
//! to the topic, in order. By default, `append` calls `fsync` before it returns
//! success. Callers that explicitly choose a wider sync cadence can batch that
//! cost and use [`Spool::sync`] when they need a durable acknowledgement.
//! `open` heals and durably truncates a torn tail frame from a crash during an
//! append.
//!
//! File: a header (the magic `KAUD`, then the `i16`
//! version), then frames. Frame: `[u32 len][record]`. Record: `[u8
//! class_tag][u32 value_len][value][u32 header_count]([u32 klen][k][u32
//! vlen][v])*`. This module uses synchronous `std::fs`, because the path is
//! degraded and low-frequency. It treats a truncated or corrupt tail frame as
//! end-of-data, but a missing or unknown file header as a hard error.
//!
//! The sidecars carry the same header: `audit.losses` then holds the `u64`
//! generation and the `u64` count, `audit.replay-offset` the `u64` offset,
//! and `audit.replay-poison` the `u64` offset and one frame. Every offset and
//! byte count here is into the frames, after the header.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use krabka_units::{
    fmt::Human as _,
    prelude::{ByteSize, ByteSizeExt as _},
};
use krabka_verified::{
    audit::{AuditLosses, settle_loss_batch},
    spool_append_decision,
};

use self::codec::{HEADER_LEN, decode_record, encode_frame, file_header, strip_file_header};
use crate::{
    ids::{MaxSpoolBytes, RecordCount, SpoolBytes},
    sink::{AuditError, AuditRecord},
};

mod codec;
mod resume;

#[cfg(test)]
pub(crate) use self::codec::HEADER_LEN as FILE_HEADER_LEN;

#[cfg(test)]
mod test_support;

const SPOOL_FILE: &str = "audit.spool";
const SPOOL_TMP: &str = "audit.spool.tmp";
const LOSS_STATE_FILE: &str = "audit.losses";
const LOSS_STATE_TMP: &str = "audit.losses.tmp";
const LOSS_STATE_LEN: usize = 16;
const REPLAY_OFFSET_FILE: &str = "audit.replay-offset";
const REPLAY_OFFSET_TMP: &str = "audit.replay-offset.tmp";
const REPLAY_POISON_FILE: &str = "audit.replay-poison";
const REPLAY_POISON_TMP: &str = "audit.replay-poison.tmp";

pub(crate) fn add_loss_state(generation: u64, count: u64, added: u64) -> (u64, u64) {
    let generation = if count == 0 {
        generation.saturating_add(1)
    } else {
        generation
    };
    (generation, count.saturating_add(added))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayRecovery {
    ClearPoison,
    RequireExplicitRecovery,
}

pub(crate) fn replay_recovery(
    valid_frame: bool,
    poison_offset: Option<u64>,
    replay_offset: u64,
) -> ReplayRecovery {
    if valid_frame && poison_offset.is_some_and(|offset| offset < replay_offset) {
        ReplayRecovery::ClearPoison
    } else {
        ReplayRecovery::RequireExplicitRecovery
    }
}

/// Writer-persisted count of fail-open records awaiting a chain marker.
#[derive(Debug)]
pub(crate) struct PendingLosses {
    state: Mutex<AuditLosses>,
    path: Option<PathBuf>,
}

impl PendingLosses {
    pub(crate) fn memory() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AuditLosses::default()),
            path: None,
        })
    }

    fn open(dir: &Path) -> Result<Arc<Self>, AuditError> {
        let path = dir.join(LOSS_STATE_FILE);
        let state = if path.exists() {
            let bytes = std::fs::read(&path).map_err(io)?;
            let bytes = strip_file_header(LOSS_STATE_FILE, &bytes)?;
            if bytes.len() != LOSS_STATE_LEN {
                return Err(AuditError::Io(format!(
                    "invalid audit loss state length {}",
                    bytes.len()
                )));
            }
            AuditLosses {
                generation: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                count: u64::from_be_bytes(bytes[8..].try_into().unwrap()),
            }
        } else {
            let state = AuditLosses::default();
            persist_loss_state(&path, state)?;
            state
        };
        Ok(Arc::new(Self {
            state: Mutex::new(state),
            path: Some(path),
        }))
    }

    pub(crate) fn add(&self, count: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.generation, state.count) = add_loss_state(state.generation, state.count, count);
    }

    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .count
    }

    #[cfg(test)]
    fn state(&self) -> AuditLosses {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn snapshot(&self) -> Option<AuditLosses> {
        let state = *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.count > 0).then_some(state)
    }

    /// Settle `batch` once its marker is durable. Losses added since the
    /// snapshot stay pending in a fresh generation (`settle_loss_batch`).
    pub(crate) fn commit(&self, batch: AuditLosses) {
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != batch.generation {
                return;
            }
            *state = settle_loss_batch(*state, batch);
            *state
        };
        self.persist_or_warn(state);
    }

    pub(crate) fn persist(&self) -> Result<(), AuditError> {
        let state = *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &self.path {
            Some(path) => persist_loss_state(path, state),
            None => Ok(()),
        }
    }

    pub(crate) fn persist_with<T>(
        &self,
        write_marker: impl FnOnce(AuditLosses) -> Result<T, AuditError>,
    ) -> Result<Option<T>, AuditError> {
        let Some(batch) = self.snapshot() else {
            return Ok(None);
        };
        self.persist()?;
        let result = write_marker(batch)?;
        self.commit(batch);
        Ok(Some(result))
    }

    /// Settle the persisted pending losses against the durable marker of
    /// their generation, if the spool holds one: a crash fell between the
    /// marker's sync and the commit's sidecar write. The marker settles only
    /// the count it reports (`settle_loss_batch`).
    fn reconcile(&self, records: &[AuditRecord]) {
        let state = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.count == 0 {
                return;
            }
            let Some(batch) = records
                .iter()
                .filter_map(loss_marker_batch)
                .find(|batch| batch.generation == state.generation)
            else {
                return;
            };
            *state = settle_loss_batch(*state, batch);
            *state
        };
        self.persist_or_warn(state);
    }

    fn persist_or_warn(&self, state: AuditLosses) {
        if let Some(path) = &self.path
            && let Err(error) = persist_loss_state(path, state)
        {
            tracing::error!(%error, "failed to persist pending audit loss count");
        }
    }
}

/// The losses a records-lost marker reports, or `None` for any other record.
fn loss_marker_batch(record: &AuditRecord) -> Option<AuditLosses> {
    if record.class != crate::event::AuditEventClass::RecordsLost {
        return None;
    }
    let value = serde_json::from_slice::<serde_json::Value>(&record.value).ok()?;
    Some(AuditLosses {
        generation: value.get("loss_generation")?.as_u64()?,
        count: value.get("records_lost")?.as_u64()?,
    })
}

fn io<E: std::fmt::Display>(e: E) -> AuditError {
    AuditError::Io(e.to_string())
}

fn persist_loss_state(path: &Path, state: AuditLosses) -> Result<(), AuditError> {
    let mut bytes = [0_u8; LOSS_STATE_LEN];
    bytes[..8].copy_from_slice(&state.generation.to_be_bytes());
    bytes[8..].copy_from_slice(&state.count.to_be_bytes());
    persist_with_header(path, LOSS_STATE_TMP, &bytes)
}

fn read_or_create_u64(path: &Path, tmp_name: &str) -> Result<u64, AuditError> {
    match std::fs::read(path) {
        Ok(bytes) => <[u8; 8]>::try_from(strip_file_header(&file_name(path), &bytes)?)
            .map(u64::from_be_bytes)
            .map_err(|_| AuditError::Io(format!("invalid state length for {}", path.display()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            persist_u64(path, tmp_name, 0)?;
            Ok(0)
        }
        Err(error) => Err(io(error)),
    }
}

fn persist_u64(path: &Path, tmp_name: &str, value: u64) -> Result<(), AuditError> {
    persist_with_header(path, tmp_name, &value.to_be_bytes())
}

/// The file name of `path`, for an error that names the file.
fn file_name(path: &Path) -> String {
    path.file_name().map_or_else(
        || path.display().to_string(),
        |name| name.to_string_lossy().into_owned(),
    )
}

/// Atomically replace `path` with the file header followed by `body`.
fn persist_with_header(path: &Path, tmp_name: &str, body: &[u8]) -> Result<(), AuditError> {
    let mut bytes = Vec::with_capacity(HEADER_LEN + body.len());
    bytes.extend_from_slice(&file_header());
    bytes.extend_from_slice(body);
    persist_bytes(path, tmp_name, &bytes)
}

fn persist_bytes(path: &Path, tmp_name: &str, bytes: &[u8]) -> Result<(), AuditError> {
    let tmp = path.with_file_name(tmp_name);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .map_err(io)?;
    file.write_all(bytes).map_err(io)?;
    file.sync_all().map_err(io)?;
    std::fs::rename(tmp, path).map_err(io)?;
    sync_parent(path)
}

/// Append-only durable spool file.
///
/// The byte cap and the running total are raw [`MaxSpoolBytes`] and
/// [`SpoolBytes`] newtypes. The spool adds and compares both of them exactly,
/// which an `f64`-backed [`ByteSize`] cannot promise. The quantity stays the
/// boundary type: [`Spool::open`] takes one and [`Spool::size`] returns one.
#[derive(Debug)]
pub struct Spool {
    path: PathBuf,
    file: File,
    max_bytes: MaxSpoolBytes,
    bytes: SpoolBytes,
    count: RecordCount,
    recovered_torn_tail: bool,
    sync_every: NonZeroU64,
    unsynced: u64,
    replay_offset: u64,
    pending_losses: Arc<PendingLosses>,
}

impl Spool {
    /// Open the spool and recover its existing contents.
    ///
    /// This function creates the directory and the file if they do not exist.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), max_size = %max_size.human(), count = tracing::field::Empty, bytes = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error if the spool directory cannot be created. Returns an
    /// error if the spool file cannot be opened, read, or truncated.
    pub fn open(dir: &Path, max_size: ByteSize) -> Result<Self, AuditError> {
        Self::open_with_sync_every(dir, max_size, NonZeroU64::MIN)
    }

    /// Open the spool with an explicit append-to-`fsync` cadence.
    ///
    /// A cadence of `N` syncs after every `N` successful appends. Appends before
    /// that boundary are visible after a process crash but are not promised to
    /// survive power loss until [`Self::sync`] succeeds.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), max_size = %max_size.human(), sync_every = sync_every.get(), count = tracing::field::Empty, bytes = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error if the spool directory cannot be created. Returns an
    /// error if the spool file cannot be opened, read, synced, or truncated.
    pub fn open_with_sync_every(
        dir: &Path,
        max_size: ByteSize,
        sync_every: NonZeroU64,
    ) -> Result<Self, AuditError> {
        std::fs::create_dir_all(dir).map_err(io)?;
        let pending_losses = PendingLosses::open(dir)?;
        let path = dir.join(SPOOL_FILE);
        let replay_offset_path = dir.join(REPLAY_OFFSET_FILE);
        let replay_offset = read_or_create_u64(&replay_offset_path, REPLAY_OFFSET_TMP)?;
        if !path.exists() {
            // Created whole, header included, by rename: a crash leaves either
            // no spool or one with its header, never a headerless file.
            persist_with_header(&path, SPOOL_TMP, &[])?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(io)?;
        let mut s = Self {
            path,
            file,
            max_bytes: MaxSpoolBytes(max_size.bytes_u64()),
            bytes: SpoolBytes(0),
            count: RecordCount(0),
            recovered_torn_tail: false,
            sync_every,
            unsynced: 0,
            replay_offset,
            pending_losses,
        };
        let (records, valid_bytes) = s.scan()?;
        let physical = s.file.metadata().map_err(io)?.len();
        s.recovered_torn_tail = header_len_u64() + valid_bytes.0 < physical;
        if s.recovered_torn_tail {
            s.file
                .set_len(header_len_u64() + valid_bytes.0)
                .map_err(io)?;
            s.file.sync_all().map_err(io)?;
            tracing::warn!(
                physical,
                valid_bytes = valid_bytes.0,
                "audit spool: truncated torn tail frame on open"
            );
        }
        s.bytes = valid_bytes;
        let unread = s.unread_records(&records, valid_bytes)?;
        s.count = RecordCount(u64::try_from(unread.len()).unwrap_or(u64::MAX));
        s.reconcile_replay_poison()?;
        s.pending_losses.reconcile(&records);
        if s.count.0 == 0 && s.replay_offset > 0 {
            s.truncate()?;
        }
        let span = tracing::Span::current();
        span.record("count", s.count.0);
        span.record("bytes", s.bytes.0);
        Ok(s)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count.0 == 0
    }

    #[must_use]
    pub fn count(&self) -> RecordCount {
        self.count
    }

    pub(crate) fn pending_losses(&self) -> Arc<PendingLosses> {
        Arc::clone(&self.pending_losses)
    }

    /// How many bytes the spool currently holds.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        ByteSize::from_bytes(self.bytes.0)
    }

    /// Whether [`Spool::open`] found a torn tail frame and cut it off.
    ///
    /// A crash partway through an append leaves a frame whose length prefix
    /// promises more bytes than follow it. `open` heals that, and this reports
    /// whether it had to -- healing a spool that was intact would mean
    /// discarding good bytes, so the two cases are worth telling apart.
    #[must_use]
    pub fn recovered_torn_tail(&self) -> bool {
        self.recovered_torn_tail
    }

    /// Append a record to the spool.
    ///
    /// Returns `Ok(false)` if the record would make the spool exceed the
    /// configured cap.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(class = ?record.class, value_bytes = record.value.len(), count = self.count.0),
        err
    )]
    /// # Errors
    /// Returns an error if the seek, write, rollback, or configured sync on the
    /// spool file fails.
    pub fn append(&mut self, record: &AuditRecord) -> Result<bool, AuditError> {
        self.append_inner(record)
    }

    /// Persist a loss marker within the configured cap.
    pub(crate) fn append_loss_marker(&mut self, record: &AuditRecord) -> Result<(), AuditError> {
        if !self.append(record)? {
            return Err(AuditError::Unavailable("audit spool is full".into()));
        }
        self.sync()
    }

    pub(crate) fn begin_replay(&self, record: &AuditRecord) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        let mut poison = self.replay_offset.to_be_bytes().to_vec();
        poison.extend_from_slice(&encode_frame(record));
        persist_with_header(&path, REPLAY_POISON_TMP, &poison)
    }

    pub(crate) fn commit_replay(&mut self, record: &AuditRecord) -> Result<(), AuditError> {
        let frame_len = u64::try_from(encode_frame(record).len()).unwrap_or(u64::MAX);
        let replay_offset = self
            .replay_offset
            .checked_add(frame_len)
            .ok_or_else(|| AuditError::Io("audit replay offset overflow".into()))?;
        persist_u64(
            &self.path.with_file_name(REPLAY_OFFSET_FILE),
            REPLAY_OFFSET_TMP,
            replay_offset,
        )?;
        self.replay_offset = replay_offset;
        self.count.0 = self.count.0.saturating_sub(1);
        self.clear_replay_poison()
    }

    pub(crate) fn abort_replay(&self) -> Result<(), AuditError> {
        self.clear_replay_poison()
    }

    fn clear_replay_poison(&self) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        match std::fs::remove_file(&path) {
            Ok(()) => sync_parent(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io(error)),
        }
    }

    fn reconcile_replay_poison(&self) -> Result<(), AuditError> {
        let path = self.path.with_file_name(REPLAY_POISON_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(io(error)),
        };
        let bytes = strip_file_header(REPLAY_POISON_FILE, &bytes)?;
        let poison_offset = bytes
            .get(..8)
            .and_then(|prefix| <[u8; 8]>::try_from(prefix).ok())
            .map(u64::from_be_bytes);
        let valid_frame = bytes
            .get(8..12)
            .and_then(|prefix| <[u8; 4]>::try_from(prefix).ok())
            .map(u32::from_be_bytes)
            .and_then(|len| usize::try_from(len).ok())
            .is_some_and(|len| len.checked_add(12) == Some(bytes.len()));
        match replay_recovery(valid_frame, poison_offset, self.replay_offset) {
            ReplayRecovery::ClearPoison => self.clear_replay_poison(),
            ReplayRecovery::RequireExplicitRecovery => {
                Err(AuditError::Poisoned(path.display().to_string()))
            }
        }
    }

    fn append_inner(&mut self, record: &AuditRecord) -> Result<bool, AuditError> {
        let frame = encode_frame(record);
        let frame_len = u64::try_from(frame.len()).unwrap_or(u64::MAX);
        let decision = spool_append_decision(
            self.bytes.0,
            frame_len,
            self.max_bytes.0,
            self.unsynced,
            self.sync_every.get(),
        );
        if !decision.accepted {
            return Ok(false);
        }
        let old_len = self.file.metadata().map_err(io)?.len();
        self.file.seek(SeekFrom::End(0)).map_err(io)?;
        if let Err(error) = self.file.write_all(&frame) {
            return Err(self.rollback_append(old_len, error));
        }
        if decision.sync
            && let Err(error) = self.file.sync_all()
        {
            return Err(self.rollback_append(old_len, error));
        }
        self.unsynced = decision.next_unsynced;
        self.bytes = SpoolBytes(decision.new_bytes);
        self.count.0 = self.count.0.saturating_add(1);
        Ok(true)
    }

    /// Force all appended records to stable storage.
    ///
    /// Callers using a sync cadence greater than one use this before reporting
    /// a durable acknowledgement.
    /// # Errors
    /// Returns an error if the spool file cannot be synced.
    pub fn sync(&mut self) -> Result<(), AuditError> {
        self.file.sync_all().map_err(io)?;
        self.unsynced = 0;
        Ok(())
    }

    fn rollback_append<E: std::fmt::Display>(&mut self, old_len: u64, error: E) -> AuditError {
        let original = error.to_string();
        let rollback = self
            .file
            .set_len(old_len)
            .and_then(|()| self.file.seek(SeekFrom::Start(old_len)).map(drop))
            .and_then(|()| self.file.sync_all());
        match rollback {
            Ok(()) => AuditError::Io(original),
            Err(rollback) => AuditError::Io(format!(
                "{original}; failed to roll back partial append: {rollback}"
            )),
        }
    }

    /// Scan the spool file for the decoded records and the logical length.
    ///
    /// The logical length is the byte offset immediately after the last
    /// complete frame that decoded successfully. This method treats a truncated
    /// or corrupt tail frame as end-of-data. `valid_bytes` then points to the
    /// position immediately before that torn frame.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(records = tracing::field::Empty, valid_bytes = tracing::field::Empty),
        err
    )]
    fn scan(&self) -> Result<(Vec<AuditRecord>, SpoolBytes), AuditError> {
        let mut buf = Vec::new();
        {
            let mut f = File::open(&self.path).map_err(io)?;
            f.read_to_end(&mut buf).map_err(io)?;
        }
        let mut out = Vec::new();
        let mut cur: &[u8] = strip_file_header(SPOOL_FILE, &buf)?;
        let mut valid_bytes = SpoolBytes(0);
        while cur.len() >= 4 {
            let len =
                usize::try_from(u32::from_be_bytes([cur[0], cur[1], cur[2], cur[3]])).unwrap_or(0);
            if cur.len() < 4 + len {
                break; // truncated tail frame
            }
            match decode_record(&cur[4..4 + len]) {
                Some(rec) => {
                    out.push(rec);
                    valid_bytes += SpoolBytes(u64::try_from(4 + len).unwrap_or(0));
                }
                None => break, // corrupt frame: stop (visible)
            }
            cur = &cur[4 + len..];
        }
        let span = tracing::Span::current();
        span.record("records", out.len());
        span.record("valid_bytes", valid_bytes.0);
        Ok((out, valid_bytes))
    }

    fn unread_records(
        &self,
        records: &[AuditRecord],
        valid_bytes: SpoolBytes,
    ) -> Result<Vec<AuditRecord>, AuditError> {
        if self.replay_offset > valid_bytes.0 {
            if valid_bytes.0 == 0 {
                return Ok(Vec::new());
            }
            return Err(AuditError::Io(format!(
                "audit replay offset {} exceeds spool length {}",
                self.replay_offset, valid_bytes.0
            )));
        }
        let mut offset = 0_u64;
        let mut unread = Vec::new();
        let mut at_boundary = self.replay_offset == 0;
        for record in records {
            if offset == self.replay_offset {
                at_boundary = true;
            }
            if at_boundary {
                unread.push(record.clone());
            }
            offset = offset
                .saturating_add(u64::try_from(encode_frame(record).len()).unwrap_or(u64::MAX));
        }
        if offset == self.replay_offset {
            at_boundary = true;
        }
        if !at_boundary {
            return Err(AuditError::Io(format!(
                "audit replay offset {} is not a frame boundary",
                self.replay_offset
            )));
        }
        Ok(unread)
    }

    /// Read every record from the start of the spool, in order.
    /// # Errors
    /// Returns an error if the spool file cannot be opened or read.
    pub fn read_all(&self) -> Result<Vec<AuditRecord>, AuditError> {
        let (records, valid_bytes) = self.scan()?;
        self.unread_records(&records, valid_bytes)
    }

    /// Clear the spool.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0, bytes = self.bytes.0), err)]
    /// # Errors
    /// Returns an error if the spool file cannot be truncated, or if the seek
    /// or sync that follows fails.
    pub fn truncate(&mut self) -> Result<(), AuditError> {
        // Down to the header, which a spool file always keeps.
        self.file.set_len(header_len_u64()).map_err(io)?;
        self.bytes = SpoolBytes(0);
        self.count = RecordCount(0);
        self.unsynced = 0;
        self.file
            .seek(SeekFrom::Start(header_len_u64()))
            .map_err(io)?;
        self.file.sync_all().map_err(io)?;
        persist_u64(
            &self.path.with_file_name(REPLAY_OFFSET_FILE),
            REPLAY_OFFSET_TMP,
            0,
        )?;
        self.replay_offset = 0;
        Ok(())
    }
}

/// [`HEADER_LEN`] as a file offset.
fn header_len_u64() -> u64 {
    u64::try_from(HEADER_LEN).expect("a six-byte header fits u64")
}

// cargo-mutants: I/O-only wrapper with no in-process signal (fsyncs directory entry).
#[cfg(unix)]
#[cfg_attr(test, mutants::skip)]
fn sync_parent(path: &Path) -> Result<(), AuditError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(io)?
        .sync_all()
        .map_err(io)
}

// cargo-mutants: I/O-only wrapper with no in-process signal (stats directory).
#[cfg(not(unix))]
#[cfg_attr(test, mutants::skip)]
fn sync_parent(path: &Path) -> Result<(), AuditError> {
    // Windows exposes no directory handle to fsync, so the directory entry is
    // already durable by the time the file write returns. Stat the parent so a
    // vanished spool directory still surfaces here rather than silently later.
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .metadata()
        .map_err(io)
        .map(drop)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::prelude::ByteSize;

    use super::*;
    use crate::{
        chain::{GENESIS_HEAD, chain_hash},
        event::{
            AuditEndpoint, AuditEvent, AuditEventClass, AuditOutcome, AuditPrincipal,
            PrivilegedPhase,
        },
        ocsf::ProductInfo,
        spool::test_support::{
            ROOMY_CAP, chained_record, check_empty_file, reopen_after_losses, seeded_spool,
            spool_with_losses,
        },
    };

    #[test]
    fn append_then_read_round_trips_records_with_headers() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.is_empty());
        let r0 = chained_record(0, &GENESIS_HEAD, b"{\"i\":0}");
        let r1 = chained_record(1, &chain_hash(&GENESIS_HEAD, 0, b"{\"i\":0}"), b"{\"i\":1}");
        check!((s.append(&r0).unwrap(), s.append(&r1).unwrap()) == (true, true));
        let read = s.read_all().unwrap();
        check!((s.count().0, s.is_empty(), read) == (2, false, vec![r0.clone(), r1.clone()]));
    }

    /// The privileged-action event shares the `ApiActivity` class tag, so the
    /// frame format carries it with no change. This proves the round trip on a
    /// record the broker actually builds, headers and OCSF value included.
    #[test]
    fn privileged_action_record_round_trips_through_the_frame_format() {
        let product = ProductInfo {
            vendor_name: "Krabka".into(),
            name: "krabka-broker".into(),
            version: "0".into(),
        };
        let event = AuditEvent::PrivilegedAction {
            outcome: AuditOutcome::Success,
            phase: PrivilegedPhase::Approved,
            action: "unclean_elect_leaders".into(),
            target: "orders-3".into(),
            proposal_id: "bg-7".into(),
            principal: AuditPrincipal {
                name: "User:bob".into(),
                auth_method: "MTls".into(),
            },
            counterparties: vec![AuditPrincipal {
                name: "User:alice".into(),
                auth_method: "MTls".into(),
            }],
            approver_set_fingerprint: "f00dcafe".into(),
            key_id: "op-1".into(),
            signature: vec![0xde, 0xad, 0xbe, 0xef],
            signature_verified: true,
            signed_at_ms: 0,
            source: AuditEndpoint {
                ip: "10.0.0.4".into(),
                port: 9092,
            },
            reason: "incident 42".into(),
            time_ms: 10,
        };
        let mut rec = AuditRecord::from_event(&event, &product);
        rec.push_chain_headers(0, &GENESIS_HEAD);

        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.append(&rec).unwrap());
        // Reopen, so the read goes through the on-disk frames and not through
        // any state `append` kept in memory.
        let s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.read_all().unwrap() == vec![rec.clone()]);
        check!(rec.class == AuditEventClass::ApiActivity);
    }

    #[test]
    fn overflow_is_rejected_not_truncated() {
        let dir = tempfile::tempdir().unwrap();
        // tiny cap: first record fits, second does not
        let r = chained_record(0, &GENESIS_HEAD, b"0123456789");
        let one = {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        let dir2 = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir2.path(), one).unwrap(); // exactly one record fits
        check!(s.append(&r).unwrap()); // accepted
        check!(!s.append(&r).unwrap()); // rejected (would exceed the cap)
        check!((s.count().0, s.read_all().unwrap().len()) == (1, 1)); // not corrupted
    }

    #[test]
    fn append_rejects_size_overflow_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ByteSize::from_bytes(u64::MAX)).unwrap();
        spool.bytes = SpoolBytes(u64::MAX);

        check!(
            !spool
                .append(&chained_record(0, &GENESIS_HEAD, b"x"))
                .unwrap()
        );
        check!((spool.bytes.0, spool.count.0) == (u64::MAX, 0));
        check_empty_file(dir.path());
    }

    #[test]
    fn replay_cursor_keeps_only_unacknowledged_records_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"a");
        let r1 = chained_record(1, &GENESIS_HEAD, b"b");
        let r2 = chained_record(2, &GENESIS_HEAD, b"c");
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&r2).unwrap();
        let physical_size = s.size();
        s.begin_replay(&r0).unwrap();
        s.commit_replay(&r0).unwrap();
        drop(s);

        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(s.size() > ByteSize::ZERO); // `*=` mutant would leave bytes at 0
        check!(s.size() == physical_size);
        check!((s.count().0, s.read_all().unwrap()) == (2, vec![r1, r2]));
        s.truncate().unwrap();
        check!((s.is_empty(), s.read_all().unwrap().is_empty()) == (true, true));
    }

    #[test]
    fn append_accepts_record_that_exactly_fills_to_max() {
        let probe = tempfile::tempdir().unwrap();
        let r = chained_record(0, &GENESIS_HEAD, b"payload");
        let one = {
            let mut s = Spool::open(probe.path(), ROOMY_CAP).unwrap();
            s.append(&r).unwrap();
            s.size()
        };
        // Cap at exactly two records: the 2nd append fills to max and MUST be
        // accepted (bytes + frame == max, not >). The `+ -> *` mutant computes
        // bytes * frame, which exceeds max and would wrongly reject.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), one * 2.0).unwrap();
        check!(s.append(&r).unwrap());
        check!(s.append(&r).unwrap());
        check!(s.count() == 2);
    }

    #[test]
    fn size_reports_the_bytes_actually_on_disk() {
        // Anchors the `SpoolBytes` -> `ByteSize` accessor to the real file
        // length, so a scale slip at that seam cannot hide behind a matching
        // slip at the `open` seam.
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        s.append(&chained_record(0, &GENESIS_HEAD, b"payload"))
            .unwrap();
        let on_disk = std::fs::metadata(dir.path().join(SPOOL_FILE))
            .unwrap()
            .len();
        check!(s.size() == ByteSize::from_bytes(on_disk - header_len_u64()));
    }

    #[test]
    fn open_heals_torn_tail_frame() {
        use std::io::Write;
        let (dir, r0, spool) = seeded_spool(b"good");
        drop(spool);

        // Simulate a crash mid-append: a length prefix claiming 100 bytes, only 3 follow.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(dir.path().join("audit.spool"))
                .unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
        }
        // Reopen heals the torn tail; the good record survives and appends continue contiguously.
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        assert2::check!((s.count().0, s.read_all().unwrap()) == (1, vec![r0.clone()]));
        let r1 = chained_record(1, &GENESIS_HEAD, b"more");
        assert2::check!(s.append(&r1).unwrap());
        assert2::check!(s.read_all().unwrap() == vec![r0, r1]);
    }

    /// `open` heals a torn tail and reports that it did. Opening an intact
    /// spool must report the opposite: healing one that was whole would mean
    /// cutting good bytes off the end.
    #[test]
    fn open_reports_whether_it_healed_a_torn_tail() {
        use std::io::Write;
        let (dir, _r0, spool) = seeded_spool(b"good");
        drop(spool);
        let spool_file = dir.path().join(SPOOL_FILE);

        let intact_len = std::fs::metadata(&spool_file).unwrap().len();
        {
            let intact = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            check!(!intact.recovered_torn_tail());
        }
        check!(std::fs::metadata(&spool_file).unwrap().len() == intact_len);

        // A crash mid-append: a length prefix promising 100 bytes, 3 behind it.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&spool_file)
                .unwrap();
            f.write_all(&100u32.to_be_bytes()).unwrap();
            f.write_all(b"abc").unwrap();
        }
        let healed = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(healed.recovered_torn_tail());
        check!(std::fs::metadata(&spool_file).unwrap().len() == intact_len);
    }

    #[test]
    fn reopen_recovers_existing_contents() {
        let dir = tempfile::tempdir().unwrap();
        let r0 = chained_record(0, &GENESIS_HEAD, b"x");
        {
            let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            s.append(&r0).unwrap();
        }
        let s2 = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!((s2.count().0, s2.read_all().unwrap()) == (1, vec![r0]));
    }

    #[test]
    fn cadence_and_explicit_sync_reset_the_unsynced_counter() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool =
            Spool::open_with_sync_every(dir.path(), ROOMY_CAP, NonZeroU64::new(2).unwrap())
                .unwrap();
        check!(
            spool
                .append(&chained_record(0, &GENESIS_HEAD, b"a"))
                .unwrap()
        );
        check!(spool.unsynced == 1);
        check!(
            spool
                .append(&chained_record(1, &GENESIS_HEAD, b"b"))
                .unwrap()
        );
        check!(spool.unsynced == 0);
        check!(
            spool
                .append(&chained_record(2, &GENESIS_HEAD, b"c"))
                .unwrap()
        );
        check!(spool.unsynced == 1);
        spool.sync().unwrap();
        check!(spool.unsynced == 0);
    }

    #[test]
    fn acknowledged_record_survives_child_crash_and_torn_tail() {
        const CHILD_DIR: &str = "KRABKA_AUDIT_SPOOL_CRASH_DIR";
        if let Some(dir) = std::env::var_os(CHILD_DIR) {
            let dir = PathBuf::from(dir);
            let mut spool = Spool::open(&dir, ROOMY_CAP).unwrap();
            let record = chained_record(0, &GENESIS_HEAD, b"acknowledged");
            check!(spool.append(&record).unwrap());
            drop(spool);

            // Model a process dying after the next frame's prefix and only a
            // fragment of its body reached the file.
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.join(SPOOL_FILE))
                .unwrap();
            file.write_all(&100u32.to_be_bytes()).unwrap();
            file.write_all(b"torn").unwrap();
            std::process::abort();
        }

        let dir = tempfile::tempdir().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("acknowledged_record_survives_child_crash_and_torn_tail")
            .env(CHILD_DIR, dir.path())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        check!(!status.success());

        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let record = chained_record(0, &GENESIS_HEAD, b"acknowledged");
        check!(spool.recovered_torn_tail());
        check!(spool.read_all().unwrap() == vec![record]);
    }

    #[test]
    fn pending_loss_survives_reopen_and_reconciles_a_durable_marker() {
        let (dir, spool, losses) = spool_with_losses(ByteSize::from_bytes(0), 3);
        losses.persist().unwrap();
        let batch = losses.snapshot().unwrap();
        drop(losses);
        drop(spool);

        let mut reopened = Spool::open(dir.path(), ByteSize::from_bytes(0)).unwrap();
        check!(reopened.pending_losses().count() == 3);
        let mut marker = AuditRecord::records_lost(batch.count, batch.generation);
        marker.push_chain_headers(0, &GENESIS_HEAD);
        check!(reopened.append_loss_marker(&marker).is_err());
        check!(reopened.size() == ByteSize::ZERO);
        drop(reopened);

        let mut reopened = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        reopened.append_loss_marker(&marker).unwrap();
        // Model a crash after the marker fsync but before clearing the sidecar.
        drop(reopened);

        let reopened = Spool::open(dir.path(), ByteSize::from_bytes(0)).unwrap();
        check!(reopened.pending_losses().count() == 0);
        check!(reopened.read_all().unwrap() == vec![marker]);
    }

    #[test]
    fn uncertain_replay_poison_survives_reopen_until_record_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let record = chained_record(0, &GENESIS_HEAD, b"uncertain");
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(spool.append(&record).unwrap());
        spool.begin_replay(&record).unwrap();

        check!(matches!(
            Spool::open(dir.path(), ROOMY_CAP),
            Err(AuditError::Poisoned(_))
        ));

        spool.commit_replay(&record).unwrap();
        drop(spool);
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(spool.is_empty());
        check!(!dir.path().join(REPLAY_POISON_FILE).exists());
    }

    #[test]
    fn commit_and_persist_pending_losses() {
        let (dir, spool, losses) = spool_with_losses(ROOMY_CAP, 5);
        losses.persist().unwrap();
        let batch = losses.snapshot().unwrap();
        let wrong_batch = AuditLosses {
            generation: batch.generation + 1,
            count: batch.count,
        };
        losses.commit(wrong_batch);
        check!(losses.count() == 5);

        losses.commit(batch);
        check!(losses.count() == 0);

        let reopened = reopen_after_losses(dir.path(), spool, losses);
        check!(reopened.pending_losses().count() == 0);
    }

    #[test]
    fn reconcile_requires_matching_class_and_generation() {
        let (_dir, _spool, losses) = spool_with_losses(ROOMY_CAP, 4);
        let generation = losses.snapshot().unwrap().generation;

        let payload =
            serde_json::to_vec(&serde_json::json!({ "loss_generation": generation })).unwrap();
        let app_event = chained_record(0, &GENESIS_HEAD, &payload);
        losses.reconcile(&[app_event]);
        check!(losses.count() == 4);

        let wrong_gen_marker = AuditRecord::records_lost(4, generation + 1);
        losses.reconcile(&[wrong_gen_marker]);
        check!(losses.count() == 4);

        let matching_marker = AuditRecord::records_lost(4, generation);
        losses.reconcile(&[matching_marker]);
        check!(losses.count() == 0);
    }

    /// A loss `AuditHandle::emit` adds while the writer's marker is in flight
    /// is neither settled by that marker nor left in its generation.
    #[test]
    fn commit_carries_a_concurrent_loss_into_a_fresh_generation() {
        let (dir, spool, losses) = spool_with_losses(ROOMY_CAP, 3);
        let batch = losses.persist_with(Ok).unwrap().unwrap();
        check!(
            losses.state()
                == AuditLosses {
                    generation: batch.generation,
                    count: 0,
                }
        );

        losses.add(2);
        let batch = losses.snapshot().unwrap();
        losses.persist().unwrap();
        losses.add(1);
        losses.commit(batch);
        let carried = AuditLosses {
            generation: batch.generation + 1,
            count: 1,
        };
        check!(losses.state() == carried);
        check!(losses.snapshot() == Some(carried));

        let reopened = reopen_after_losses(dir.path(), spool, losses);
        check!(reopened.pending_losses().state() == carried);
    }

    /// Crash between the marker's fsync and the commit, with a concurrent loss
    /// already in the sidecar: reconciliation settles the marker's count only,
    /// and a later reopen does not settle the remainder a second time.
    #[test]
    fn reconcile_settles_only_the_markers_count() {
        let (dir, mut spool, losses) = spool_with_losses(ROOMY_CAP, 3);
        let batch = losses.snapshot().unwrap();
        losses.persist().unwrap();
        losses.add(1);
        losses.persist().unwrap();
        let mut marker = AuditRecord::records_lost(batch.count, batch.generation);
        marker.push_chain_headers(0, &GENESIS_HEAD);
        spool.append_loss_marker(&marker).unwrap();
        drop(losses);
        drop(spool);

        let carried = AuditLosses {
            generation: batch.generation + 1,
            count: 1,
        };
        for _ in 0..2 {
            let reopened = Spool::open(dir.path(), ROOMY_CAP).unwrap();
            check!(reopened.pending_losses().state() == carried);
            check!(reopened.read_all().unwrap() == vec![marker.clone()]);
        }
    }

    #[cfg(unix)]
    fn is_root() -> bool {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if let Some(rest) = line.strip_prefix("Uid:") {
                    let fields: Vec<&str> = rest.split_whitespace().collect();
                    if let Some(euid) = fields.get(1) {
                        return *euid == "0";
                    }
                }
            }
        }
        false
    }

    #[test]
    #[cfg(unix)]
    fn read_or_create_u64_propagates_non_not_found_error() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forbidden");
        std::fs::write(&path, [0u8; 8]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if is_root() || std::fs::File::open(&path).is_ok() {
            return;
        }
        let result = read_or_create_u64(&path, "tmp");
        check!(matches!(result, Err(AuditError::Io(_))));
    }

    #[test]
    fn abort_replay_clears_poison_and_clear_poison_handles_absent() {
        let (dir, record, spool) = seeded_spool(b"data");

        check!(spool.clear_replay_poison().is_ok());

        spool.begin_replay(&record).unwrap();
        check!(dir.path().join(REPLAY_POISON_FILE).exists());

        spool.abort_replay().unwrap();
        check!(!dir.path().join(REPLAY_POISON_FILE).exists());
    }

    #[test]
    fn reconcile_replay_poison_detects_torn_frame() {
        let dir = tempfile::tempdir().unwrap();
        let poison_path = dir.path().join(REPLAY_POISON_FILE);
        let mut corrupted = file_header().to_vec();
        corrupted.extend_from_slice(&0u64.to_be_bytes());
        corrupted.extend_from_slice(&100u32.to_be_bytes());
        corrupted.extend_from_slice(b"abcd");
        std::fs::write(&poison_path, &corrupted).unwrap();

        let replay_offset_path = dir.path().join(REPLAY_OFFSET_FILE);
        std::fs::write(&replay_offset_path, with_header(&100u64.to_be_bytes())).unwrap();

        let res = Spool::open(dir.path(), ROOMY_CAP);
        check!(matches!(res, Err(AuditError::Poisoned(_))));
    }

    #[test]
    fn replayed_spool_is_truncated_on_reopen() {
        let (dir, record, mut spool) = seeded_spool(b"hello");
        spool.commit_replay(&record).unwrap();
        drop(spool);

        let reopened = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(reopened.size() == ByteSize::ZERO);
        check_empty_file(dir.path());
    }

    #[test]
    fn unread_records_rejects_replay_offset_past_valid_bytes() {
        let (dir, _record, spool) = seeded_spool(b"valid");
        let valid_len = spool.size().bytes_u64();
        drop(spool);

        let replay_offset_path = dir.path().join(REPLAY_OFFSET_FILE);
        std::fs::write(
            &replay_offset_path,
            with_header(&(valid_len + 100).to_be_bytes()),
        )
        .unwrap();

        let res = Spool::open(dir.path(), ROOMY_CAP);
        check!(matches!(res, Err(AuditError::Io(_))));
    }

    #[test]
    #[cfg(unix)]
    fn clear_replay_poison_propagates_permission_denied() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, record, spool) = seeded_spool(b"data");
        spool.begin_replay(&record).unwrap();

        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        if is_root() || std::fs::write(dir.path().join("probe"), b"probe").is_ok() {
            let _ = std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755));
            let _ = std::fs::remove_file(dir.path().join("probe"));
            return;
        }
        let res = spool.clear_replay_poison();
        let _ = std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755));
        check!(matches!(res, Err(AuditError::Io(_))));
    }

    #[test]
    #[cfg(unix)]
    fn reconcile_replay_poison_propagates_read_error() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let poison_path = dir.path().join(REPLAY_POISON_FILE);
        std::fs::write(&poison_path, [0u8; 16]).unwrap();
        std::fs::set_permissions(&poison_path, std::fs::Permissions::from_mode(0o000)).unwrap();
        if is_root() || std::fs::File::open(&poison_path).is_ok() {
            let _ = std::fs::set_permissions(&poison_path, std::fs::Permissions::from_mode(0o644));
            return;
        }

        let res = Spool::open(dir.path(), ROOMY_CAP);
        let _ = std::fs::set_permissions(&poison_path, std::fs::Permissions::from_mode(0o644));
        check!(matches!(res, Err(AuditError::Io(_))));
    }

    /// `body` behind the current file header, as the spool writes it.
    fn with_header(body: &[u8]) -> Vec<u8> {
        let mut bytes = file_header().to_vec();
        bytes.extend_from_slice(body);
        bytes
    }

    /// One record, frame by frame, as `audit.spool` holds it. These bytes are
    /// the 1.x on-disk format: a change to them breaks every 1.x spool.
    const GOLDEN_SPOOL: &[u8] = &[
        b'K', b'A', b'U', b'D', 0x00, 0x00, // magic, version 0
        0x00, 0x00, 0x00, 0x16, // frame length 22
        0x03, // class tag: application_lifecycle
        0x00, 0x00, 0x00, 0x02, b'{', b'}', // value
        0x00, 0x00, 0x00, 0x01, // one header
        0x00, 0x00, 0x00, 0x01, b'k', // key
        0x00, 0x00, 0x00, 0x02, b'v', b'1', // value
    ];

    fn golden_record() -> AuditRecord {
        AuditRecord {
            class: AuditEventClass::ApplicationLifecycle,
            value: b"{}".to_vec(),
            headers: vec![("k".into(), b"v1".to_vec())],
        }
    }

    fn spooled_golden_record() -> (tempfile::TempDir, Spool) {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(spool.append(&golden_record()).unwrap());
        (dir, spool)
    }

    #[test]
    fn spool_file_is_the_golden_bytes() {
        let (dir, _spool) = spooled_golden_record();

        check!(std::fs::read(dir.path().join(SPOOL_FILE)).unwrap() == GOLDEN_SPOOL);
    }

    #[test]
    fn golden_spool_file_reads_back_as_its_record() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(SPOOL_FILE), GOLDEN_SPOOL).unwrap();
        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();

        check!(
            (spool.recovered_torn_tail(), spool.read_all().unwrap())
                == (false, vec![golden_record()])
        );
    }

    /// The sidecars, as a spool that has spooled the golden record, started
    /// its replay and lost three records in generation 1 leaves them.
    #[test]
    fn sidecar_files_are_the_golden_bytes() {
        let (dir, spool) = spooled_golden_record();
        spool.pending_losses().add(3);
        spool.pending_losses().persist().unwrap();
        spool.begin_replay(&golden_record()).unwrap();

        let header: &[u8] = &[b'K', b'A', b'U', b'D', 0x00, 0x00];
        let cases: [(&str, Vec<u8>); 3] = [
            (
                LOSS_STATE_FILE,
                [header, &1u64.to_be_bytes(), &3u64.to_be_bytes()].concat(),
            ),
            (REPLAY_OFFSET_FILE, [header, &0u64.to_be_bytes()].concat()),
            (
                REPLAY_POISON_FILE,
                [header, &0u64.to_be_bytes(), &GOLDEN_SPOOL[6..]].concat(),
            ),
        ];
        for (file, want) in cases {
            check!(
                std::fs::read(dir.path().join(file)).unwrap() == want,
                "{file}"
            );
        }
    }

    /// The sidecars read back: the loss state and replay offset come back
    /// whole, and a poison file left behind by a replay the offset already
    /// passed is cleared.
    #[test]
    fn golden_sidecar_files_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let header: &[u8] = &[b'K', b'A', b'U', b'D', 0x00, 0x00];
        let frame_len = u64::try_from(GOLDEN_SPOOL.len() - 6).unwrap();
        let golden_two = [GOLDEN_SPOOL, &GOLDEN_SPOOL[6..]].concat();
        std::fs::write(dir.path().join(SPOOL_FILE), &golden_two).unwrap();
        std::fs::write(
            dir.path().join(LOSS_STATE_FILE),
            [header, &1u64.to_be_bytes(), &3u64.to_be_bytes()].concat(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(REPLAY_OFFSET_FILE),
            [header, &frame_len.to_be_bytes()].concat(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(REPLAY_POISON_FILE),
            [header, &0u64.to_be_bytes(), &GOLDEN_SPOOL[6..]].concat(),
        )
        .unwrap();

        let spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        check!(
            (
                spool.pending_losses().state(),
                spool.replay_offset,
                spool.read_all().unwrap(),
                dir.path().join(REPLAY_POISON_FILE).exists(),
            ) == (
                AuditLosses {
                    generation: 1,
                    count: 3,
                },
                frame_len,
                vec![golden_record()],
                false,
            )
        );
    }

    /// Every file in the spool directory refuses a header it does not know:
    /// none at all (the 0.x layout, which started straight with the data) is
    /// `found: None`, and a version other than 0 is named.
    #[test]
    fn open_refuses_a_missing_or_unknown_file_header() {
        let v0_frame = &GOLDEN_SPOOL[6..];
        let future: &[u8] = &[b'K', b'A', b'U', b'D', 0x00, 0x01];
        let negative: &[u8] = &[b'K', b'A', b'U', b'D', 0xff, 0xff];
        // (file, contents, version the error must report)
        let cases: Vec<(&str, Vec<u8>, Option<i16>)> = vec![
            (SPOOL_FILE, v0_frame.to_vec(), None),
            (SPOOL_FILE, Vec::new(), None),
            (SPOOL_FILE, b"KAU".to_vec(), None),
            (SPOOL_FILE, [future, v0_frame].concat(), Some(1)),
            (SPOOL_FILE, negative.to_vec(), Some(-1)),
            (LOSS_STATE_FILE, [0u8; 16].to_vec(), None),
            (LOSS_STATE_FILE, [future, &[0u8; 16]].concat(), Some(1)),
            (REPLAY_OFFSET_FILE, [0u8; 8].to_vec(), None),
            (REPLAY_OFFSET_FILE, [future, &[0u8; 8]].concat(), Some(1)),
            (
                REPLAY_POISON_FILE,
                [&0u64.to_be_bytes()[..], v0_frame].concat(),
                None,
            ),
            (
                REPLAY_POISON_FILE,
                [future, &0u64.to_be_bytes(), v0_frame].concat(),
                Some(1),
            ),
        ];
        for (file, contents, found) in cases {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(file), &contents).unwrap();
            let got = match Spool::open(dir.path(), ROOMY_CAP) {
                Err(AuditError::UnsupportedSpoolFormat { file, found }) => Some((file, found)),
                _ => None,
            };
            check!(
                got == Some((file.to_owned(), found)),
                "{file} {contents:x?}"
            );
        }
    }

    #[test]
    fn unsupported_spool_format_names_the_file_and_the_version() {
        let cases = [
            (
                None,
                "audit spool file audit.spool: has no version header; it predates krabka 1.0, \
                 so the audit spool directory must be emptied (the node reformatted) before \
                 this broker starts",
            ),
            (
                Some(3),
                "audit spool file audit.spool: has version 3, which this build does not read",
            ),
        ];
        for (found, want) in cases {
            let error = AuditError::UnsupportedSpoolFormat {
                file: SPOOL_FILE.into(),
                found,
            };
            check!(error.to_string() == want);
        }
    }
}
