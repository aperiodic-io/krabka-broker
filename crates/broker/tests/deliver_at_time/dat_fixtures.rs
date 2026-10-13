//! The vocabulary the KFC-1 cases are written in: the two delivery modes, the
//! `Visible` snapshot of what a consumer can see, the wall-clock reading, the
//! batch builder, and the timing constants that keep every assertion off a
//! race.
//!
//! These are the values a case reasons about rather than the requests it
//! sends, which is why they live apart from the wire helpers in `dat_wire`.

use {
    crate::support::records::value_record,
    bytes::Bytes,
    krabka_log::DeliveryPolicy,
    krabka_protocol::records::RecordBatch,
    qubit_clock::{StdWallClock, WallClock as _},
};

/// How far ahead of produce time a record that must activate during a case is
/// stamped.
///
/// It is long enough that the read taken right after the produce is
/// unambiguously before the delivery time on a loaded machine, and short enough
/// that a case does not spend long waiting for it.
pub const ACTIVATION_DELAY_MS: i64 = 2_000;

/// How far ahead a record that must stay pending for a whole case is stamped.
/// One hour: no run of this suite comes near it.
pub const PENDING_HORIZON_MS: i64 = 3_600_000;

/// How long a delivery time is allowed to sit in the past for a record that
/// must be due the moment it is produced.
pub const ALREADY_DUE_MS: i64 = 60_000;

/// One delivery mode: the `delivery.mode` value that selects it, and the
/// [`DeliveryPolicy`] the partition's log reports once the topic config has
/// reached it.
#[derive(Debug, Clone, Copy)]
pub struct Mode {
    pub value: &'static str,
    pub policy: DeliveryPolicy,
}

pub const IMMEDIATE: Mode = Mode {
    value: "immediate",
    policy: DeliveryPolicy::Immediate,
};

pub const SCHEDULED: Mode = Mode {
    value: "scheduled",
    policy: DeliveryPolicy::Scheduled,
};

/// Everything a consumer can learn about one partition without a group: the
/// offset `ListOffsets` LATEST reports, which is where a seek-to-end lands, and
/// the record values a fetch from the start of the log serves.
#[derive(Debug, PartialEq, Eq)]
pub struct Visible {
    pub latest: i64,
    pub values: Vec<String>,
}

impl Visible {
    pub fn of(latest: i64, values: &[&str]) -> Self {
        Self {
            latest,
            values: values.iter().map(|value| (*value).to_owned()).collect(),
        }
    }
}

/// The wall-clock reading a case's expectations are written against, in
/// milliseconds since the Unix epoch.
///
/// It reads the same production wall clock the broker's delivery path does, so
/// a delivery time this returns plus `ACTIVATION_DELAY_MS` means the same thing
/// on both sides of the wire.
pub fn now_ms() -> i64 {
    StdWallClock::new()
        .now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since_epoch| {
            i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)
        })
}

// One batch whose delivery time — its `max_timestamp` — is `delivery_ms`, with
// one record per entry of `values`.
pub fn batch_at(delivery_ms: i64, values: &[&str]) -> RecordBatch {
    let count = i32::try_from(values.len()).expect("a test batch is small");
    let mut batch = RecordBatch {
        base_timestamp: delivery_ms,
        max_timestamp: delivery_ms,
        last_offset_delta: count - 1,
        ..RecordBatch::default()
    };
    for (index, value) in values.iter().enumerate() {
        batch.records.push(value_record(
            i32::try_from(index).expect("a test batch is small"),
            Some(Bytes::from((*value).to_owned())),
        ));
    }
    batch
}
