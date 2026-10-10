//! The acknowledged ledger: how it is produced, and the two properties it
//! must have before any fault is injected.
//!
//! Two producers append to the same partition at the same time, with
//! `acks=all` and idempotence on. What comes back is a history of concurrent
//! operations, and the suite checks it twice.
//!
//! [`assert_acked_ledger`] is the structural check: the offsets the brokers
//! handed out are gap-free from zero, and no value was acknowledged twice.
//!
//! [`assert_linearizable_history`] is the semantic one. A Kafka partition is a
//! log, so every acknowledged append must be explainable by *some* total order
//! that respects real time: an operation that returned before another was
//! invoked has to come first in that order. `stateright`'s
//! [`LinearizabilityTester`] searches for such an order against
//! [`KafkaLogSpec`], which is the log as a sequential object. A history where
//! two overlapping appends both got offset 4, or where a later append got an
//! earlier offset than one that had already returned, has no such order.
//!
//! The invoke and return instants come from one shared counter rather than
//! from the wall clock, because the ordering the checker needs is a happens
//! before relation, and a counter states it exactly.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use bytes::Bytes;
use krabka_client_producer::{Acks, Producer};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};

use crate::{APPENDERS, RECORDS_PER_APPENDER, TOPIC};

/// One append, identified by the bytes it carries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct AppendOp(Vec<u8>);

/// The offset the broker acknowledged that append at.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct AppendRet(i64);

/// A Kafka partition as a sequential object: an append returns the index it
/// landed at, and the log only grows.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
struct KafkaLogSpec {
    values: Vec<Vec<u8>>,
}

impl SequentialSpec for KafkaLogSpec {
    type Op = AppendOp;
    type Ret = AppendRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        let offset = i64::try_from(self.values.len()).expect("test history fits i64");
        self.values.push(op.0.clone());
        AppendRet(offset)
    }
}

/// One acknowledged record, with the two counter readings that place it in
/// real time relative to every other one.
#[derive(Debug)]
pub(crate) struct AckedRecord {
    /// The checker's process id. Each append gets its own, so no process ever
    /// has two operations in flight.
    client: u64,
    pub(crate) value: Vec<u8>,
    partition: i32,
    pub(crate) offset: i64,
    invoke_order: u64,
    return_order: u64,
}

#[derive(Debug)]
enum HistoryEvent<'a> {
    Invoke(&'a AckedRecord),
    Return(&'a AckedRecord),
}

/// Run `APPENDERS` producers against `bootstrap` at once and return every
/// acknowledged record, sorted by offset.
///
/// The appenders are joined rather than spawned. They share one clock and
/// interleave at every await point either way, and joining keeps the whole
/// history on the caller's task, so a panic inside one appender fails the case
/// where it happened.
pub(crate) async fn produce_concurrently(bootstrap: &str) -> Vec<AckedRecord> {
    let clock = Arc::new(AtomicU64::new(0));
    let appenders = (0..APPENDERS)
        .map(|appender| produce_appender(bootstrap.to_owned(), appender, Arc::clone(&clock)));
    let mut ledger: Vec<AckedRecord> = futures_util::future::join_all(appenders)
        .await
        .into_iter()
        .flatten()
        .collect();
    ledger.sort_unstable_by_key(|record| record.offset);
    ledger
}

async fn produce_appender(
    bootstrap: String,
    appender: u64,
    clock: Arc<AtomicU64>,
) -> Vec<AckedRecord> {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id(format!("diskless-jepsen-appender-{appender}"))
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(2))
        .build()
        .await
        .expect("producer build");

    let mut records =
        Vec::with_capacity(usize::try_from(RECORDS_PER_APPENDER).expect("record count fits usize"));
    for sequence in 0..RECORDS_PER_APPENDER {
        let client = appender * RECORDS_PER_APPENDER + sequence + 1;
        let value = format!("appender-{appender}-record-{sequence}").into_bytes();
        let invoke_order = clock.fetch_add(1, Ordering::SeqCst);
        let metadata = producer
            .send(crate::support::producer::producer_record(
                crate::support::producer::ProducerRecordSetup {
                    topic: (TOPIC).into(),
                    partition: Some(krabka_ids::PartitionIndex(0)),
                    value: Some(Bytes::copy_from_slice(&value)),
                    ..Default::default()
                },
            ))
            .await
            .expect("acks=all record");
        let return_order = clock.fetch_add(1, Ordering::SeqCst);
        records.push(AckedRecord {
            client,
            value,
            partition: metadata.partition,
            offset: metadata.offset,
            invoke_order,
            return_order,
        });
    }
    producer.flush().await.expect("producer flush");
    producer.close().await.expect("producer close");
    records
}

/// The offset one past the last acknowledged record: what every survivor must
/// hold before the acking broker is taken away.
pub(crate) fn durable_end(ledger: &[AckedRecord]) -> i64 {
    i64::try_from(ledger.len()).expect("ledger length fits i64")
}

/// The `(offset, value)` pairs the readback must reproduce.
pub(crate) fn ledger_values(ledger: &[AckedRecord]) -> Vec<(i64, Vec<u8>)> {
    ledger
        .iter()
        .map(|record| (record.offset, record.value.clone()))
        .collect()
}

/// Every append was acknowledged, at a distinct offset, with no gap.
pub(crate) fn assert_acked_ledger(ledger: &[AckedRecord]) {
    assert!(
        ledger.len()
            == usize::try_from(APPENDERS * RECORDS_PER_APPENDER).expect("record count fits usize")
    );
    for (expected_offset, record) in ledger.iter().enumerate() {
        assert!(record.partition == 0);
        assert!(
            record.offset == i64::try_from(expected_offset).expect("offset fits i64"),
            "acked ledger is not gap-free: {ledger:?}"
        );
    }
    let unique_values = ledger
        .iter()
        .map(|record| record.value.as_slice())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        unique_values.len() == ledger.len(),
        "duplicate ledger values"
    );
}

/// Some total order over the acknowledged appends explains every offset the
/// brokers returned, and respects the real-time order of the history.
pub(crate) fn assert_linearizable_history(ledger: &[AckedRecord]) {
    let mut events = ledger
        .iter()
        .flat_map(|record| {
            [
                (record.invoke_order, HistoryEvent::Invoke(record)),
                (record.return_order, HistoryEvent::Return(record)),
            ]
        })
        .collect::<Vec<_>>();
    events.sort_unstable_by_key(|(order, _)| *order);

    let mut checker = LinearizabilityTester::new(KafkaLogSpec::default());
    for (_, event) in events {
        match event {
            HistoryEvent::Invoke(record) => {
                checker
                    .on_invoke(record.client, AppendOp(record.value.clone()))
                    .expect("one in-flight operation per client");
            }
            HistoryEvent::Return(record) => {
                checker
                    .on_return(record.client, AppendRet(record.offset))
                    .expect("return matches invoke");
            }
        }
    }
    assert!(
        checker.serialized_history().is_some(),
        "acked producer history is not linearizable"
    );
}
