//! The header identity: with `dedup.key.header` set, the value of the first
//! header of that name is the identity, the Kafka key is ignored, and a record
//! without a usable header fails its whole batch with `INVALID_RECORD`.

use {
    crate::{
        harness::{Topic, appended_at, stored, topic},
        support::{
            self,
            dedup::{DEDUP_HEADER, Rec, StoredRecord},
        },
    },
    assert2::{assert, check},
    krabka_broker::codes,
    krabka_protocol::owned::produce_response::{
        BatchIndexAndErrorMessage, PartitionProduceResponse,
    },
};

/// A record in header mode: its Kafka key, its header value and its value.
fn tagged(key: &str, id: &str, value: &str) -> Rec { Rec::key(key).dedup_id(id).value(value) }

async fn header_topic<'a>(p: &'a support::InProcess, name: &'a str) -> Topic<'a> {
    topic()
        .p(p)
        .name(name)
        .key_header(DEDUP_HEADER)
        .call()
        .await
}

/// The header value is the identity: the same key with a new header value is
/// kept, a new key with a header value already seen is dropped, and a record
/// with a null key is deduplicated by its header.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_header_value_is_the_identity_and_the_key_is_ignored() {
    let p = support::start().await;
    let t = header_topic(&p, "by-header").await;

    let first = t.send(&[tagged("k1", "h1", "first")]).await;
    let second = t
        .send(&[tagged("k1", "h2", "second"), tagged("k2", "h1", "third")])
        .await;
    let third = t
        .send(&[tagged("k3", "h2", "fourth"), tagged("k1", "h3", "fifth")])
        .await;
    // A null key does not matter in header mode: the header is the identity.
    let keyless = t
        .send(&[
            Rec::keyless("sixth").dedup_id("h4"),
            Rec::keyless("seventh").dedup_id("h4"),
            Rec::keyless("eighth").dedup_id("h1"),
        ])
        .await;
    let contents = t.contents().await;

    assert!(
        [first, second, third, keyless]
            == [
                appended_at(0),
                appended_at(1),
                appended_at(3),
                appended_at(5)
            ]
    );
    assert!(
        contents.records
            == [
                stored(0, "k1", "first"),
                stored(1, "k1", "second"),
                stored(4, "k1", "fifth"),
                StoredRecord {
                    offset: 5,
                    key: None,
                    value: Some(b"sixth".to_vec()),
                },
            ]
    );
    assert!(contents.high_watermark == 8);
    p.broker.shutdown().await;
}

/// When the header repeats, its first occurrence is the identity, and headers
/// of other names are not read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_header_of_the_configured_name_wins() {
    let p = support::start().await;
    let t = header_topic(&p, "first-header").await;

    let repeated = Rec::key("k")
        .header("x-other", Some("h9"))
        .dedup_id("h1")
        .dedup_id("h2")
        .value("repeated");
    let rows = [
        t.send(&[repeated]).await,
        t.send(&[tagged("k", "h2", "second-occurrence")]).await,
        t.send(&[tagged("k", "h1", "first-occurrence")]).await,
        t.send(&[tagged("k", "h9", "other-name")]).await,
    ];
    let contents = t.contents().await;

    assert!(
        rows == [
            appended_at(0),
            appended_at(1),
            appended_at(2),
            appended_at(3)
        ]
    );
    assert!(
        contents.records
            == [
                stored(0, "k", "repeated"),
                stored(1, "k", "second-occurrence"),
                stored(3, "k", "other-name"),
            ]
    );
    p.broker.shutdown().await;
}

/// An empty header value is an identity like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_header_value_is_an_identity() {
    let p = support::start().await;
    let t = header_topic(&p, "empty-header").await;

    let first = t
        .send(&[tagged("k1", "", "first"), tagged("k2", "", "second")])
        .await;
    let again = t.send(&[tagged("k3", "", "third")]).await;
    let contents = t.contents().await;

    assert!([first, again] == [appended_at(0), appended_at(2)]);
    assert!(contents.records == [stored(0, "k1", "first")]);
    assert!(contents.high_watermark == 3);
    p.broker.shutdown().await;
}

/// The row Kafka's `LogValidator.processRecordErrors` answers when each record
/// at `indexes` has no value for the header.
fn refused(topic: &str, indexes: &[i32]) -> PartitionProduceResponse {
    let message = format!(
        "Record has no value for the header '{DEDUP_HEADER}' that dedup.key.header names in \
         topic partition {topic}-0"
    );
    let record_errors: Vec<BatchIndexAndErrorMessage> = indexes
        .iter()
        .map(|&batch_index| BatchIndexAndErrorMessage {
            batch_index_error_message: Some(message.clone()),
            batch_index,
            ..Default::default()
        })
        .collect();
    // Java's `List.toString` of at most the first three `RecordError`s.
    let shown = record_errors
        .iter()
        .take(3)
        .map(|error| {
            format!(
                "RecordError(batchIndex={}, message='{message}')",
                error.batch_index
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let total = record_errors.len();
    PartitionProduceResponse {
        error_message: Some(format!(
            "One or more records have been rejected due to {total} record errors in total, and \
             only showing the first three errors at most: [{shown}]"
        )),
        record_errors,
        error_code: codes::INVALID_RECORD,
        base_offset: -1,
        ..appended_at(0)
    }
}

/// A record with no header of the configured name, or with a null value for
/// it, has no identity. The whole batch is refused with one `record_errors`
/// entry per such record. Nothing is appended, and the identities of the
/// batch's other records are not indexed: sent again without the offending
/// records, every one of them is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_without_a_usable_header_fails_its_whole_batch() {
    let p = support::start().await;
    let missing = || Rec::key("k").value("missing");
    let null = || Rec::key("k").header(DEDUP_HEADER, None).value("null");
    let other_name = || Rec::key("k").header("x-other", Some("h1")).value("other");
    // The first header of the name counts, so a null one is not rescued by a
    // later one with a value.
    let null_then_value = || {
        Rec::key("k")
            .header(DEDUP_HEADER, None)
            .dedup_id("h9")
            .value("null-first")
    };
    let cases: [(&str, Vec<Rec>, &[i32]); 5] = [
        ("a missing header in the middle", vec![missing()], &[1]),
        ("a null header value", vec![null()], &[1]),
        ("only a header of another name", vec![other_name()], &[1]),
        (
            "a null value before a usable one",
            vec![null_then_value()],
            &[1],
        ),
        (
            "several offending records",
            vec![missing(), null(), other_name()],
            &[1, 3, 4],
        ),
    ];
    for (index, (name, offending, indexes)) in cases.into_iter().enumerate() {
        let topic_name = format!("refused-{index}");
        let t = header_topic(&p, &topic_name).await;
        let seeded = t.send(&[tagged("k", "seed", "seed")]).await;
        let batch = mixed_batch(&offending, indexes);

        let row = t.send(&batch).await;
        let unchanged = t.contents().await;
        let valid: Vec<Rec> = batch
            .iter()
            .zip(0..)
            .filter(|(_, at)| !indexes.contains(at))
            .map(|(rec, _)| rec.clone())
            .collect();
        let retry = t.send(&valid).await;
        let contents = t.contents().await;

        check!(seeded == appended_at(0), "{name}");
        check!(row == refused(&topic_name, indexes), "{name}");
        check!(unchanged.high_watermark == 1, "{name}");
        check!(unchanged.records == [stored(0, "k", "seed")], "{name}");
        check!(retry == appended_at(1), "{name}");
        let kept: Vec<StoredRecord> = valid
            .iter()
            .zip(1..)
            .map(|(rec, offset)| StoredRecord {
                offset,
                key: rec.key.clone(),
                value: Some(rec.value.clone()),
            })
            .collect();
        check!(contents.records[1..] == kept[..], "{name}");
    }
    p.broker.shutdown().await;
}

/// A batch whose records at `indexes` are `offending`, in order, and whose
/// other records carry fresh header values.
fn mixed_batch(offending: &[Rec], indexes: &[i32]) -> Vec<Rec> {
    let len = indexes.iter().max().map_or(0, |last| last + 1).max(3);
    let mut offending = offending.iter();
    (0..len)
        .map(|at| {
            if indexes.contains(&at) {
                offending
                    .next()
                    .expect("one offending record per index")
                    .clone()
            } else {
                tagged(&format!("k{at}"), &format!("fresh-{at}"), &format!("v{at}"))
            }
        })
        .collect()
}
