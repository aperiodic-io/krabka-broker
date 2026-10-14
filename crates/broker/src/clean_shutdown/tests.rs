//! The clean-shutdown proof: what a graceful stop leaves, what a crash leaves,
//! and how a controller reads either one.

use {
    super::{
        FILE_NAME, FORMAT_VERSION, ProofError, UNPROVEN, decode, encode, restart_was_clean, take,
        write,
    },
    crate::test_support::broker_registration,
    assert2::assert,
    krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, NodeId},
};

const NODE: NodeId = NodeId(2);

/// An image holding one registration for [`NODE`] at `broker_epoch`.
fn image_registering_node_at(broker_epoch: i64) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            broker_epoch,
            incarnation_id: uuid::Uuid::from_u128(7),
            host: "broker-2".into(),
            endpoints: vec![crate::test_support::plaintext_broker_endpoint(
                "broker-2", 9092,
            )],
            log_dirs: vec![uuid::Uuid::from_u128(11)],
            ..broker_registration(NODE)
        },
    ));
    image
}

/// The crash case, and the default the whole feature rests on: a log dir that
/// never held a proof cannot read as a clean restart against *any* broker
/// epoch, including the lowest one a broker can hold.
#[test]
fn a_log_dir_with_no_proof_never_reads_as_a_clean_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let image = image_registering_node_at(0);

    assert!(!restart_was_clean(&image, NODE, take(dir.path())));
}

/// A graceful stop's epoch comes back to the next start.
#[test]
fn a_written_proof_comes_back_to_the_next_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    write(dir.path(), 4242);

    assert!(take(dir.path()) == 4242);
}

/// The proof covers one restart. A broker that starts and then dies has
/// already spent it, so the start after that finds nothing.
#[test]
fn a_proof_is_spent_by_the_start_that_reads_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    write(dir.path(), 17);

    assert!(take(dir.path()) == 17);
    assert!(take(dir.path()) == UNPROVEN);
}

/// The exact bytes Kafka 4.x's `CleanShutdownFileHandler.write(42)` leaves in
/// `.kafka_cleanshutdown`, and krabka leaves in the same file. This pins the
/// on-disk format: a change to it is a change to the 1.x contract.
const GOLDEN: &str = r#"{"version":0,"brokerEpoch":42}"#;

#[test]
fn the_proof_holds_kafkas_bytes() {
    let dir = tempfile::tempdir().expect("temp dir");
    write(dir.path(), 42);

    let on_disk = std::fs::read_to_string(dir.path().join(".kafka_cleanshutdown"))
        .expect("the proof is at Kafka's file name");
    assert!(
        (FILE_NAME, encode(42), on_disk)
            == (".kafka_cleanshutdown", GOLDEN.to_owned(), GOLDEN.to_owned())
    );
}

/// Every document the reader meets, and what it makes of it. Kafka's reader
/// ignores unknown fields, so krabka's does too.
#[yare::parameterized(
    golden_document = { GOLDEN, Ok(42) },
    fields_reordered = { r#"{"brokerEpoch":42,"version":0}"#, Ok(42) },
    unknown_field_ignored = { r#"{"version":0,"brokerEpoch":42,"extra":true}"#, Ok(42) },
    unknown_positive_version = {
        r#"{"version":1,"brokerEpoch":42}"#,
        Err(ProofError::UnknownVersion { found: 1 }),
    },
    unknown_negative_version = {
        r#"{"version":-1,"brokerEpoch":42}"#,
        Err(ProofError::UnknownVersion { found: -1 }),
    },
)]
fn decode_reads_only_version_zero_documents(text: &str, want: Result<i64, ProofError>) {
    assert2::assert!(decode(text) == want, "decode({text})");
}

#[test]
fn the_format_version_is_zero() {
    assert!(FORMAT_VERSION == 0);
}

/// Text that is not the document is malformed: the 0.x layout (a bare
/// decimal epoch), a document with no version, one with no epoch, and junk.
#[yare::parameterized(
    bare_epoch = { "42" },
    missing_version = { r#"{"brokerEpoch":42}"# },
    missing_epoch = { r#"{"version":0}"# },
    null_epoch = { r#"{"version":0,"brokerEpoch":null}"# },
    junk = { "not-an-epoch" },
    empty = { "" },
)]
fn decode_refuses_documents_that_are_not_kafkas(text: &str) {
    assert2::assert!(
        let Err(ProofError::Malformed(_)) = decode(text),
        "decode({text:?})"
    );
}

/// A file that is not a proof -- of an unknown version, in the 0.x layout,
/// or junk -- makes the restart unclean, and does not stay behind for a later
/// start to retry. Kafka's `CleanShutdownFileHandler.read` answers every such
/// file the same way.
#[yare::parameterized(
    unknown_version = { r#"{"version":1,"brokerEpoch":42}"# },
    legacy_epoch = { "42" },
    junk = { "not-an-epoch" },
)]
fn an_unreadable_proof_is_unproven_and_still_spent(text: &str) {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(dir.path().join(FILE_NAME), text).expect("seed file");

    assert2::assert!(take(dir.path()) == UNPROVEN, "take over {text:?}");
    assert2::assert!(!dir.path().join(FILE_NAME).exists(), "{text:?} was spent");
}

/// The controller's rule: the offered epoch has to be the epoch the cluster
/// still holds. Anything else -- a stale epoch, the unproven sentinel, or no
/// registration at all -- is an unclean restart.
#[test]
fn only_the_held_epoch_proves_a_clean_restart() {
    let image = image_registering_node_at(90);

    assert!(restart_was_clean(&image, NODE, 90));
    assert!(!restart_was_clean(&image, NODE, 89));
    assert!(!restart_was_clean(&image, NODE, UNPROVEN));
    assert!(!restart_was_clean(
        &MetadataImage::new(uuid::Uuid::nil()),
        NODE,
        90
    ));
}

#[test]
fn unproven_constant_is_negative_one() {
    assert!(UNPROVEN == -1);
}
