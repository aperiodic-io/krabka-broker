//! Docker-gated: the JVM `kafka-dump-log --cluster-metadata-decoder` parses a
//! Krabka-built `bootstrap.checkpoint` cleanly.
//!
//! ```text
//! cargo test -p krabka-broker --test kraft_checkpoint_jvm -- --ignored --nocapture
//! ```

use assert2::assert;
use krabka_protocol::records::metadata::checkpoint::build_bootstrap_checkpoint;

const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";

krabka_macros::jvm_checkpoint_dump_fixture!(dump_checkpoint);

#[test]
#[ignore = "requires Docker"]
fn jvm_dump_log_parses_krabka_bootstrap_checkpoint() {
    let bytes = build_bootstrap_checkpoint(&[
        ("metadata.version", 25),
        ("group.version", 1),
        ("transaction.version", 2),
    ]);
    let dir = tempfile::tempdir().expect("tempdir");
    // kafka-dump-log infers the base offset from the file name; a real
    // bootstrap.checkpoint is named `bootstrap.checkpoint`.
    let out = dump_checkpoint(dir.path(), "bootstrap.checkpoint", &bytes, KAFKA_IMAGE);
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!("{text}");
    assert!(out.status.success(), "kafka-dump-log must succeed: {text}");
    // (needle, expected presence in the dump-log output)
    let cases = [
        ("SnapshotHeader", true),
        ("FEATURE_LEVEL_RECORD", true),
        ("metadata.version", true),
        ("SnapshotFooter", true),
        // `isvalid: false` would mean a batch failed CRC validation.
        ("isvalid: false", false),
    ];
    for (needle, expected) in cases {
        assert!(
            text.contains(needle) == expected,
            "kafka-dump-log output must {} {needle:?}: {text}",
            if expected { "contain" } else { "not contain" },
        );
    }
}
