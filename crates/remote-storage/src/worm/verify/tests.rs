//! Behaviour tests for a verification run as a whole: the tamper matrix that
//! grades one attacker edit per row at both depths, and the reports a clean and
//! an empty archive produce.

use {
    super::*,
    crate::worm::{
        manifest::ManifestSeq,
        verify::test_support::{Archive, SEGMENT_SPAN, STRAY, Tamper},
    },
    assert2::check,
    object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path},
};

#[tokio::test]
async fn an_exact_externally_authenticated_object_is_not_an_orphan() {
    let archive = Archive::build(&[1]).await;
    Tamper::StrayObject.apply(&archive).await;
    let request = VerifyRequest {
        externally_authenticated_objects: std::collections::BTreeSet::from([format!(
            "{}/{STRAY}",
            archive.dir
        )]),
        ..Default::default()
    };

    let report = verify_archive(&archive.store, &request, &archive.trusted())
        .await
        .unwrap();
    check!(report.ok());
}

#[tokio::test]
async fn backup_capture_namespace_is_not_a_classic_partition() {
    let archive = Archive::build(&[1]).await;
    archive
        .store
        .put(
            &Path::from("archive/restore-inputs/0001/manifest.json"),
            PutPayload::from_static(b"capture"),
        )
        .await
        .unwrap();

    let report = archive.verify().await;
    check!(report.ok());
    check!(report.partitions.len() == 1);
    check!(report.global_orphan_objects.is_empty());
}

#[tokio::test]
async fn an_object_outside_partition_and_capture_directories_is_a_global_orphan() {
    let archive = Archive::build(&[1]).await;
    archive
        .store
        .put(
            &Path::from("archive/unclaimed.bin"),
            PutPayload::from_static(b"unclaimed"),
        )
        .await
        .unwrap();

    let report = archive.verify().await;
    check!(!report.ok());
    check!(report.global_orphan_objects == vec!["archive/unclaimed.bin"]);
}

/// The kind of break a row expects, matched against the reason text the
/// report carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Category {
    Size,
    Missing,
    Chain,
    Signature,
    ObjectSet,
    Format,
    Digest,
    Tip,
}

impl Category {
    fn matches(self, reason: &str) -> bool {
        match self {
            Category::Size => reason.contains("the manifest records a size of"),
            Category::Missing => reason.contains("is missing from the archive"),
            Category::Chain => {
                reason.contains("chain sequence gap") || reason.contains("chain head mismatch")
            }
            Category::Signature => {
                reason.contains("unsigned")
                    || reason.contains("untrusted")
                    || reason.contains("does not verify")
            }
            Category::ObjectSet => {
                reason.contains("not named by any manifest")
                    || reason.contains("named more than once")
                    || reason.contains("segment coordinates or suffix")
                    || reason.contains("archive holds")
                    || reason.contains("names no segment objects")
            }
            Category::Format => reason.contains("format version"),
            Category::Digest => reason.contains("hashes to"),
            Category::Tip => reason.contains("does not match the expected head"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Incomplete,
    Break(Category),
}

/// Which objects the row expects to find unaccounted for.
#[derive(Clone, Copy)]
enum Orphans {
    None,
    Stray,
    Object(usize, usize),
    /// Every object of one segment, which is what deleting its manifest
    /// leaves behind.
    Segment(usize),
}

impl Orphans {
    fn expected(self, archive: &Archive) -> Vec<String> {
        match self {
            Orphans::None => Vec::new(),
            Orphans::Stray => vec![format!("{}/{STRAY}", archive.dir)],
            Orphans::Object(segment, object) => {
                vec![archive.segments[segment].entries[object].key.clone()]
            }
            Orphans::Segment(i) => {
                let mut keys: Vec<String> = archive.segments[i]
                    .entries
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect();
                keys.sort();
                keys
            }
        }
    }
}

struct Row {
    name: &'static str,
    runs: &'static [usize],
    tamper: Tamper,
    /// Pass the untampered tip as [`VerifyRequest::expect_head`].
    expect_tip: bool,
    shallow: Outcome,
    deep: Outcome,
    unsigned: u64,
    untrusted: u64,
    /// Chain runs the report must describe, when the walk completed.
    epoch_spans: Option<usize>,
    orphans: Orphans,
}

const fn row(name: &'static str, tamper: Tamper, shallow: Outcome, deep: Outcome) -> Row {
    Row {
        name,
        runs: &[3],
        tamper,
        expect_tip: false,
        shallow,
        deep,
        unsigned: 0,
        untrusted: 0,
        epoch_spans: None,
        orphans: Orphans::None,
    }
}

fn tamper_matrix() -> Vec<Row> {
    vec![
        Row {
            epoch_spans: Some(1),
            ..row("clean", Tamper::None, Outcome::Ok, Outcome::Ok)
        },
        // The one row that proves `--deep` earns its keep: the body changed
        // but the size did not, so nothing short of a re-hash sees it.
        row(
            "flip one byte in a .log",
            Tamper::FlipLogByte(1),
            Outcome::Ok,
            Outcome::Break(Category::Digest),
        ),
        row(
            "truncate a .log",
            Tamper::TruncateLog(1),
            Outcome::Break(Category::Size),
            Outcome::Break(Category::Size),
        ),
        row(
            "delete a .log",
            Tamper::DeleteLog(1),
            Outcome::Break(Category::Missing),
            Outcome::Break(Category::Missing),
        ),
        row(
            "manifest re-signed with a wrong prev_head",
            Tamper::RewritePrevHead(1),
            Outcome::Break(Category::Chain),
            Outcome::Break(Category::Chain),
        ),
        Row {
            orphans: Orphans::Segment(2),
            ..row(
                "delete the newest manifest",
                Tamper::DeleteManifest(2),
                Outcome::Incomplete,
                Outcome::Incomplete,
            )
        },
        Row {
            expect_tip: true,
            orphans: Orphans::Segment(2),
            ..row(
                "delete the newest manifest, with an expected head",
                Tamper::DeleteManifest(2),
                Outcome::Break(Category::Tip),
                Outcome::Break(Category::Tip),
            )
        },
        Row {
            orphans: Orphans::Segment(1),
            ..row(
                "delete a middle manifest",
                Tamper::DeleteManifest(1),
                Outcome::Break(Category::Chain),
                Outcome::Break(Category::Chain),
            )
        },
        row(
            "manifest signed by a different key",
            Tamper::SignWithAnotherKey(1),
            Outcome::Break(Category::Signature),
            Outcome::Break(Category::Signature),
        ),
        Row {
            untrusted: 1,
            ..row(
                "unknown key_id",
                Tamper::SignWithUnknownKeyId(1),
                Outcome::Incomplete,
                Outcome::Incomplete,
            )
        },
        Row {
            unsigned: 1,
            ..row(
                "unsigned manifest",
                Tamper::Unsign(1),
                Outcome::Incomplete,
                Outcome::Incomplete,
            )
        },
        row(
            "embedded public key rewritten",
            Tamper::RewritePublicKey(1),
            Outcome::Break(Category::Signature),
            Outcome::Break(Category::Signature),
        ),
        row(
            "duplicate object entry",
            Tamper::DuplicateObject(1),
            Outcome::Break(Category::ObjectSet),
            Outcome::Break(Category::ObjectSet),
        ),
        row(
            "object suffix does not match its key",
            Tamper::WrongObjectCoordinates(1),
            Outcome::Break(Category::ObjectSet),
            Outcome::Break(Category::ObjectSet),
        ),
        Row {
            orphans: Orphans::Object(1, 1),
            ..row(
                "manifest drops one existing object",
                Tamper::DropObjectEntry(1),
                Outcome::Break(Category::ObjectSet),
                Outcome::Break(Category::ObjectSet),
            )
        },
        Row {
            runs: &[2, 2],
            epoch_spans: Some(2),
            ..row("two epochs", Tamper::None, Outcome::Ok, Outcome::Ok)
        },
        Row {
            epoch_spans: Some(1),
            orphans: Orphans::Stray,
            ..row(
                "stray object under the prefix",
                Tamper::StrayObject,
                Outcome::Incomplete,
                Outcome::Incomplete,
            )
        },
        Row {
            // A manifest the verifier will not accept names nothing, so
            // the objects it used to account for become orphans.
            orphans: Orphans::Segment(1),
            ..row(
                "format_version bumped",
                Tamper::BumpFormatVersion(1),
                Outcome::Break(Category::Format),
                Outcome::Break(Category::Format),
            )
        },
    ]
}

async fn run_row(row: &Row, depth: VerifyDepth, expected: Outcome) {
    let archive = Archive::build(row.runs).await;
    let tip = archive.tip();
    row.tamper.apply(&archive).await;

    let request = VerifyRequest {
        depth,
        expect_head: row.expect_tip.then_some(tip),
        ..Default::default()
    };
    let report = verify_archive(&archive.store, &request, &archive.trusted())
        .await
        .unwrap();
    let label = format!("{} / {depth:?}", row.name);
    check!(report.partitions.len() == 1, "{label}: one partition");
    let partition = &report.partitions[0];

    match expected {
        Outcome::Ok => {
            check!(
                partition.first_break.as_ref().map(|b| b.reason.as_str()) == None,
                "{label}"
            );
            check!(partition.ok, "{label}");
        }
        Outcome::Incomplete => {
            check!(
                partition.first_break.is_none(),
                "{label}: incomplete is not a chain break"
            );
            check!(!partition.ok, "{label}");
        }
        Outcome::Break(category) => {
            check!(!partition.ok, "{label}");
            match partition.first_break.as_ref() {
                Some(found) => {
                    check!(
                        category.matches(&found.reason),
                        "{label}: `{}` is not a {category:?} break",
                        found.reason
                    );
                }
                None => {
                    check!(false, "{label}: ok is false with no break recorded");
                }
            }
        }
    }
    check!(partition.unsigned_manifests == row.unsigned, "{label}");
    check!(partition.untrusted_manifests == row.untrusted, "{label}");
    check!(
        partition.orphan_objects == row.orphans.expected(&archive),
        "{label}"
    );
    if let Some(spans) = row.epoch_spans {
        check!(partition.epochs.len() == spans, "{label}");
    }
}

#[tokio::test]
async fn shallow_verify_grades_every_tamper() {
    for row in tamper_matrix() {
        run_row(&row, VerifyDepth::Shallow, row.shallow).await;
    }
}

#[tokio::test]
async fn deep_verify_grades_every_tamper() {
    for row in tamper_matrix() {
        run_row(&row, VerifyDepth::Deep, row.deep).await;
    }
}

#[tokio::test]
async fn verify_reports_a_clean_archive_in_full() {
    let archive = Archive::build(&[3]).await;
    let report = archive.verify().await;

    let last = &archive.segments[2].manifest.body;
    let expected = ArchiveVerifyReport {
        partitions: vec![PartitionVerifyReport {
            partition_dir: archive.dir.clone(),
            manifests: 3,
            objects_checked: 6,
            create_precondition_objects: ObjectProtectionReport::default(),
            bucket_retention_objects: ObjectProtectionReport {
                count: 6,
                sample: archive
                    .segments
                    .iter()
                    .flat_map(|segment| segment.entries.iter().map(|entry| entry.key.clone()))
                    .collect(),
            },
            epochs: vec![EpochSpan {
                epoch_id: last.chain.epoch_id,
                first_seq: ManifestSeq(0),
                last_seq: ManifestSeq(2),
                manifests: 3,
                start_offset: 0,
                end_offset: 3 * SEGMENT_SPAN - 1,
                head: archive.tip(),
            }],
            unsigned_manifests: 0,
            untrusted_manifests: 0,
            orphan_objects: Vec::new(),
            offset_gaps: Vec::new(),
            head: Some(archive.tip()),
            ok: true,
            first_break: None,
        }],
        global_orphan_objects: Vec::new(),
    };
    check!(report == expected);
    check!(report.ok());
    check!(report.manifests() == 3);
    check!(report.fully_attested());
    check!(!report.has_epoch_restarts());
    check!(report.first_break() == None);
}

#[tokio::test]
async fn authentication_returns_the_signed_object_claims() {
    let archive = Archive::build(&[2]).await;
    let authenticated = authenticate_archive(
        &archive.store,
        &VerifyRequest::default(),
        &archive.trusted(),
    )
    .await
    .unwrap();

    check!(authenticated.report().ok());
    check!(authenticated.report().fully_attested());
    check!(authenticated.objects().len() == 4);
    for segment in &archive.segments {
        for entry in &segment.entries {
            check!(authenticated.objects().get(&entry.key) == Some(entry));
        }
    }
}

#[tokio::test]
async fn verify_of_an_empty_archive_is_ok() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let report = verify_archive(
        &store,
        &VerifyRequest::default(),
        &TrustedManifestKeys::default(),
    )
    .await
    .unwrap();

    check!(
        report
            == ArchiveVerifyReport {
                partitions: Vec::new(),
                global_orphan_objects: Vec::new(),
            }
    );
    check!(report.ok());
    check!(report.manifests() == 0);
    check!(report.fully_attested());
    check!(!report.has_epoch_restarts());
}
