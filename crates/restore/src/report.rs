//! The restore summary an operator reads, and its text and JSON renderings.
//!
//! This module owns the report model and nothing else. It aggregates what the
//! other stages establish: which partitions were restored, how many segments
//! and records each kept, where each partition's log now ends, which segments
//! failed verification and were skipped under `--continue-on-corrupt`, and
//! which bounds were applied. The text rendering is for a human during an
//! incident, and the JSON rendering is for a runbook that has to assert on the
//! outcome. Both render the same model, so they can never disagree. A dry run
//! produces the same report as a real run, with `dry_run` set.

use std::{collections::BTreeMap, fmt::Write as _, path::PathBuf};

use serde::Serialize;
use uuid::Uuid;

use crate::materialize::SegmentOutcome;

/// How [`RestoreReport::render`] writes the summary.
///
/// This is the `--report` value, so it is part of the command-line surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportFormat {
    /// Aligned plain text, for a person.
    #[default]
    Text,
    /// One JSON object, for a script.
    Json,
}

/// What the restore did, as a whole.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestoreReport {
    /// Whether the run wrote anything.
    pub dry_run: bool,
    /// The target log directory.
    pub log_dir: PathBuf,
    /// The cluster id the target was formatted with.
    pub cluster_id: Uuid,
    /// Trusted WORM coverage, absent for an integrity-only restore.
    pub authentication: Option<AuthenticationReport>,
    /// Diskless-WAL recovery boundary, absent when no capture was supplied.
    pub diskless: Option<DisklessRestoreReport>,
    /// Cluster metadata recovered from the controller snapshot.
    pub metadata: MetadataRestoreReport,
    /// One entry per restored partition, ordered by topic then partition.
    pub partitions: Vec<PartitionReport>,
    /// Segments that failed verification and were skipped.
    pub skipped: Vec<SkippedSegment>,
}

/// Diskless-WAL recovery outcome and its explicit limitations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DisklessRestoreReport {
    pub captured_at_ms: u64,
    pub source_cutoffs: Vec<i64>,
    pub partitions: Vec<DisklessPartitionReport>,
    pub limitations: Vec<String>,
}

/// Recovery boundary for one diskless partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DisklessPartitionReport {
    pub topic: String,
    pub partition: i32,
    pub topic_id: Uuid,
    pub delete_floor: i64,
    pub recovery_cutoff: i64,
    pub objects: u64,
    pub batches: u64,
    pub records: u64,
    pub records_dropped: u64,
}

/// Signed WORM evidence applied to the bytes consumed by restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuthenticationReport {
    /// Manifest-chain partitions authenticated.
    pub partitions: usize,
    /// Signed manifests authenticated.
    pub manifests: u64,
    /// Objects whose consumed bytes matched a signed digest.
    pub objects: u64,
    /// Trusted public keys supplied, including rotation keys.
    pub trusted_keys: usize,
    /// Verified tip of each partition chain.
    pub chain_heads: BTreeMap<String, String>,
    /// Whether each tip was checked against an independently held boundary.
    pub tail_truncation_protected: bool,
}

/// Cluster metadata recovered alongside the archived partition data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataRestoreReport {
    /// Snapshot used for recovery, absent when none was supplied.
    pub snapshot: Option<PathBuf>,
    /// Topic-configuration records restored for selected topics.
    pub topic_configs: usize,
    /// ACL records restored.
    pub access_control_entries: usize,
    /// Client-quota records restored.
    pub client_quotas: usize,
    /// SCRAM credential records restored.
    pub scram_credentials: usize,
    /// Finalized feature-level records restored.
    pub feature_levels: usize,
    /// Topics whose configuration was unavailable because no snapshot was supplied.
    pub topics_without_configuration: Vec<String>,
}

impl MetadataRestoreReport {
    /// Warning emitted when cluster metadata could not be recovered.
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        self.snapshot.is_none().then(|| {
            format!(
                "WARNING: no --metadata-snapshot; topic configuration was not recovered for: {}; ACLs, client quotas, SCRAM credentials, and feature levels were not recovered",
                self.topics_without_configuration.join(", ")
            )
        })
    }
}

/// What the restore did for one partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionReport {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Stable topic id, as the archive records it.
    pub topic_id: Uuid,
    /// One entry per segment the restore wrote, in offset order.
    pub segments: Vec<SegmentOutcome>,
}

/// One segment the restore could not verify and did not write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SkippedSegment {
    /// Topic of the skipped segment.
    pub topic: String,
    /// Partition index of the skipped segment.
    pub partition: i32,
    /// The segment id the archive names the copy with.
    pub segment_id: Uuid,
    /// Why verification rejected it.
    pub reason: String,
}

impl RestoreReport {
    /// Render the report in `format`.
    ///
    /// The rendering is total. Every field of the model is a string, a number,
    /// or a sequence of the same, so the JSON encoder has no failure mode to
    /// report.
    #[must_use]
    pub fn render(&self, format: ReportFormat) -> String {
        match format {
            ReportFormat::Text => self.render_text(),
            ReportFormat::Json => self.render_json(),
        }
    }

    /// Render as one JSON object.
    ///
    /// `serde_json::to_string_pretty` fails only on a map with non-string
    /// keys, a `NaN`/`infinite` float, or a type whose `Serialize` impl
    /// itself errors. `RestoreReport` has none of those: every field is a
    /// string, an integer, a `Uuid`, a `PathBuf`, a bool, or a sequence or
    /// struct built from the same, so the `.expect` below documents a real
    /// invariant rather than papering over a live failure mode.
    fn render_json(&self) -> String {
        serde_json::to_string_pretty(self)
            .expect("RestoreReport holds only strings, numbers, and sequences of the same")
    }

    /// Render as aligned text for an operator reading the terminal or a log.
    ///
    /// This groups segments by partition and sums their counters, rather than
    /// listing every segment: a wall of per-segment lines is not what an
    /// operator scanning the output first needs, and per-segment detail is
    /// exactly what the JSON rendering is for. What a person mid-incident
    /// wants first, in order, is: did this write anything, what cluster is
    /// it, and then per partition, how far it got and what changed on the
    /// way. Batches the bound rewrote or emptied are called out separately
    /// from the kept/dropped record counts, because "records dropped" alone
    /// does not tell an operator who ran `--exclude-key` that the exclusion
    /// actually matched something.
    fn render_text(&self) -> String {
        let mut out = String::new();
        let status = if self.dry_run {
            "dry run against"
        } else {
            "wrote"
        };
        writeln!(out, "krabka restore: {status} {}", self.log_dir.display())
            .expect("String writer is infallible");
        if self.dry_run {
            writeln!(out, "  (dry run: nothing was written to disk)")
                .expect("String writer is infallible");
        }
        writeln!(out, "cluster id: {}", self.cluster_id).expect("String writer is infallible");

        if let Some(diskless) = &self.diskless {
            writeln!(
                out,
                "diskless WAL capture: {} partition{}, captured at {} ms",
                diskless.partitions.len(),
                plural(diskless.partitions.len() as u64),
                diskless.captured_at_ms
            )
            .expect("String writer is infallible");
            for partition in &diskless.partitions {
                writeln!(
                    out,
                    "  {}-{}: floor {}, restored through offset {} (exclusive); {} batch{}, {} record{} kept, {} dropped",
                    partition.topic,
                    partition.partition,
                    partition.delete_floor,
                    partition.recovery_cutoff,
                    partition.batches,
                    plural(partition.batches),
                    partition.records,
                    plural(partition.records),
                    partition.records_dropped,
                )
                .expect("String writer is infallible");
            }
            for limitation in &diskless.limitations {
                writeln!(out, "WARNING: {limitation}").expect("String writer is infallible");
            }
        }

        if let Some(authentication) = &self.authentication {
            writeln!(
                out,
                "authentication: {} manifest{}, {} object{}, {} partition{}, {} trusted key{}",
                authentication.manifests,
                plural(authentication.manifests),
                authentication.objects,
                plural(authentication.objects),
                authentication.partitions,
                plural(authentication.partitions as u64),
                authentication.trusted_keys,
                plural(authentication.trusted_keys as u64),
            )
            .expect("String writer is infallible");
            let tail = if authentication.tail_truncation_protected {
                "tail truncation protected by independently pinned heads"
            } else {
                "tail truncation not protected without independently pinned heads"
            };
            writeln!(
                out,
                "authentication boundary: verified chain tips {}; {tail}",
                authentication
                    .chain_heads
                    .iter()
                    .map(|(partition, head)| format!("{partition}={head}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .expect("String writer is infallible");
        }

        if let Some(warning) = self.metadata.warning() {
            writeln!(out, "{warning}").expect("String writer is infallible");
        } else {
            writeln!(
                out,
                "metadata: {} topic config{}, {} ACL{}, {} client quota{}, {} SCRAM credential{}, {} feature level{}",
                self.metadata.topic_configs,
                plural(self.metadata.topic_configs as u64),
                self.metadata.access_control_entries,
                plural(self.metadata.access_control_entries as u64),
                self.metadata.client_quotas,
                plural(self.metadata.client_quotas as u64),
                self.metadata.scram_credentials,
                plural(self.metadata.scram_credentials as u64),
                self.metadata.feature_levels,
                plural(self.metadata.feature_levels as u64),
            )
            .expect("String writer is infallible");
        }

        let mut current_topic: Option<(&str, Uuid)> = None;
        for partition in &self.partitions {
            let topic_key = (partition.topic.as_str(), partition.topic_id);
            if current_topic != Some(topic_key) {
                writeln!(out).expect("String writer is infallible");
                writeln!(out, "{} (topic id {})", partition.topic, partition.topic_id)
                    .expect("String writer is infallible");
                current_topic = Some(topic_key);
            }
            write_partition_line(&mut out, partition);
        }

        if !self.skipped.is_empty() {
            writeln!(out).expect("String writer is infallible");
            writeln!(
                out,
                "skipped ({} segment{} failed verification):",
                self.skipped.len(),
                plural(self.skipped.len() as u64)
            )
            .expect("String writer is infallible");
            for segment in &self.skipped {
                writeln!(
                    out,
                    "  {} partition {} segment {}: {}",
                    segment.topic, segment.partition, segment.segment_id, segment.reason
                )
                .expect("String writer is infallible");
            }
        }

        out
    }
}

/// Write one partition's summary line, aggregating its segments' counters.
///
/// A partition with no restored segments still renders, rather than indexing
/// into an empty `segments` slice or dividing by zero: the report model does
/// not forbid it even though `discover.rs`'s `EmptyArchive` check makes it
/// unlikely in practice.
fn write_partition_line(out: &mut String, partition: &PartitionReport) {
    let Some(first) = partition.segments.first() else {
        writeln!(
            out,
            "  partition {}: no segments restored",
            partition.partition
        )
        .expect("String writer is infallible");
        return;
    };
    let last = partition
        .segments
        .last()
        .expect("just checked segments is non-empty");

    let mut records_kept = 0u64;
    let mut records_dropped = 0u64;
    let mut batches_rewritten = 0u64;
    let mut batches_emptied = 0u64;
    for segment in &partition.segments {
        records_kept += segment.records_kept;
        records_dropped += segment.records_dropped;
        batches_rewritten += segment.batches_rewritten;
        batches_emptied += segment.batches_emptied;
    }

    let segment_count = partition.segments.len() as u64;
    let mut notes = Vec::new();
    if batches_rewritten > 0 {
        notes.push(format!(
            "{} batch{} rewritten",
            format_thousands(batches_rewritten),
            plural(batches_rewritten)
        ));
    }
    if batches_emptied > 0 {
        notes.push(format!(
            "{} batch{} emptied",
            format_thousands(batches_emptied),
            plural(batches_emptied)
        ));
    }
    let notes = if notes.is_empty() {
        String::new()
    } else {
        format!(" ({})", notes.join(", "))
    };

    writeln!(
        out,
        "  partition {}: {} segment{}, offsets [{}, {}], {} records kept, {} dropped{notes}",
        partition.partition,
        format_thousands(segment_count),
        plural(segment_count),
        first.base_offset,
        last.end_offset,
        format_thousands(records_kept),
        format_thousands(records_dropped),
    )
    .expect("String writer is infallible");
}

/// `"s"` for anything but exactly one, so counts read as plain English.
fn plural(count: u64) -> &'static str {
    if count == 1 { "" } else { "s" }
}

/// Group `n`'s digits with `,` every three places, for a human scanning a
/// terminal. `krabka-restore` has no formatting-number dependency in its
/// `Cargo.toml`, and this is a handful of lines, so it is hand-rolled rather
/// than pulling one in.
fn format_thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::Offset;
    use uuid::Uuid;

    use super::{
        MetadataRestoreReport, PartitionReport, ReportFormat, RestoreReport, SegmentOutcome,
        SkippedSegment, format_thousands, plural,
    };

    #[derive(Clone, Copy, Default)]
    struct RestoredRecordCount(u64);

    #[derive(Clone, Copy, Default)]
    struct RestoredBatchCount(u64);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct SegmentOutcomeSetup {
        #[default(Offset(0))]
        base: Offset,
        #[default(Offset(402))]
        end: Offset,
        #[default(RestoredRecordCount(402))]
        kept: RestoredRecordCount,
        dropped: RestoredRecordCount,
        rewritten: RestoredBatchCount,
        emptied: RestoredBatchCount,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum RestoreMode {
        Perform,
        DryRun,
    }

    fn segment(setup: SegmentOutcomeSetup) -> SegmentOutcome {
        let SegmentOutcomeSetup {
            base,
            end,
            kept,
            dropped,
            rewritten,
            emptied,
        } = setup;
        SegmentOutcome {
            segment_id: Uuid::from_u128(u128::from(base.0.unsigned_abs()) + 1),
            base_offset: base,
            end_offset: end,
            batches_kept: 1,
            batches_rewritten: rewritten.0,
            batches_emptied: emptied.0,
            records_kept: kept.0,
            records_dropped: dropped.0,
            bytes_written: 1024,
        }
    }

    fn sample_report(mode: RestoreMode) -> RestoreReport {
        RestoreReport {
            dry_run: mode == RestoreMode::DryRun,
            log_dir: "/var/lib/krabka/restored".into(),
            cluster_id: Uuid::from_u128(0xC1_A5_7E_00),
            authentication: None,
            diskless: None,
            metadata: MetadataRestoreReport {
                snapshot: Some("/backup/metadata.checkpoint".into()),
                topic_configs: 2,
                access_control_entries: 1,
                client_quotas: 1,
                scram_credentials: 1,
                feature_levels: 2,
                topics_without_configuration: vec![],
            },
            partitions: vec![
                PartitionReport {
                    topic: "orders".to_owned(),
                    partition: 0,
                    topic_id: Uuid::from_u128(0x0001),
                    segments: vec![
                        segment(SegmentOutcomeSetup {
                            end: Offset(4999),
                            kept: RestoredRecordCount(4995),
                            dropped: RestoredRecordCount(5),
                            rewritten: RestoredBatchCount(1),
                            ..Default::default()
                        }),
                        segment(SegmentOutcomeSetup {
                            base: Offset(5000),
                            end: Offset(12489),
                            kept: RestoredRecordCount(7485),
                            dropped: RestoredRecordCount(4),
                            emptied: RestoredBatchCount(1),
                            ..Default::default()
                        }),
                    ],
                },
                PartitionReport {
                    topic: "orders".to_owned(),
                    partition: 1,
                    topic_id: Uuid::from_u128(0x0001),
                    segments: vec![segment(SegmentOutcomeSetup {
                        end: Offset(8021),
                        kept: RestoredRecordCount(8015),
                        dropped: RestoredRecordCount(6),
                        ..Default::default()
                    })],
                },
                PartitionReport {
                    topic: "orders-archive".to_owned(),
                    partition: 0,
                    topic_id: Uuid::from_u128(0x0002),
                    segments: vec![segment(SegmentOutcomeSetup::default())],
                },
            ],
            skipped: vec![SkippedSegment {
                topic: "orders-2".to_owned(),
                partition: 3,
                segment_id: Uuid::from_u128(0xBAD),
                reason: "checksum mismatch in batch at offset 900".to_owned(),
            }],
        }
    }

    #[test]
    fn default_report_format_is_text() {
        check!(ReportFormat::default() == ReportFormat::Text);
    }

    #[test]
    fn format_thousands_groups_digits() {
        let cases = [
            (0u64, "0"),
            (5, "5"),
            (999, "999"),
            (1000, "1,000"),
            (12480, "12,480"),
            (1_234_567, "1,234,567"),
        ];
        for (input, expected) in cases {
            check!(format_thousands(input) == expected);
        }
    }

    #[test]
    fn plural_is_empty_only_for_one() {
        check!(plural(0) == "s");
        check!(plural(1) == "");
        check!(plural(2) == "s");
    }

    #[test]
    fn text_rendering_contains_key_facts() {
        let report = sample_report(RestoreMode::Perform);
        let text = report.render(ReportFormat::Text);

        check!(text.contains("orders"));
        check!(text.contains("orders-archive"));
        check!(text.contains(&report.cluster_id.to_string()));
        check!(text.contains("partition 0"));
        check!(text.contains("partition 1"));
        check!(text.contains("[0, 12489]"));
        check!(text.contains("12,480 records kept"));
        check!(text.contains("9 dropped"));
        check!(text.contains("1 batch rewritten"));
        check!(text.contains("1 batch emptied"));
        check!(text.contains("checksum mismatch in batch at offset 900"));
        check!(text.contains("orders-2"));
    }

    #[test]
    fn text_rendering_exact_snapshot_for_one_partition() {
        let report = RestoreReport {
            dry_run: false,
            log_dir: "/var/lib/krabka/restored".into(),
            cluster_id: Uuid::nil(),
            authentication: None,
            diskless: None,
            metadata: MetadataRestoreReport {
                snapshot: Some("/backup/metadata.checkpoint".into()),
                topic_configs: 1,
                access_control_entries: 0,
                client_quotas: 0,
                scram_credentials: 0,
                feature_levels: 0,
                topics_without_configuration: vec![],
            },
            partitions: vec![PartitionReport {
                topic: "orders-archive".to_owned(),
                partition: 0,
                topic_id: Uuid::nil(),
                segments: vec![segment(SegmentOutcomeSetup::default())],
            }],
            skipped: vec![],
        };

        let expected = "krabka restore: wrote /var/lib/krabka/restored\n\
             cluster id: 00000000-0000-0000-0000-000000000000\n\
             metadata: 1 topic config, 0 ACLs, 0 client quotas, 0 SCRAM credentials, 0 feature levels\n\
             \n\
             orders-archive (topic id 00000000-0000-0000-0000-000000000000)\n\
             \x20 partition 0: 1 segment, offsets [0, 402], 402 records kept, 0 dropped\n";

        check!(report.render(ReportFormat::Text) == expected);
    }

    #[test]
    fn dry_run_is_visible_in_text() {
        let dry = sample_report(RestoreMode::DryRun).render(ReportFormat::Text);
        let real = sample_report(RestoreMode::Perform).render(ReportFormat::Text);

        check!(dry.contains("dry run"));
        check!(!real.contains("dry run"));
    }

    #[test]
    fn empty_skipped_list_omits_section() {
        let mut report = sample_report(RestoreMode::Perform);
        report.skipped.clear();
        let text = report.render(ReportFormat::Text);

        check!(!text.contains("skipped"));
    }

    #[test]
    fn nonempty_skipped_list_names_every_segment_and_reason() {
        let mut report = sample_report(RestoreMode::Perform);
        report.skipped.push(SkippedSegment {
            topic: "returns".to_owned(),
            partition: 7,
            segment_id: Uuid::from_u128(0xF00D),
            reason: "truncated batch header".to_owned(),
        });
        let text = report.render(ReportFormat::Text);

        for skipped in &report.skipped {
            check!(text.contains(&skipped.topic));
            check!(text.contains(&skipped.segment_id.to_string()));
            check!(text.contains(&skipped.reason));
        }
    }

    #[test]
    fn zero_segment_partition_does_not_panic() {
        let report = RestoreReport {
            dry_run: true,
            log_dir: "/tmp/restore".into(),
            cluster_id: Uuid::nil(),
            authentication: None,
            diskless: None,
            metadata: MetadataRestoreReport {
                snapshot: None,
                topic_configs: 0,
                access_control_entries: 0,
                client_quotas: 0,
                scram_credentials: 0,
                feature_levels: 0,
                topics_without_configuration: vec!["empty-topic".to_owned()],
            },
            partitions: vec![PartitionReport {
                topic: "empty-topic".to_owned(),
                partition: 0,
                topic_id: Uuid::nil(),
                segments: vec![],
            }],
            skipped: vec![],
        };

        let text = report.render(ReportFormat::Text);

        check!(text.contains("empty-topic"));
        check!(text.contains("no segments restored"));
        check!(text.contains("WARNING: no --metadata-snapshot"));
        check!(text.contains("ACLs, client quotas, SCRAM credentials, and feature levels"));
    }

    #[test]
    fn json_rendering_round_trips_field_values() {
        let report = sample_report(RestoreMode::Perform);
        let json = report.render(ReportFormat::Json);
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        check!(value["dry_run"] == serde_json::json!(false));
        for (pointer, expected) in [
            (
                "/cluster_id",
                serde_json::json!(report.cluster_id.to_string()),
            ),
            ("/partitions/0/topic", serde_json::json!("orders")),
            ("/partitions/0/partition", serde_json::json!(0)),
            (
                "/partitions/0/segments/0/records_kept",
                serde_json::json!(4995),
            ),
            (
                "/partitions/0/segments/1/batches_emptied",
                serde_json::json!(1),
            ),
        ] {
            check!(value.pointer(pointer) == Some(&expected), "{pointer}");
        }
        check!(
            value["skipped"][0]["reason"]
                == serde_json::json!("checksum mismatch in batch at offset 900")
        );
    }
}
