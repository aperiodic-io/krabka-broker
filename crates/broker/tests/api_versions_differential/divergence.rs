//! The checked-in record of how krabka's `ApiVersions` table differs from a
//! pinned real-Kafka broker's.
//!
//! [`DivergenceReport`] is the outer join of the two advertised tables on API
//! key. It is written to `tests/fixtures/api_versions/divergence.json` and read
//! back by the differential suite, so a range krabka starts or stops
//! advertising, or a range that moves away from Kafka's, arrives in a diff
//! rather than passing unnoticed. `aspect generate-kip-matrix` reads the same
//! file for the version columns of `docs/KIP_MATRIX.md`.

use std::path::Path;

use krabka_protocol::owned::api_versions_response::ApiVersion;
use serde::{Deserialize, Serialize};

/// An inclusive advertised version range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VersionRange {
    pub(crate) min: i16,
    pub(crate) max: i16,
}

impl From<&ApiVersion> for VersionRange {
    fn from(api: &ApiVersion) -> Self {
        Self {
            min: api.min_version,
            max: api.max_version,
        }
    }
}

/// How krabka's row for one API key stands against the Kafka oracle's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Verdict {
    /// Both advertise the key over the same range.
    Same,
    /// Both advertise the key, over different ranges.
    RangeDiffers,
    /// krabka advertises the key and the oracle does not. Both tables are read
    /// on a client listener with no client-metrics receiver, and krabka scopes
    /// its own response to that pair, so this verdict now means krabka serves
    /// an API the pinned Kafka release has not shipped -- not that krabka
    /// advertises a control-plane key on a listener Kafka would not.
    KrabkaOnly,
    /// The oracle advertises the key and krabka does not.
    KafkaOnly,
}

/// One API key's row of the join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ApiRow {
    pub(crate) api_key: i16,
    /// The canonical Kafka request name, from krabka-protocol's generated
    /// `ApiKey` registry.
    pub(crate) name: String,
    pub(crate) krabka: Option<VersionRange>,
    pub(crate) kafka: Option<VersionRange>,
    pub(crate) verdict: Verdict,
    /// Why this row's divergence is the one this repository means to have.
    ///
    /// Filled from [`RANGE_DIVERGENCE_INTENTS`] for every
    /// [`Verdict::RangeDiffers`] row, and `None` everywhere else. A range that
    /// starts differing without a sentence written for it panics in
    /// [`DivergenceReport::build`] rather than landing in the checked-in file
    /// unexplained.
    pub(crate) intent: Option<String>,
}

/// Why krabka advertises a different range from the oracle, one entry per
/// [`Verdict::RangeDiffers`] row, keyed by `api_key`.
///
/// The table is exhaustive by construction: [`DivergenceReport::build`] panics
/// on a `RangeDiffers` row with no entry here, so a range that moves away from
/// Kafka's fails `divergence_from_real_kafka_matches_the_expectation` until
/// someone decides what the divergence is for. `aspect generate-kip-matrix`
/// renders the sentence beside the two version columns in `docs/KIP_MATRIX.md`.
///
/// It is empty (#784): with `unstable.api.versions.enable` and
/// `legacy_request_versions_enable` at their defaults, which is how the suite
/// starts krabka, every listener advertises exactly Kafka 4.3.1's table. The
/// Kafka trunk versions krabka implements (`ApiVersions` v5, `TxnOffsetCommit`
/// v6, the streams v1 pair, api keys 93 and 94) and the pre-4.0 versions it
/// still decodes (`Fetch` v0-v3, `ListOffsets` v0) are opt-ins, recorded in
/// `api_catalog`'s notes and `docs/KIP_MATRIX.md` instead.
const RANGE_DIVERGENCE_INTENTS: &[(i16, &str)] = &[];

/// Keys whose advertised ranges match the oracle's while the versions krabka
/// serves do not, one entry each, with why krabka means it.
///
/// `ApiVersions` reports what a broker advertises, not what it accepts, so the
/// join alone cannot see such a divergence. [`DivergenceReport::build`] labels
/// these rows [`Verdict::RangeDiffers`] anyway, keeping both advertised ranges
/// in the version columns, so `docs/KIP_MATRIX.md` states the divergence
/// instead of calling it a match. The build panics if one of these keys stops
/// advertising the same range, since the sentence would then be describing a
/// different divergence.
///
/// It is empty (#784): `Produce` v0-v2, advertised by both brokers, is refused
/// by krabka too unless `legacy_request_versions_enable` is set.
const SERVED_RANGE_DIVERGENCES: &[(i16, &str)] = &[];

/// The two recorded-intent tables one join reads.
#[derive(Clone, Copy)]
struct Intents<'a> {
    range: &'a [(i16, &'a str)],
    served: &'a [(i16, &'a str)],
}

/// The tables this repository records.
const RECORDED: Intents<'static> = Intents {
    range: RANGE_DIVERGENCE_INTENTS,
    served: SERVED_RANGE_DIVERGENCES,
};

/// The recorded intent for one row of the join.
///
/// Only a [`Verdict::RangeDiffers`] row carries one: a matching range needs no
/// explanation, and a one-sided row is explained once, in prose, by the
/// oracle's listener and configuration rather than per API key.
///
/// # Panics
///
/// Panics when a range differs and `intents` has no entry for the key. That
/// is the point of the table: a new divergence stops the differential suite
/// until someone writes down whether it is meant.
fn range_divergence_intent(intents: Intents<'_>, api_key: i16, verdict: Verdict) -> Option<String> {
    if verdict != Verdict::RangeDiffers {
        return None;
    }
    let Some((_, intent)) = intents
        .range
        .iter()
        .chain(intents.served)
        .find(|(key, _)| *key == api_key)
    else {
        panic!(
            "api_key {api_key} now advertises a different range from the \
             oracle and no intent is recorded for it. Add an entry to \
             `RANGE_DIVERGENCE_INTENTS` saying whether krabka means to \
             diverge here -- and, if it does not, change \
             `api_catalog::supported_apis` instead."
        )
    };
    Some((*intent).to_owned())
}

/// Both advertised tables, joined and sorted by API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DivergenceReport {
    /// The image the `kafka` column was read from, tag included.
    pub(crate) oracle_image: String,
    pub(crate) apis: Vec<ApiRow>,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct DivergenceSetup<'a> {
    #[default(RECORDED)]
    intents: Intents<'a>,
    #[default("mirror.gcr.io/apache/kafka:4.3.1")]
    oracle_image: &'a str,
}

impl DivergenceReport {
    /// Join krabka's advertised table against the oracle's.
    pub(crate) fn build(oracle_image: &str, krabka: &[ApiVersion], kafka: &[ApiVersion]) -> Self {
        Self::build_with(
            krabka,
            kafka,
            DivergenceSetup {
                oracle_image,
                ..Default::default()
            },
        )
    }

    /// [`Self::build`] against `intents` rather than the recorded tables.
    fn build_with(krabka: &[ApiVersion], kafka: &[ApiVersion], setup: DivergenceSetup<'_>) -> Self {
        let DivergenceSetup {
            intents,
            oracle_image,
        } = setup;
        let mut keys: Vec<i16> = krabka
            .iter()
            .chain(kafka)
            .map(|api| api.api_key)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        keys.sort_unstable();

        let find = |table: &[ApiVersion], key: i16| -> Option<VersionRange> {
            table
                .iter()
                .find(|api| api.api_key == key)
                .map(VersionRange::from)
        };

        let apis = keys
            .into_iter()
            .map(|api_key| {
                let krabka = find(krabka, api_key);
                let kafka = find(kafka, api_key);
                let served_divergence = intents.served.iter().any(|(key, _)| *key == api_key);
                let verdict = match (krabka, kafka) {
                    (Some(ours), Some(theirs)) if ours == theirs && served_divergence => {
                        Verdict::RangeDiffers
                    }
                    (Some(_), Some(_)) if served_divergence => panic!(
                        "api_key {api_key} is recorded in `SERVED_RANGE_DIVERGENCES` \
                         as advertising the oracle's range, and no longer does; \
                         rewrite or move its entry"
                    ),
                    (Some(ours), Some(theirs)) if ours == theirs => Verdict::Same,
                    (Some(_), Some(_)) => Verdict::RangeDiffers,
                    (Some(_), None) => Verdict::KrabkaOnly,
                    // A key reaches the join only from one of the two tables,
                    // so the empty case cannot arise.
                    (None, _) => Verdict::KafkaOnly,
                };
                ApiRow {
                    api_key,
                    name: krabka_broker::telemetry::api_name(api_key).to_owned(),
                    krabka,
                    kafka,
                    verdict,
                    intent: range_divergence_intent(intents, api_key, verdict),
                }
            })
            .collect();

        Self {
            oracle_image: oracle_image.to_owned(),
            apis,
        }
    }

    /// Read the checked-in report.
    ///
    /// # Panics
    ///
    /// Panics when the file is missing or does not deserialize, both of which
    /// mean the expectation has to be regenerated rather than compared.
    pub(crate) fn load(path: &Path) -> Self {
        let body = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
    }

    /// Overwrite the checked-in report with this one.
    ///
    /// # Panics
    ///
    /// Panics when the file cannot be written.
    pub(crate) fn store(&self, path: &Path) {
        let mut body = serde_json::to_string_pretty(self).expect("serialize divergence report");
        body.push('\n');
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("create {}: {e}", parent.display()));
        }
        std::fs::write(path, &body).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        eprintln!(
            "KRABKA[test] rewrote {} ({} bytes)",
            path.display(),
            body.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn api(api_key: i16, min_version: i16, max_version: i16) -> ApiVersion {
        ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        }
    }

    fn range(min: i16, max: i16) -> VersionRange {
        VersionRange { min, max }
    }

    /// Intent tables of the shape the recorded ones once had, so the join's
    /// labelling is exercised whatever the recorded tables hold.
    const SAMPLE: Intents<'static> = Intents {
        range: &[(1, "Fetch serves older versions.")],
        served: &[(0, "Produce serves older versions.")],
    };

    /// The sample intent for `api_key`, as `build_with` writes it.
    fn intent(api_key: i16) -> Option<String> {
        range_divergence_intent(SAMPLE, api_key, Verdict::RangeDiffers)
    }

    fn build(oracle: &str, krabka: &[ApiVersion], kafka: &[ApiVersion]) -> DivergenceReport {
        DivergenceReport::build_with(
            krabka,
            kafka,
            DivergenceSetup {
                intents: SAMPLE,
                oracle_image: oracle,
            },
        )
    }

    #[test]
    fn join_covers_both_tables_and_labels_each_key() {
        let krabka = vec![api(3, 0, 13), api(1, 4, 17), api(80, 0, 1)];
        let kafka = vec![api(3, 0, 13), api(1, 4, 18), api(88, 0, 0)];
        assert!(
            build("oracle:1.2.3", &krabka, &kafka)
                == DivergenceReport {
                    oracle_image: "oracle:1.2.3".to_owned(),
                    apis: vec![
                        ApiRow {
                            api_key: 1,
                            name: "Fetch".to_owned(),
                            krabka: Some(range(4, 17)),
                            kafka: Some(range(4, 18)),
                            verdict: Verdict::RangeDiffers,
                            intent: intent(1),
                        },
                        ApiRow {
                            api_key: 3,
                            name: "Metadata".to_owned(),
                            krabka: Some(range(0, 13)),
                            kafka: Some(range(0, 13)),
                            verdict: Verdict::Same,
                            intent: None,
                        },
                        ApiRow {
                            api_key: 80,
                            name: "AddRaftVoter".to_owned(),
                            krabka: Some(range(0, 1)),
                            kafka: None,
                            verdict: Verdict::KrabkaOnly,
                            intent: None,
                        },
                        ApiRow {
                            api_key: 88,
                            name: "StreamsGroupHeartbeat".to_owned(),
                            krabka: None,
                            kafka: Some(range(0, 0)),
                            verdict: Verdict::KafkaOnly,
                            intent: None,
                        },
                    ],
                }
        );
    }

    /// A key is recorded in one table or the other, never both, and every
    /// recorded sentence says something.
    #[test]
    fn every_recorded_intent_is_non_empty_and_keyed_once() {
        let all: Vec<&(i16, &str)> = RANGE_DIVERGENCE_INTENTS
            .iter()
            .chain(SERVED_RANGE_DIVERGENCES)
            .collect();
        let unique: std::collections::BTreeSet<i16> = all.iter().map(|(key, _)| *key).collect();
        assert!(unique.len() == all.len());
        assert!(all.iter().all(|(_, intent)| !intent.trim().is_empty()));
    }

    /// #863: a key that advertises the oracle's own range, recorded as served
    /// wider, still carries the reason.
    #[test]
    fn a_served_range_divergence_is_recorded_on_a_matching_advertised_range() {
        let report = build("oracle:1.2.3", &[api(0, 0, 13)], &[api(0, 0, 13)]);
        assert!(
            report.apis
                == vec![ApiRow {
                    api_key: 0,
                    name: "Produce".to_owned(),
                    krabka: Some(range(0, 13)),
                    kafka: Some(range(0, 13)),
                    verdict: Verdict::RangeDiffers,
                    intent: intent(0),
                }]
        );
    }

    /// A served-range entry describes a key whose advertised range matches;
    /// once it does not, the sentence is about something else.
    #[test]
    #[should_panic(expected = "no longer does")]
    fn a_served_range_divergence_whose_advertised_range_moves_panics() {
        let _ = build("oracle:1.2.3", &[api(0, 3, 13)], &[api(0, 0, 13)]);
    }

    /// A range that starts differing with nothing written for it fails the
    /// differential suite rather than landing in the checked-in file.
    #[test]
    #[should_panic(expected = "no intent is recorded")]
    fn an_unrecorded_range_divergence_panics() {
        let _ = build("oracle:1.2.3", &[api(3, 0, 13)], &[api(3, 0, 12)]);
    }

    /// #784: with nothing recorded, any range divergence at all fails the
    /// suite, since krabka's default table is Kafka 4.3.1's.
    #[test]
    #[should_panic(expected = "no intent is recorded")]
    fn the_recorded_tables_admit_no_range_divergence() {
        let _ = DivergenceReport::build("oracle:1.2.3", &[api(1, 0, 18)], &[api(1, 4, 18)]);
    }

    /// The checked-in fixture is byte-for-byte what [`DivergenceReport::store`]
    /// writes for the report it decodes to.
    ///
    /// The differential suite needs Docker, so the file is sometimes edited by
    /// hand between regenerations. This is what keeps such an edit from
    /// producing a file the next regeneration would reformat, which would show
    /// up as noise in the diff that is meant to be the change under review.
    #[test]
    fn the_checked_in_report_is_exactly_what_store_writes() {
        // `include_str!` reads the fixture at compile time, which is what
        // `compile_data` in //crates/broker:BUILD.bazel makes available.
        // Keeping `CARGO_MANIFEST_DIR` for a runtime read instead would embed
        // this checkout's absolute path in the test binary, and the Bazel
        // build rejects that.
        const CHECKED_IN: &str = include_str!("../fixtures/api_versions/divergence.json");

        let dir = tempfile::tempdir().expect("tempdir");
        let decoded = dir.path().join("decoded.json");
        std::fs::write(&decoded, CHECKED_IN).expect("write checked-in");
        let report = DivergenceReport::load(&decoded);

        let rewritten = dir.path().join("divergence.json");
        report.store(&rewritten);

        assert!(std::fs::read_to_string(&rewritten).expect("read rewritten") == CHECKED_IN);
    }

    #[test]
    fn report_round_trips_through_json() {
        let report = build("oracle:1.2.3", &[api(1, 0, 18)], &[api(1, 4, 18)]);
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested").join("divergence.json");
        report.store(&path);
        assert!(DivergenceReport::load(&path) == report);
    }
}
