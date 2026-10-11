//! The three differential cases.

use std::{collections::BTreeSet, path::PathBuf};

use assert2::assert;
use krabka_protocol::owned::api_versions_response::ApiVersion;

use crate::{
    divergence::DivergenceReport,
    oracle::{ORACLE_IMAGE, OracleBroker, krabka_api_versions, start_krabka},
    parse::{advertised, parse_single_broker},
    support::{JvmListeners, manifest_dir},
};

/// Where the checked-in report lives.
fn expectation_path() -> PathBuf {
    manifest_dir()
        .join("tests")
        .join("fixtures")
        .join("api_versions")
        .join("divergence.json")
}

/// Whether this run should rewrite the expectation instead of comparing to it.
///
/// This suite is the file's only writer, so regenerating is an explicit opt-in:
/// `KRABKA_UPDATE_API_VERSIONS_EXPECTATION=1 cargo test -p krabka-broker
/// --test api_versions_differential -- --ignored`. A Bazel sandbox cannot write
/// to the source tree, which is the other reason this is off by default.
fn regenerating() -> bool {
    std::env::var("KRABKA_UPDATE_API_VERSIONS_EXPECTATION").is_ok_and(|v| v == "1")
}

/// What the client listener advertises, in the order the JVM tool prints,
/// which is ascending API key.
///
/// `start_krabka` binds one `PLAINTEXT` listener and configures no
/// client-metrics receiver, so this is the pair the broker answers that
/// listener with: the KIP-714 keys are withheld the way a stock Kafka broker
/// withholds them, and so is every key in
/// `api_catalog::INTER_BROKER_ONLY_APIS`.
fn catalog_sorted_by_key() -> Vec<ApiVersion> {
    let mut apis = krabka_broker::api_catalog::supported_apis(
        krabka_broker::api_catalog::ListenerKind::Client,
        krabka_broker::api_catalog::ClientMetricsReceiver::Absent,
        krabka_broker::api_catalog::VersionGates::default(),
    );
    apis.sort_by_key(|api| api.api_key);
    apis
}

/// Decode the stock broker's table, retaining its container until the case ends.
fn kafka_advertised_table() -> (OracleBroker, Vec<ApiVersion>) {
    let oracle = OracleBroker::start();
    let output = oracle.api_versions();
    eprintln!("KRABKA[test] {ORACLE_IMAGE} api-versions:\n{output}");
    let kafka =
        advertised(&parse_single_broker(&output).unwrap_or_else(|e| panic!("{e}\n{output}")));
    (oracle, kafka)
}

/// The API keys one advertised table names.
fn key_set(apis: &[ApiVersion]) -> BTreeSet<i16> {
    apis.iter().map(|api| api.api_key).collect()
}

/// Read krabka's advertised table the way a client does: through the JVM tool.
async fn krabka_advertised_table() -> Vec<ApiVersion> {
    let listeners = JvmListeners::allocate();
    let (broker, _dir) = start_krabka(&listeners).await;
    let output = krabka_api_versions(&listeners);
    broker.shutdown().await;
    eprintln!("KRABKA[test] krabka api-versions:\n{output}");
    let rows = parse_single_broker(&output).unwrap_or_else(|e| panic!("{e}\n{output}"));
    advertised(&rows)
}

/// The JVM tool's view of krabka's `ApiVersions` response is exactly the
/// catalog the broker builds that response from.
///
/// This is the claim the suite exists for. `api_catalog` carries a per-KIP
/// comment for nearly every row and the handler copies the list onto the wire,
/// but until something read the table back through a real client nothing tied
/// those comments to what a client sees. The whole parsed table is compared
/// against the whole catalog, so an added, dropped, or re-ranged key fails here
/// rather than only in the unit test that asserts the handler copied the list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn krabka_advertises_exactly_the_api_catalog() {
    let table = krabka_advertised_table().await;
    assert!(table == catalog_sorted_by_key());
}

/// krabka's table beside a real Kafka broker's, compared against the
/// checked-in expectation.
///
/// Every difference is legitimate until someone reviews it, so the suite does
/// not judge them: it records the outer join and fails when the join moves.
/// Regenerate with `KRABKA_UPDATE_API_VERSIONS_EXPECTATION=1` and the diff is
/// the change under review.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn divergence_from_real_kafka_matches_the_expectation() {
    let krabka = krabka_advertised_table().await;

    let (_oracle, kafka) = kafka_advertised_table();

    let observed = DivergenceReport::build(ORACLE_IMAGE, &krabka, &kafka);
    // Every key on either side has a canonical name in krabka-protocol's
    // generated registry. One that does not is a key this repository cannot
    // reason about, so it fails here rather than landing as `Unknown` in the
    // checked-in file and the matrix built from it.
    let unnamed: Vec<i16> = observed
        .apis
        .iter()
        .filter(|row| row.name == "Unknown")
        .map(|row| row.api_key)
        .collect();
    assert!(
        unnamed.is_empty(),
        "api keys with no protocol name: {unnamed:?}"
    );

    let path = expectation_path();
    if regenerating() {
        observed.store(&path);
    }
    let expected = DivergenceReport::load(&path);
    assert!(
        observed == expected,
        "the advertised-version divergence moved for {}; rerun with \
         KRABKA_UPDATE_API_VERSIONS_EXPECTATION=1 and review {}",
        moved_rows(&observed, &expected),
        path.display(),
    );
}

/// The set of API keys the tool prints for krabka's client listener is the set
/// it prints for a stock Kafka broker.
///
/// The divergence case above records the whole join, ranges included, so it
/// moves whenever a range moves. This one asks the narrower question the issue
/// behind the split is about: `kafka-broker-api-versions.sh` against krabka
/// names neither more nor fewer APIs than the same command against Kafka, so a
/// tool or an audit that reads the advertised set to infer a node's role reads
/// krabka as the broker it is. Both tables are read with the tool from the same
/// pinned image, so nothing about the reader differs between the two halves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn the_client_listener_prints_the_same_api_key_set_as_kafka() {
    let krabka = krabka_advertised_table().await;

    let (_oracle, kafka) = kafka_advertised_table();

    let ours = key_set(&krabka);
    let theirs = key_set(&kafka);
    let krabka_only: Vec<i16> = ours.difference(&theirs).copied().collect();
    let kafka_only: Vec<i16> = theirs.difference(&ours).copied().collect();
    assert!(
        ours == theirs,
        "krabka advertises {krabka_only:?} that {ORACLE_IMAGE} does not, and misses \
         {kafka_only:?} that it does"
    );
}

/// The API keys whose recorded row changed, for the failure message. The
/// assertion above compares the reports whole, and whole is 75 rows: naming the
/// handful that moved is what a reader needs before reading that dump.
fn moved_rows(observed: &DivergenceReport, expected: &DivergenceReport) -> String {
    let keys = |report: &DivergenceReport| -> BTreeSet<i16> {
        report.apis.iter().map(|row| row.api_key).collect()
    };
    let mut moved: Vec<i16> = keys(observed)
        .symmetric_difference(&keys(expected))
        .copied()
        .collect();
    moved.extend(observed.apis.iter().filter_map(|row| {
        let was = expected
            .apis
            .iter()
            .find(|old| old.api_key == row.api_key)?;
        (was != row).then_some(row.api_key)
    }));
    moved.sort_unstable();
    format!("api keys {moved:?}")
}
