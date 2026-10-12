// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! The KIP-584 write-side surface. `ApiVersions` v3 and above expose the
//! feature surface that the JVM admin tooling reads. `supported_features`
//! advertises `metadata.version` from `min = 7` (`3.3-IV3`) to `max = 30`
//! (`4.3-IV0`, Kafka 4.3.1's latest production level, while
//! `unstable.feature.versions.enable` is off), and from v4 every other feature
//! from level 0, as Kafka does.
//!
//! A standalone, self-bootstrapped broker behaves like a freshly formatted
//! Kafka 4.3 cluster. It finalizes every registered feature at its release
//! default for `metadata.version = 30` (`4.3-IV0`, Kafka 4.3's
//! `LATEST_PRODUCTION`), not at trunk's unstable 4.4 levels, which an operator
//! reaches only under `unstable.feature.versions.enable`.
//! At 4.3-IV0 that is `group.version = 1`, `transaction.version = 2`, and ELR,
//! `share.version` and `streams.version` at 1. It also reports a real
//! `finalized_features_epoch` of `>= 0`. `tests/feature_finalization.rs`
//! exercises `UpdateFeatures`. `transaction.version` is advertised up to 2, the
//! highest level Kafka defines. KIP-939 needs no higher one.
//!
//! A finalized `metadata.version` above the connecting JVM client's known
//! `MetadataVersion` enum, or one with `finalized_features_epoch = 0`, makes
//! that client throw `IllegalArgumentException` out of
//! `MetadataVersion.fromFeatureLevel(N)`. That failure once broke 19
//! `broker-jvm-acceptance` tests, and it is why a fresh cluster bootstraps at
//! the latest level a released Kafka knows. This test guards the fresh-broker
//! surface.

use assert2::assert;

mod support;

use krabka_format::LATEST_PRODUCTION_METADATA_VERSION;

#[tokio::test]
async fn v3_response_advertises_supported_and_bootstrapped_finalized_features() {
    let p = support::start().await;

    let resp = support::discovery::api_versions(&p.client).await;

    assert!(resp.error_code == 0, "{resp:?}");

    // KIP-584 write-side: supported_features advertises metadata.version over
    // the supported range; the standalone broker self-bootstraps the release
    // defaults, so finalized_features carries metadata.version=4.3-IV0 and
    // group.version=1 with a real (>= 0) epoch. See the module-level note for
    // the JVM compatibility rationale.
    let mv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version advertised in supported_features");
    assert!(mv.min_version == 7, "{resp:?}");
    assert!(
        mv.max_version == LATEST_PRODUCTION_METADATA_VERSION,
        "{resp:?}"
    );
    let gv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version advertised in supported_features");
    // The client negotiates ApiVersions v4, where Kafka advertises every
    // feature from its minimum production level, 0 for all but
    // metadata.version (`BrokerFeatures.defaultSupportedFeatures`). Below v4
    // Kafka omits a zero-minimum feature instead (`alterFeatureLevel0`).
    assert!(gv.min_version == 0, "{resp:?}");
    assert!(gv.max_version == 1, "{resp:?}");
    let tv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version advertised in supported_features");
    assert!(tv.min_version == 0, "{resp:?}");
    assert!(tv.max_version == 2, "{resp:?}");

    // A self-bootstrapped broker finalizes the release defaults.
    let finalized_metadata_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("metadata.version finalized at bootstrap");
    assert!(
        finalized_metadata_version.max_version_level == LATEST_PRODUCTION_METADATA_VERSION,
        "{resp:?}"
    );
    let finalized_group_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version finalized at bootstrap");
    assert!(finalized_group_version.max_version_level == 1, "{resp:?}");
    let finalized_transaction_version = resp
        .finalized_features
        .iter()
        .find(|f| f.name == "transaction.version")
        .expect("transaction.version finalized at bootstrap");
    assert!(
        finalized_transaction_version.max_version_level == 2,
        "{resp:?}"
    );
    // The krabka-owned `krabka.version` is advertised over [0, 1] but
    // bootstraps at level 0, which Kafka omits from the finalized features,
    // so that a JVM node can still join. An operator finalizes it later.
    let kv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "krabka.version")
        .expect("krabka.version advertised in supported_features");
    assert!((kv.min_version, kv.max_version) == (0, 1), "{resp:?}");
    assert!(
        resp.finalized_features
            .iter()
            .all(|f| f.name != "krabka.version"),
        "{resp:?}"
    );
    assert!(
        resp.finalized_features_epoch >= 0,
        "self-bootstrapped broker finalizes defaults so epoch must be >= 0: {resp:?}"
    );

    p.broker.shutdown().await;
}
