//! KIP-1022 `krabka format --feature` end-to-end test. A standalone-formatted
//! log dir whose feature levels came from `--feature` boots a broker that
//! finalizes exactly those levels and surfaces them through `ApiVersions`.
//!
//! `--standalone` is load-bearing. It writes a `VotersRecord`, so the broker
//! uses the formatted voter set and *skips* the in-process self-bootstrap that
//! would otherwise re-seed every feature at the latest release. That proves the
//! `--feature` overrides survive boot and nothing overwrites them.

use {
    crate::support::{client::connect_owned, discovery::api_versions_request_for},
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig},
};

mod support;

/// Run `krabka format --standalone … --feature …` as a subprocess. The test
/// shells out through `env!("CARGO")`, because this crate does not get the
/// `krabka-format` `CARGO_BIN_EXE_*` variable. The dev-dep keeps the
/// Formats a standalone log directory with explicit `--feature` overrides.
///
/// Called in process rather than spawned: the formatting is setup for the
/// `ApiVersions` assertion below, not the thing under test, and a subprocess would
/// need a Cargo working tree to build from, which a Bazel test sandbox does not
/// have. `krabka-format`'s own `format_smoke` suite runs the real binary.
async fn run_krabka_format_with_features(
    log_dir: &std::path::Path,
    node_id: u64,
    controller_listener: &str,
    features: &[&str],
) {
    let mut argv = vec![
        "krabka-format".to_string(),
        "--log-dir".to_string(),
        log_dir.to_str().unwrap().to_string(),
        "--standalone".to_string(),
        "--node-id".to_string(),
        node_id.to_string(),
        "--controller-listener".to_string(),
        controller_listener.to_string(),
    ];
    for f in features {
        argv.push("--feature".to_string());
        argv.push((*f).to_string());
    }
    let code = krabka_format::run_from_args(argv).await;
    assert!(code == 0, "krabka-format exited {code}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standalone_format_feature_overrides_surface_in_api_versions() {
    support::init_tracing();

    // Pre-bind a concrete controller port so it can be baked into the
    // VotersRecord at format time and re-bound by the broker on boot.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let client_addr = client_addrs[0];
    let controller_addr = controller_addrs[0];

    let dir = tempfile::tempdir().unwrap();
    // `krabka format` creates the dir itself and refuses a non-empty one.
    let boot_dir = dir.path().join("boot");
    run_krabka_format_with_features(
        &boot_dir,
        1,
        &controller_addr.to_string(),
        // transaction.version pinned to 1 (default at the latest release is 2);
        // group.version pinned to 0 (default is 1) → omitted → not finalized.
        &["transaction.version=1", "group.version=0"],
    )
    .await;

    let mut cfg = BrokerConfig::for_tests(boot_dir.clone());
    cfg.broker_id = 1;
    cfg.node_id = krabka_broker::NodeId(1);
    cfg.listen_addr = client_addr;
    cfg.advertised_listener = client_addr.to_string();
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(krabka_broker::NodeId(1), controller_addr.to_string())];
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Bootstrap;

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let handle = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker start");
    let bootstrap = handle.listen_addr().to_string();
    let client = connect_owned(&bootstrap, "fmt-feature-test", "client build").await;

    let av = client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    assert!(av.error_code == 0, "{av:?}");

    let finalized = |name: &str| {
        av.finalized_features
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.max_version_level)
    };

    // transaction.version=1 took effect (not the release default 2);
    // group.version=0 → omitted → not finalized at all;
    // metadata.version was not overridden → Kafka 4.3's latest production.
    for (feature, want) in [
        ("transaction.version", Some(1)),
        ("group.version", None),
        (
            "metadata.version",
            Some(krabka_format::LATEST_PRODUCTION_METADATA_VERSION),
        ),
    ] {
        check!(
            finalized(feature) == want,
            "{feature} must be finalized at {want:?}: {:?}",
            av.finalized_features
        );
    }

    handle.shutdown().await;
}
