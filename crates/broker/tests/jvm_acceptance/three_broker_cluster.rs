//! Three-broker `SASL_PLAINTEXT` clusters.
//!
//! A third voter is what lets the JVM `kafka-leader-election` and
//! `kafka-reassign-partitions` tools move a leader off a live node, so the
//! suites that drive those tools boot the cluster here.

use krabka_broker::{Broker, BrokerConfig};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct SaslClusterSetup<'a> {
    #[default(super::sasl::ADMIN)]
    pub admin: &'a str,
    #[default(super::sasl::ADMIN_PASS)]
    pub admin_pass: &'a str,
    pub extra_users: &'a [(&'a str, &'a str)],
}

pub(crate) type SaslCluster = (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
);

/// Boot and probe all voters before administering a registered SASL cluster.
pub(crate) async fn start_registered_sasl_cluster(setup: SaslClusterSetup<'_>) -> SaslCluster {
    let cluster = start_three_broker_sasl_plaintext_jvm_cluster_with_users(setup).await;
    super::docker::nc_check_connectivity();
    super::wait::wait_three_brokers_registered(&cluster.0, &cluster.1, &cluster.2, 3).await;
    cluster
}

/// Third broker for the 3-broker `SASL_PLAINTEXT` JVM cluster.
/// Broker 2 (`node_id`=2) lives on `broker1_listen()` / `broker1_advertised()`.
/// Spawn three in-process brokers that share one inter-broker SASL credential.
///
/// * Broker 1: 0.0.0.0:9092 (data) / 0.0.0.0:9093 (controller)
/// * Broker 2: 0.0.0.0:9094 (data) / 0.0.0.0:9095 (controller)
/// * Broker 3: 0.0.0.0:9096 (data) / 0.0.0.0:9097 (controller)
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
/// A caller needs the `cfg*` values to revive a broker after shutdown.
/// Pass them with `BootstrapMode::Rejoin`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster(
    setup: SaslClusterSetup<'_>,
) -> SaslCluster {
    start_three_broker_sasl_plaintext_jvm_cluster_with_users(setup).await
}

/// Like [`start_three_broker_sasl_plaintext_jvm_cluster`] but also provisions
/// `extra_users` as PLAIN credentials on all three brokers.
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_with_users(
    setup: SaslClusterSetup<'_>,
) -> SaslCluster {
    start_three_broker_sasl_plaintext_jvm_cluster_configured(setup, |_| {}).await
}

pub(crate) async fn start_sasl_cluster<const N: usize>(
    listeners: [crate::support::JvmListeners; N],
    setup: SaslClusterSetup<'_>,
    adjust: impl Fn(&mut BrokerConfig),
) -> (
    [krabka_broker::BrokerHandle; N],
    [BrokerConfig; N],
    [tempfile::TempDir; N],
) {
    use krabka_broker::config::{InterBrokerCredentials, ListenerSpec};
    use krabka_security::{ListenerProtocol, SaslMechanism};

    let SaslClusterSetup {
        admin,
        admin_pass,
        extra_users,
    } = setup;
    crate::support::init_jvm_tracing("krabka_broker=info");
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dirs: [tempfile::TempDir; N] =
        std::array::from_fn(|_| tempfile::tempdir().expect("broker directory"));
    let voters: Vec<_> = listeners
        .iter()
        .enumerate()
        .map(|(index, listener)| {
            (
                u64::try_from(index + 1).expect("node id"),
                listener.controller.parse().expect("controller address"),
            )
        })
        .collect();
    let configs: [BrokerConfig; N] = std::array::from_fn(|index| {
        let listener = &listeners[index];
        let mut config = crate::support::jvm_broker_config(
            dirs[index].path().to_path_buf(),
            crate::support::JvmBrokerSetup {
                node: krabka_broker::NodeId(voters[index].0),
                listen: listener.listen.parse().expect("client address"),
                controller: voters[index].1,
                advertised: listener.advertised.clone(),
                voters: crate::support::controller_voters(&voters),
            },
        );
        config.listeners = vec![ListenerSpec {
            advertised: listener.advertised.clone(),
            ..crate::support::listeners::listener(
                "SASL_PLAINTEXT",
                config.listen_addr,
                ListenerProtocol::SaslPlaintext,
            )
        }];
        config.inter_broker_listener_name = "SASL_PLAINTEXT".into();
        config.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        config.super_users.insert(admin.into());
        config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: admin.into(),
            password: admin_pass.into(),
        });
        config
            .plain_credentials
            .insert(admin.into(), admin_pass.into());
        for (user, password) in extra_users {
            config
                .plain_credentials
                .insert((*user).into(), (*password).into());
        }
        adjust(&mut config);
        config.authorizer = std::sync::Arc::new(
            krabka_broker::authorizer::SimpleAclAuthorizer::new(config.super_users.clone()),
        );
        config
    });
    // Every static voter must start before awaiting any one node's election.
    let starts = configs.clone().map(|config| {
        tokio::spawn(async move { Broker::start(config).await.expect("broker start") })
    });
    let mut brokers = Vec::with_capacity(N);
    for start in starts {
        brokers.push(start.await.expect("broker start task"));
    }
    (
        brokers
            .try_into()
            .unwrap_or_else(|_| panic!("broker count")),
        configs,
        dirs,
    )
}

pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_configured(
    setup: SaslClusterSetup<'_>,
    adjust: impl Fn(&mut BrokerConfig),
) -> SaslCluster {
    let ([h0, h1, h2], [cfg0, cfg1, cfg2], [dir0, dir1, dir2]) =
        start_sasl_cluster(super::ports::cluster_listeners(), setup, adjust).await;
    (h0, h1, h2, cfg0, cfg1, cfg2, dir0, dir1, dir2)
}
