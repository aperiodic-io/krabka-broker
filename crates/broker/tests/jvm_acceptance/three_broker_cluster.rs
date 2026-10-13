//! Three-broker `SASL_PLAINTEXT` clusters.
//!
//! A third voter is what lets the JVM `kafka-leader-election` and
//! `kafka-reassign-partitions` tools move a leader off a live node, so the
//! suites that drive those tools boot the cluster here.

use {
    crate::support::{JvmListeners, controller_voters, init_jvm_tracing, jvm_broker_config},
    krabka_broker::{
        Broker, BrokerConfig,
        config::{InterBrokerCredentials, ListenerSpec},
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
};

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
#[bon::builder]
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster(
    #[builder(default = super :: sasl :: ADMIN)] admin: &'_ str,
    #[builder(default = super :: sasl :: ADMIN_PASS)] admin_pass: &'_ str,
    #[builder(default)] extra_users: &'_ [(&'_ str, &'_ str)],
    #[builder(default)] wait_until_registered: bool,
) -> SaslCluster {
    let setup = SaslClusterSetup {
        admin,
        admin_pass,
        extra_users,
    };
    let cluster = start_three_broker_sasl_plaintext_jvm_cluster_configured()
        .admin(setup.admin)
        .admin_pass(setup.admin_pass)
        .extra_users(setup.extra_users)
        .adjust(|_| {})
        .call()
        .await;
    if wait_until_registered {
        super::docker::nc_check_connectivity();
        super::wait::wait_three_brokers_registered(&cluster.0, &cluster.1, &cluster.2, 3).await;
    }
    cluster
}

#[bon::builder]
pub(crate) async fn start_sasl_cluster<const N: usize>(
    listeners: [JvmListeners; N],
    #[builder(default = super :: sasl :: ADMIN)] admin: &'_ str,
    #[builder(default = super :: sasl :: ADMIN_PASS)] admin_pass: &'_ str,
    #[builder(default)] extra_users: &'_ [(&'_ str, &'_ str)],
    adjust: impl Fn(&mut BrokerConfig),
) -> (
    [krabka_broker::BrokerHandle; N],
    [BrokerConfig; N],
    [tempfile::TempDir; N],
) {
    let setup = SaslClusterSetup {
        admin,
        admin_pass,
        extra_users,
    };
    let SaslClusterSetup {
        admin,
        admin_pass,
        extra_users,
    } = setup;
    init_jvm_tracing("krabka_broker=info");
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
        let mut config = jvm_broker_config()
            .log_dir(dirs[index].path().to_path_buf())
            .node(voters[index].0)
            .listen(listener.listen.parse().expect("client address"))
            .controller(voters[index].1)
            .advertised(listener.advertised.clone())
            .voters(controller_voters(&voters))
            .call();
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

#[bon::builder]
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_configured(
    #[builder(default = super :: sasl :: ADMIN)] admin: &'_ str,
    #[builder(default = super :: sasl :: ADMIN_PASS)] admin_pass: &'_ str,
    #[builder(default)] extra_users: &'_ [(&'_ str, &'_ str)],
    adjust: impl Fn(&mut BrokerConfig),
) -> SaslCluster {
    let setup = SaslClusterSetup {
        admin,
        admin_pass,
        extra_users,
    };
    let ([h0, h1, h2], [cfg0, cfg1, cfg2], [dir0, dir1, dir2]) = start_sasl_cluster()
        .listeners(super::ports::cluster_listeners())
        .admin(setup.admin)
        .admin_pass(setup.admin_pass)
        .extra_users(setup.extra_users)
        .adjust(adjust)
        .call()
        .await;
    (h0, h1, h2, cfg0, cfg1, cfg2, dir0, dir1, dir2)
}
