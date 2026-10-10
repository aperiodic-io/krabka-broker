//! Two-broker clusters that authenticate their own inter-broker traffic.
//!
//! The pair shares one SASL credential and dials its peer through the
//! advertised `host.docker.internal` name, which is the same address the JVM
//! containers use, so one metadata response serves both.

/// Host port assignments for the two-broker JVM inter-broker test. The
/// `SASL_PLAINTEXT` listener of broker 0 binds an allocated port (advertised as
/// an allocated port) and broker 1 binds an allocated port
/// (advertised as an allocated port). Inter-broker traffic flows
/// over the same listeners. Each broker uses the host's resolver to resolve
type TwoSaslBrokers = (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
);

async fn start_configured(
    admin: &str,
    password: &str,
    adjust: impl Fn(&mut krabka_broker::BrokerConfig),
) -> TwoSaslBrokers {
    let ([first, second], _, [first_dir, second_dir]) =
        super::three_broker_cluster::start_sasl_cluster(
            super::ports::cluster_listeners(),
            crate::jvm_acceptance::SaslClusterSetup {
                admin,
                admin_pass: password,
                ..Default::default()
            },
            adjust,
        )
        .await;
    (first, second, first_dir, second_dir)
}

/// Spawn two in-process brokers that share a single inter-broker SASL
/// credential. Each broker has one `SASL_PLAINTEXT` listener. Both set
/// `plain_credentials[admin] = admin_pass`, so each broker can authenticate
/// to the other with the same admin identity. The inter-broker listener
/// name on both is `"SASL_PLAINTEXT"`, so the broker peers dial each
/// other's advertised host. This function sets that host to
/// `host.docker.internal:<port>`, so the JVM containers can use the same
/// metadata response.
pub(crate) async fn start_two_sasl_brokers(admin: &str, admin_pass: &str) -> TwoSaslBrokers {
    start_configured(admin, admin_pass, |_| {}).await
}

/// Spawn two in-process brokers that share an inter-broker SASL
/// credential AND both terminate TLS on the data plane and the controller
/// quorum listener. Mirrors [`start_two_sasl_brokers`] but with the
/// `SASL_SSL` listener protocol and `controller_listener_protocol = ctrl`,
/// which is usually `ListenerProtocol::SaslSsl`. Each broker advertises
/// `host.docker.internal:<port>` so the JVM containers can reach them with
/// `--add-host=host.docker.internal:host-gateway` AND so each broker can
/// dial its peer with the same host name.
pub(crate) async fn start_two_sasl_ssl_brokers_with_controller_protocol(
    ctrl_protocol: krabka_security::ListenerProtocol,
    admin: &str,
    admin_pass: &str,
) -> TwoSaslBrokers {
    use krabka_security::{ClientAuthMode, SaslMechanism, TlsConfig};

    let security = crate::support::manifest_dir().join("tests/fixtures/security");
    let cert_path = security.join("dev_cert.pem");
    start_configured(admin, admin_pass, |config| {
        let listener = &mut config.listeners[0];
        listener.name = "SASL_SSL".into();
        listener.protocol = krabka_security::ListenerProtocol::SaslSsl;
        config.inter_broker_listener_name = "SASL_SSL".into();
        config.controller_listener_protocol = ctrl_protocol;
        // TLS adds handshake round trips to both controller and data traffic.
        config.controller_election_timeout = krabka_units::secs(8);
        config.tls_config = Some(TlsConfig {
            cert_chain_path: cert_path.clone(),
            private_key_path: security.join("dev_key.pem"),
            trust_roots_path: Some(cert_path.clone()),
            client_ca_path: None,
            client_auth: ClientAuthMode::Disabled,
        });
        config
            .enabled_sasl_mechanisms
            .push(SaslMechanism::ScramSha512);
    })
    .await
}
