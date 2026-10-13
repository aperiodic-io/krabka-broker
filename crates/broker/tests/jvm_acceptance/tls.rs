//! Single-broker bring-up for the TLS-terminating listeners, `SSL` and
//! `SASL_SSL`.
//!
//! The JVM client needs the dev certificate in a JKS truststore before it can
//! complete either handshake, so the keytool round-trip that builds one lives
//! here too.

use {
    super::{docker::KAFKA_IMAGE, ports::broker0_advertised},
    crate::support::{fixture_cache_dir, manifest_dir},
    assert2::assert,
    krabka_broker::BrokerConfig,
    std::process::Command,
};

/// Spawn the broker with a single `SSL` listener on an allocated port
/// (advertised as an allocated port) with the dev cert/key from
/// `crates/broker/tests/fixtures/security/`. No SASL. Mirrors
/// [`start_host_broker`] otherwise, but flips the protocol to `Ssl` and
/// supplies a [`TlsConfig`].
pub(crate) async fn start_ssl_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    super::broker::start_host_broker_with(|config| {
        configure_tls(config, krabka_security::ListenerProtocol::Ssl);
        let tls = config.tls_config.as_ref().expect("TLS config");
        assert!(
            tls.cert_chain_path.exists(),
            "dev cert missing at {}",
            tls.cert_chain_path.display()
        );
        assert!(
            tls.private_key_path.exists(),
            "dev key missing at {}",
            tls.private_key_path.display()
        );
    })
    .await
}

/// Build a JKS truststore from the dev cert PEM. This function runs
/// `keytool` inside a one-shot Docker container. It returns the host-side
/// path to a `ts.jks` file, chmod `0644` so the non-root user of the
/// cp-kafka container can read it once it is bind-mounted.
///
/// The result is cached under `<tmp>/krabka-jvm-truststore-<fixture digest>/ts.jks`, so later
/// calls from this test and from the `SASL_SSL` test skip the keytool
/// round-trip.
///
/// The cp-kafka:6.1.1 image ships its own JRE and `keytool` binary, so this
/// function reuses them with `--entrypoint keytool` instead of pulling
/// `openjdk:17`. The image is always on disk, because the SSL test itself
/// runs `kafka-broker-api-versions` from the same image.
pub(crate) fn prepare_jks_truststore() -> std::path::PathBuf {
    let cache_dir = fixture_cache_dir("krabka-jvm-truststore", &["dev_cert.pem"]);
    std::fs::create_dir_all(&cache_dir).expect("mkdir truststore cache");
    let ts_path = cache_dir.join("ts.jks");

    // Stage the cert in the cache dir so the bind mount is a directory we
    // control. This sidesteps mount-path quoting on /mnt/c under WSL.
    let manifest_dir = manifest_dir();
    let cert_src = manifest_dir
        .join("tests")
        .join("fixtures")
        .join("security")
        .join("dev_cert.pem");
    let cert_staged = cache_dir.join("dev_cert.pem");
    std::fs::copy(&cert_src, &cert_staged).expect("copy dev_cert.pem to cache");

    if !ts_path.exists() {
        let mount = format!("{}:/work", cache_dir.display());
        // Run keytool + chmod as root inside the container so the host
        // file ends up world-readable. `--user 0:0` lets keytool create
        // `/work/ts.jks` regardless of host-dir owner (CI runner-owned
        // tmpdir blocks cp-kafka's non-root default user). The `chmod
        // 0644` is inside the container too because the file is owned
        // by root on the host once keytool runs as root, so the host-side
        // runner user can't chmod it later.
        let inner = "set -e; \
             keytool -import -alias krabka -file /work/dev_cert.pem \
                 -keystore /work/ts.jks -storepass changeit -noprompt && \
             chmod 0644 /work/ts.jks";
        let out = Command::new("docker")
            .args([
                "run",
                "--rm",
                "--user",
                "0:0",
                "-v",
                &mount,
                "--entrypoint",
                "bash",
                KAFKA_IMAGE,
                "-c",
                inner,
            ])
            .output()
            .expect("spawn keytool");
        assert!(
            out.status.success(),
            "keytool import failed: stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        assert!(
            ts_path.exists(),
            "keytool reported success but ts.jks missing at {}",
            ts_path.display(),
        );
    }

    ts_path
}

/// Spawn the broker with a single `SASL_SSL` listener. The listener enables
/// the PLAIN and SCRAM-SHA-512 mechanisms, uses the dev cert/key for TLS,
/// and gets `admin` as the super-user PLAIN identity, so `admin` can call
/// `AlterUserScramCredentials` to provision SCRAM users.
///
/// This is the dual-mech broker from [`start_dual_mech_broker`] flipped
/// from `SaslPlaintext` to `SaslSsl` with a `TlsConfig` attached. That is
/// the production-shape listener configuration.
pub(crate) fn start_sasl_ssl_broker(
    admin: &str,
    admin_pass: &str,
) -> impl std::future::Future<Output = (krabka_broker::BrokerHandle, tempfile::TempDir)> {
    super::broker::start_host_broker_with(|config| {
        super::sasl::configure_sasl(config, &[(admin, admin_pass)], Some(admin));
        config
            .enabled_sasl_mechanisms
            .push(krabka_security::SaslMechanism::ScramSha512);
        configure_tls(config, krabka_security::ListenerProtocol::SaslSsl);
    })
}

fn configure_tls(config: &mut BrokerConfig, protocol: krabka_security::ListenerProtocol) {
    use {
        krabka_broker::config::ListenerSpec,
        krabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig},
    };

    let name = if protocol == ListenerProtocol::SaslSsl {
        "SASL_SSL"
    } else {
        "SSL"
    };
    let security = manifest_dir().join("tests/fixtures/security");
    config.listeners = vec![ListenerSpec {
        name: name.into(),
        bind_addr: config.listen_addr,
        advertised: broker0_advertised().into(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    config.inter_broker_listener_name = name.into();
    config.tls_config = Some(TlsConfig {
        cert_chain_path: security.join("dev_cert.pem"),
        private_key_path: security.join("dev_key.pem"),
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });
}
