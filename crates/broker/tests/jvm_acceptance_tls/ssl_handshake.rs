//! The bare TLS handshake against an `SSL`-only listener.
//!
//! This is the smallest of the TLS cases and the only one with no SASL layer
//! above it, so it sits on its own: a JVM client with a JKS truststore has to
//! complete the handshake and exchange one `ApiVersions` request, and nothing
//! else in the suite proves that in isolation.

use std::process::Stdio;

use assert2::assert;

use crate::jvm_acceptance::{
    KAFKA_IMAGE, broker0_advertised, nc_check_connectivity, prepare_jks_truststore,
    start_ssl_broker, write_client_props,
};

/// End-to-end TLS handshake check against an `SSL`-only listener. The test
/// drives `kafka-broker-api-versions` from inside the cp-kafka container
/// with a JKS truststore that holds the broker's dev cert. It verifies that
/// the JVM client completes the TLS handshake and exchanges an
/// `ApiVersions` request over the encrypted channel.
///
/// The test turns off hostname verification with
/// `ssl.endpoint.identification.algorithm=`, because the CN of the dev cert
/// is `krabka-dev`, not `host.docker.internal`. The dev cert is a
/// self-signed ECDSA P-256 end-entity, regenerated from the original
/// ED25519 + CA:TRUE fixture. cp-kafka:6.1.1 ships Java 11, whose
/// `SunJSSE` does not advertise `ed25519` signature schemes during the TLS
/// handshake, so the JVM client would reject ED25519 server certs with
/// `NoSignatureSchemesInCommon`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_ssl_handshake_succeeds() {
    let (broker, _dir) = start_ssl_broker().await;
    nc_check_connectivity();

    let truststore_path = prepare_jks_truststore();

    let props = "security.protocol=SSL\n\
                 ssl.truststore.location=/truststore.jks\n\
                 ssl.truststore.password=changeit\n\
                 ssl.endpoint.identification.algorithm=\n";
    let props_tmp = write_client_props(props);
    let ts_mount = format!("{}:/truststore.jks:ro", truststore_path.display());

    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: KAFKA_IMAGE,
        mounts: &[&props_tmp.mount_str(), &ts_mount],
        args: &[
            "kafka-broker-api-versions",
            "--bootstrap-server",
            broker0_advertised(),
            "--command-config",
            "/client.properties",
        ],
        ..Default::default()
    })
    .stderr(Stdio::piped())
    .stdout(Stdio::piped())
    .output()
    .expect("spawn kafka-broker-api-versions");
    eprintln!(
        "KRABKA[test] ssl api-versions status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        out.status.success(),
        "ssl handshake failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    broker.shutdown().await;
}
