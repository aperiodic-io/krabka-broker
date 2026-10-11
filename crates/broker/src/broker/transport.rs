//! Process-wide TLS setup and the shared inter-broker client, built before any
//! other startup phase because the metadata quorum, the replicator, and the
//! heartbeat loops all dial through them. It is its own module so the crypto
//! provider install, the kTLS probe, and the dialer construction stay in one
//! place instead of at the head of the startup sequence.

use std::sync::Arc;

use crate::{
    config::BrokerConfig,
    error::BrokerError,
    network::{client::InterBrokerClient, ktls_probe::probe_ktls_support},
};

pub(super) struct StartupTransport {
    pub(super) tls_dynamic: Option<Arc<krabka_security::DynamicServerConfig>>,
    pub(super) ktls_enabled: bool,
    pub(super) inter_broker_client: Arc<InterBrokerClient>,
}

pub(super) async fn prepare_startup_transport(
    config: &BrokerConfig,
) -> Result<StartupTransport, BrokerError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    config.validate()?;
    let tls_dynamic = config
        .tls_config
        .as_ref()
        .map(krabka_security::DynamicServerConfig::from_tls_config)
        .transpose()
        .map_err(|error| BrokerError::Tls(error.to_string()))?;
    let ktls_enabled = if tls_dynamic.is_some() {
        probe_ktls_support().await
    } else {
        false
    };
    match (ktls_enabled, tls_dynamic.is_some()) {
        (true, _) => tracing::info!(
            "Linux kTLS supported: TLS fetch connections will use kernel-offloaded sendfile"
        ),
        (false, true) => {
            tracing::info!("Linux kTLS unavailable: TLS fetch connections use userspace rustls");
        }
        (false, false) => {}
    }
    let tls_connector = config
        .tls_config
        .as_ref()
        .map(krabka_security::TlsConfig::build_client_config_with_identity)
        .transpose()
        .map_err(|error| BrokerError::Tls(error.to_string()))?
        .map(tokio_rustls::TlsConnector::from);
    let inter_broker_client = Arc::new(InterBrokerClient::new_with_policy(
        tls_connector,
        config.inter_broker_credentials.clone(),
        config.client_dispatch_queue_capacity,
        config.client_frame_max,
        config.socket_send_buffer,
        config.socket_receive_buffer,
    ));
    Ok(StartupTransport {
        tls_dynamic,
        ktls_enabled,
        inter_broker_client,
    })
}
