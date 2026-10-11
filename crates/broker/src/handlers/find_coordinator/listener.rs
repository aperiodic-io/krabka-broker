//! Resolving the local broker's advertised address for the listener a
//! `FindCoordinator` request arrived on.
//!
//! Kafka answers with the connection listener's advertised address, so a TLS
//! client is pointed at the TLS listener and a plaintext client at the
//! plaintext one. That per-listener choice, and its fallback rules, live here
//! rather than in the request handling that consumes the result.

use crate::config::BrokerConfig;

/// The local broker's advertised `host:port` string for the request's listener.
///
/// Kafka returns the connection listener's advertised address, so a TLS client
/// gets the TLS listener's `advertised` and a plaintext client gets the
/// plaintext one. The function falls back to the legacy top-level
/// `advertised_listener` when the connection listener is not one of this
/// broker's configured listeners. The single-listener default is one such case,
/// where `connection_listener_name == "PLAINTEXT"` still resolves it.
///
/// A matched listener whose advertised port is `0` is unusable as a coordinator
/// address. That port is OS-assigned and dynamic, and it is common in test
/// harnesses. In that case the function falls back to `advertised_listener`,
/// which `Broker::start` rewrites to the real bound port after it binds.
pub(super) fn local_advertised_for_listener(
    config: &BrokerConfig,
    connection_listener_name: &str,
) -> String {
    config
        .effective_listeners()
        .into_iter()
        .find(|l| l.name == connection_listener_name && !l.advertised.ends_with(":0"))
        .map_or_else(|| config.advertised_listener.clone(), |l| l.advertised)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{
        SslPrincipalMapper,
        config::{BrokerConfig, ListenerSpec},
    };

    fn listener(name: &str, advertised: &str) -> ListenerSpec {
        ListenerSpec {
            name: name.to_string(),
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            advertised: advertised.to_string(),
            protocol: krabka_security::ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
            principal_mapper: SslPrincipalMapper::default(),
        }
    }

    /// A request on the `"tls"` listener resolves the local coordinator to the
    /// tls listener's advertised address. A request on `"plain"` resolves to the
    /// plain listener's address.
    #[test]
    fn local_advertised_tracks_connection_listener() {
        let config = BrokerConfig {
            advertised_listener: "legacy:1000".to_string(),
            listeners: vec![
                listener("plain", "plain-host:9092"),
                listener("tls", "tls-host:9094"),
            ],
            ..Default::default()
        };
        assert!(local_advertised_for_listener(&config, "tls") == "tls-host:9094");
        assert!(local_advertised_for_listener(&config, "plain") == "plain-host:9092");
    }

    /// When the connection listener is not configured, the function falls back
    /// to the legacy top-level `advertised_listener`.
    #[test]
    fn local_advertised_falls_back_to_legacy() {
        let config = BrokerConfig {
            advertised_listener: "legacy:1000".to_string(),
            listeners: vec![listener("plain", "plain-host:9092")],
            ..Default::default()
        };
        assert!(local_advertised_for_listener(&config, "external") == "legacy:1000");
    }
}
