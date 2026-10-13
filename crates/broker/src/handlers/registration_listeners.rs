//! Validation shared by broker and controller registration listeners.

/// Authorize registration before refusing a node that is not the active controller.
pub(super) fn registration_refusal(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &super::RequestContext<'_>,
) -> Option<i16> {
    if super::cluster_action_denied(broker.config.authorizer.as_ref(), image, ctx) {
        Some(CLUSTER_AUTHORIZATION_FAILED)
    } else if !super::forward_to_controller::is_active_controller(broker) {
        Some(NOT_CONTROLLER)
    } else {
        None
    }
}

/// Borrow the fields shared by broker and controller listener registrations.
pub(super) struct ListenerFields<'a> {
    pub(super) name: &'a str,
    pub(super) host: &'a str,
    pub(super) port: u16,
    pub(super) security_protocol: i16,
}

/// Retain the caller's errors while applying Kafka's ordered listener checks.
/// Both wire listener types carry the same name, host, port and protocol fields.
pub(super) fn decode<'a, Listener, Error>(
    listeners: &'a [Listener],
    fields: impl Fn(&'a Listener) -> ListenerFields<'a>,
    empty: impl FnOnce() -> Error,
    invalid: impl Fn() -> Error,
    unknown_protocol: impl Fn() -> Error,
) -> Result<Vec<BrokerEndpoint>, Error> {
    if listeners.is_empty() {
        return Err(empty());
    }
    let mut names = HashSet::with_capacity(listeners.len());
    listeners
        .iter()
        .map(|listener| {
            let listener = fields(listener);
            if listener.name.is_empty()
                || listener.host.is_empty()
                || listener.port == 0
                || !names.insert(listener.name.to_owned())
            {
                return Err(invalid());
            }
            let protocol = match listener.security_protocol {
                0 => ListenerProtocol::Plaintext,
                1 => ListenerProtocol::Ssl,
                2 => ListenerProtocol::SaslPlaintext,
                3 => ListenerProtocol::SaslSsl,
                _ => return Err(unknown_protocol()),
            };
            Ok(BrokerEndpoint {
                name: listener.name.to_owned(),
                host: listener.host.to_owned(),
                port: listener.port,
                protocol,
            })
        })
        .collect()
}
use {
    crate::{
        broker::Broker,
        codes::{CLUSTER_AUTHORIZATION_FAILED, NOT_CONTROLLER},
    },
    krabka_metadata::BrokerEndpoint,
    krabka_security::ListenerProtocol,
    std::collections::HashSet,
};
