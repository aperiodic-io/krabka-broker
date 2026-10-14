//! The listener grammar of a controller registration.
//!
//! Every endpoint must be a named, hosted, non-zero port on a security protocol
//! the wire defines, with no repeated name and at least one entry.

use {
    krabka_metadata::BrokerEndpoint, krabka_protocol::owned::controller_registration_request,
    krabka_security::ListenerProtocol, std::collections::HashSet,
};

/// A listener as the registration request carries it, borrowed from the
/// request.
#[derive(Clone, Copy, Debug)]
struct WireListener<'a> {
    name: &'a str,
    host: &'a str,
    port: u16,
    /// The wire code of the security protocol.
    protocol: i16,
}

pub(super) fn decode_controller_listeners(
    listeners: &[controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    decode_listeners(listeners.iter().map(|listener| WireListener {
        name: listener.name.as_str(),
        host: listener.host.as_str(),
        port: listener.port,
        protocol: listener.security_protocol,
    }))
}

fn decode_listeners<'a>(
    listeners: impl Iterator<Item = WireListener<'a>>,
) -> Result<Vec<BrokerEndpoint>, String> {
    let mut names = HashSet::new();
    let endpoints = listeners
        .map(
            |WireListener {
                 name,
                 host,
                 port,
                 protocol,
             }| {
                if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name.to_owned())
                {
                    return Err("invalid or duplicate registration listener".into());
                }
                let protocol = protocol_from_wire(protocol)
                    .ok_or_else(|| "unknown listener security protocol".to_owned())?;
                Ok(BrokerEndpoint {
                    name: name.to_owned(),
                    host: host.to_owned(),
                    port,
                    protocol,
                })
            },
        )
        .collect::<Result<Vec<_>, String>>()?;
    if endpoints.is_empty() {
        return Err("registration has no listeners".into());
    }
    Ok(endpoints)
}

fn protocol_from_wire(protocol: i16) -> Option<ListenerProtocol> {
    match protocol {
        0 => Some(ListenerProtocol::Plaintext),
        1 => Some(ListenerProtocol::Ssl),
        2 => Some(ListenerProtocol::SaslPlaintext),
        3 => Some(ListenerProtocol::SaslSsl),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    fn registration_listener(
        name: &'static str,
        host: &'static str,
        port: u16,
        protocol: i16,
    ) -> WireListener<'static> {
        WireListener {
            name,
            host,
            port,
            protocol,
        }
    }

    /// The controller listener decoder reads the wire listener and runs the
    /// shared checks.
    #[test]
    fn the_controller_listener_decoder_rejects_an_unusable_listener() {
        let controller_bad = vec![controller_registration_request::Listener {
            name: "CONTROLLER".to_owned(),
            host: String::new(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        }];
        check!(decode_controller_listeners(&controller_bad).is_err());

        let controller_ok = vec![controller_registration_request::Listener {
            name: "CONTROLLER".to_owned(),
            host: "host".to_owned(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        }];
        let decoded = decode_controller_listeners(&controller_ok).expect("a usable listener");
        check!(decoded.len() == 1 && decoded[0].port == 9093);
    }

    /// The wire's security-protocol numbering, and nothing outside it.
    #[yare::parameterized(
        plaintext = { 0i16, Some(ListenerProtocol::Plaintext) },
        ssl = { 1, Some(ListenerProtocol::Ssl) },
        sasl_plaintext = { 2, Some(ListenerProtocol::SaslPlaintext) },
        sasl_ssl = { 3, Some(ListenerProtocol::SaslSsl) },
        unknown = { 4, None },
        negative = { -1, None },
        maximum = { i16::MAX, None },
    )]
    fn listener_protocol_numbers_map_to_their_protocols(
        wire: i16,
        expected: Option<ListenerProtocol>,
    ) {
        check!(protocol_from_wire(wire) == expected, "protocol {wire}");
    }

    /// A listener set is rejected when any entry is unusable or repeats a
    /// name, and an empty set is rejected outright.
    #[yare::parameterized(
        one_listener = {
            "one usable listener",
            vec![registration_listener("PLAINTEXT", "host", 9092, 0)],
            true
        },
        distinct_listeners = {
            "two, differently named",
            vec![
                registration_listener("PLAINTEXT", "host", 9092, 0),
                registration_listener("SSL", "host", 9093, 1),
            ],
            true,
        },
        empty = { "none at all", vec![], false },
        nameless = {
            "a nameless listener",
            vec![registration_listener("", "host", 9092, 0)],
            false
        },
        hostless = {
            "a hostless listener",
            vec![registration_listener("PLAINTEXT", "", 9092, 0)],
            false
        },
        zero_port = {
            "port zero",
            vec![registration_listener("PLAINTEXT", "host", 0, 0)],
            false
        },
        duplicate_name = {
            "a repeated name",
            vec![
                registration_listener("PLAINTEXT", "host", 9092, 0),
                registration_listener("PLAINTEXT", "host", 9093, 0),
            ],
            false,
        },
        unknown_protocol = {
            "an unknown security protocol",
            vec![registration_listener("PLAINTEXT", "host", 9092, 9)],
            false
        },
    )]
    fn registration_listeners_must_be_usable_and_uniquely_named(
        case: &str,
        listeners: Vec<WireListener<'static>>,
        accepted: bool,
    ) {
        let got = decode_listeners(listeners.iter().copied());
        check!(got.is_ok() == accepted, "{case}: {got:?}");
    }
}
