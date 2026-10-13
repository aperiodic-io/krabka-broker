//! The listener grammar of a controller registration.
//!
//! Every endpoint must be a named, hosted, non-zero port on a security protocol
//! the wire defines, with no repeated name and at least one entry.

use {
    krabka_metadata::BrokerEndpoint, krabka_protocol::owned::controller_registration_request,
    krabka_security::ListenerProtocol, std::collections::HashSet,
};

pub(super) fn decode_controller_listeners(
    listeners: &[controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    decode_listeners(listeners.iter().map(|listener| {
        (
            listener.name.as_str(),
            listener.host.as_str(),
            listener.port,
            listener.security_protocol,
        )
    }))
}

fn decode_listeners<'a>(
    listeners: impl Iterator<Item = (&'a str, &'a str, u16, i16)>,
) -> Result<Vec<BrokerEndpoint>, String> {
    let mut names = HashSet::new();
    let endpoints = listeners
        .map(|(name, host, port, protocol)| {
            if name.is_empty() || host.is_empty() || port == 0 || !names.insert(name.to_owned()) {
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
        })
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
    #[test]
    fn listener_protocol_numbers_map_to_their_protocols() {
        let cases = [
            (0i16, Some(ListenerProtocol::Plaintext)),
            (1, Some(ListenerProtocol::Ssl)),
            (2, Some(ListenerProtocol::SaslPlaintext)),
            (3, Some(ListenerProtocol::SaslSsl)),
            (4, None),
            (-1, None),
            (i16::MAX, None),
        ];
        for (wire, want) in cases {
            check!(protocol_from_wire(wire) == want, "protocol {wire}");
        }
    }

    /// A listener set is rejected when any entry is unusable or repeats a
    /// name, and an empty set is rejected outright.
    #[test]
    fn registration_listeners_must_be_usable_and_uniquely_named() {
        type Row<'a> = (&'a str, Vec<(&'a str, &'a str, u16, i16)>, bool);
        let cases: Vec<Row<'_>> = vec![
            (
                "one usable listener",
                vec![("PLAINTEXT", "host", 9092, 0)],
                true,
            ),
            (
                "two, differently named",
                vec![("PLAINTEXT", "host", 9092, 0), ("SSL", "host", 9093, 1)],
                true,
            ),
            ("none at all", vec![], false),
            ("a nameless listener", vec![("", "host", 9092, 0)], false),
            (
                "a hostless listener",
                vec![("PLAINTEXT", "", 9092, 0)],
                false,
            ),
            ("port zero", vec![("PLAINTEXT", "host", 0, 0)], false),
            (
                "a repeated name",
                vec![
                    ("PLAINTEXT", "host", 9092, 0),
                    ("PLAINTEXT", "host", 9093, 0),
                ],
                false,
            ),
            (
                "an unknown security protocol",
                vec![("PLAINTEXT", "host", 9092, 9)],
                false,
            ),
        ];
        for (what, listeners, accepted) in cases {
            let got = decode_listeners(listeners.iter().map(|&(n, h, p, proto)| (n, h, p, proto)));
            check!(got.is_ok() == accepted, "{what}: {got:?}");
        }
    }
}
