pub(crate) const DEFAULT_KAFKA_HOST: &str = "localhost";
pub(crate) const DEFAULT_KAFKA_PORT: u16 = 9092;

/// A `host:port` address split into its host and port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostPort {
    pub(crate) host: String,
    pub(crate) port: u16,
}

pub(crate) fn parse_host_port(addr: &str) -> Option<HostPort> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    Some(HostPort {
        host: host.to_string(),
        port,
    })
}

/// Split an advertised `host:port` listener address for a `BrokerEndpoint` or
/// a `FindCoordinator` row. Splits on the last `:`, so a bracketed IPv6
/// literal keeps its inner colons. A value that is not `host:port` logs a
/// warning naming it and falls back to `localhost:9092`.
pub(crate) fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    if let Some(HostPort { host, port }) = parse_host_port(addr) {
        return (host, port);
    }
    tracing::warn!(
        addr,
        "advertised listener {addr:?} is not host:port; falling back to \
         {DEFAULT_KAFKA_HOST}:{DEFAULT_KAFKA_PORT}"
    );
    (DEFAULT_KAFKA_HOST.into(), DEFAULT_KAFKA_PORT)
}

/// The host a node advertises for a listener bound to `ip`.
///
/// A concrete address is what the socket answers on, so it is the host. A
/// wildcard address names no host that a peer can dial, so the node advertises
/// its own host name, which `local_host_name` reads. Kafka does the same: its
/// `ListenerInfo.withWildcardHostnamesResolved` advertises
/// `InetAddress.getLocalHost().getCanonicalHostName()` for a wildcard listener.
/// A node that cannot read its host name advertises `127.0.0.1`, which only
/// that node can reach.
pub(crate) fn advertised_host(
    ip: std::net::IpAddr,
    local_host_name: impl FnOnce() -> Option<String>,
) -> String {
    if ip.is_unspecified() {
        local_host_name().unwrap_or_else(|| "127.0.0.1".to_owned())
    } else {
        ip.to_string()
    }
}

/// This machine's host name, as the operating system reports it, or `None`
/// when it reports none.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn local_host_name() -> Option<String> {
    hostname::get()
        .ok()?
        .into_string()
        .ok()
        .filter(|name| !name.is_empty())
}

/// WASI reports no host name. Its listeners come from the embedder, which
/// binds them, so no wildcard bind needs one.
#[cfg(target_family = "wasm")]
pub(crate) fn local_host_name() -> Option<String> { None }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A concrete bind advertises itself. A wildcard bind, IPv4 or IPv6,
    /// advertises the host name, and loopback only when there is no name.
    #[test]
    fn a_wildcard_bind_advertises_the_host_name() {
        struct Case {
            want: &'static str,
            advertised: String,
        }

        let name = || Some("ducker02".to_owned());
        let nameless = || None;
        let cases = [
            Case {
                want: "192.0.2.10",
                advertised: advertised_host("192.0.2.10".parse().unwrap(), name),
            },
            Case {
                want: "ducker02",
                advertised: advertised_host("0.0.0.0".parse().unwrap(), name),
            },
            Case {
                want: "ducker02",
                advertised: advertised_host("::".parse().unwrap(), name),
            },
            Case {
                want: "127.0.0.1",
                advertised: advertised_host("0.0.0.0".parse().unwrap(), nameless),
            },
        ];
        let advertised: Vec<&str> = cases.iter().map(|case| case.advertised.as_str()).collect();
        let expected: Vec<&str> = cases.iter().map(|case| case.want).collect();
        assert!(advertised == expected);
    }

    #[test]
    fn advertised_listener_parser_preserves_valid_host_ports_and_uses_fallback() {
        struct Case {
            input: &'static str,
            want: HostPort,
        }
        let host_port = |host: &str, port| HostPort {
            host: host.to_owned(),
            port,
        };
        let cases = [
            Case {
                input: "broker-1.example:19092",
                want: host_port("broker-1.example", 19092),
            },
            Case {
                input: "[2001:db8::7]:9094",
                want: host_port("[2001:db8::7]", 9094),
            },
            Case {
                input: "missing-port",
                want: host_port("localhost", 9092),
            },
            Case {
                input: "broker:not-a-port",
                want: host_port("localhost", 9092),
            },
        ];
        for Case { input, want } in cases {
            let (host, port) = parse_advertised_host_port(input);
            assert!(HostPort { host, port } == want, "input {input:?}");
        }
    }
}
