pub(crate) const DEFAULT_KAFKA_HOST: &str = "localhost";
pub(crate) const DEFAULT_KAFKA_PORT: u16 = 9092;

pub(crate) fn parse_host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    Some((host.to_string(), port))
}

/// Split an advertised `host:port` listener address for a `BrokerEndpoint` or
/// a `FindCoordinator` row. Splits on the last `:`, so a bracketed IPv6
/// literal keeps its inner colons. A value that is not `host:port` logs a
/// warning naming it and falls back to `localhost:9092`.
pub(crate) fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    if let Some(host_port) = parse_host_port(addr) {
        return host_port;
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
        let name = || Some("ducker02".to_owned());
        let nameless = || None;
        let cases = [
            (
                "192.0.2.10",
                advertised_host("192.0.2.10".parse().unwrap(), name),
            ),
            (
                "ducker02",
                advertised_host("0.0.0.0".parse().unwrap(), name),
            ),
            ("ducker02", advertised_host("::".parse().unwrap(), name)),
            (
                "127.0.0.1",
                advertised_host("0.0.0.0".parse().unwrap(), nameless),
            ),
        ];
        let advertised: Vec<&str> = cases.iter().map(|(_, host)| host.as_str()).collect();
        let expected: Vec<&str> = cases.iter().map(|(want, _)| *want).collect();
        assert!(advertised == expected);
    }

    #[test]
    fn advertised_listener_parser_preserves_valid_host_ports_and_uses_fallback() {
        let cases = [
            ("broker-1.example:19092", ("broker-1.example", 19092)),
            ("[2001:db8::7]:9094", ("[2001:db8::7]", 9094)),
            ("missing-port", ("localhost", 9092)),
            ("broker:not-a-port", ("localhost", 9092)),
        ];
        for (input, (host, port)) in cases {
            assert!(
                parse_advertised_host_port(input) == (host.to_string(), port),
                "input {input:?}"
            );
        }
    }
}
