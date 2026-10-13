//! Listener fixtures with explicit security protocols and advertised addresses.

use {
    krabka_broker::{SslPrincipalMapper, config::ListenerSpec},
    krabka_security::ListenerProtocol,
    std::net::SocketAddr,
};

pub fn listener(name: &str, addr: SocketAddr, protocol: ListenerProtocol) -> ListenerSpec {
    ListenerSpec {
        name: name.to_owned(),
        bind_addr: addr,
        advertised: addr.to_string(),
        protocol,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: SslPrincipalMapper::default(),
    }
}

pub fn loopback_listener(name: &str, protocol: ListenerProtocol) -> ListenerSpec {
    listener(name, "127.0.0.1:0".parse().unwrap(), protocol)
}
