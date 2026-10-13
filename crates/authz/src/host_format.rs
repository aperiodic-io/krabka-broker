//! Renders an [`IpAddr`] the way the JDK's `InetAddress.getHostAddress()`
//! renders it, because that is the text Kafka compares ACL hosts against.
//!
//! Rust's [`std::fmt::Display`] impl for [`Ipv6Addr`] and [`IpAddr`] favors
//! the RFC 5952 compressed form (`::1`), and keeps an IPv4-mapped IPv6 peer
//! (`::ffff:10.0.0.5`) as an IPv6 address. The JDK does neither:
//!
//! - `Inet6Address.getHostAddress()` writes all eight 16-bit groups in lower
//!   hex, colon-separated, with no leading zeros dropped from the group count
//!   and no `::` zero-compression, e.g. loopback is `0:0:0:0:0:0:0:1`.
//! - The socket layer hands the JDK an `Inet4Address` for an IPv4-mapped IPv6
//!   peer, so `getHostAddress()` on that peer is the dotted-quad form, e.g.
//!   `10.0.0.5`, not `::ffff:10.0.0.5`.
//!
//! [`jdk_host_address`] reproduces both: [`IpAddr::to_canonical`] performs the
//! IPv4-mapped unwrap, then a genuine IPv6 address is rendered group by group.

use std::{
    fmt::Write as _,
    net::{IpAddr, Ipv6Addr},
};

/// Formats `ip` the way `InetAddress.getHostAddress()` would, so it can be
/// compared against an ACL host string written by Kafka tooling.
#[must_use]
pub fn jdk_host_address(ip: IpAddr) -> String {
    match ip.to_canonical() {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format_ipv6_groups(v6),
    }
}

/// Eight lower-hex groups, colon-separated, no zero-compression -- the form
/// `Inet6Address.getHostAddress()` uses for a genuine (non-mapped) IPv6
/// address.
fn format_ipv6_groups(addr: Ipv6Addr) -> String {
    let segments = addr.segments();
    let mut out = String::with_capacity(39);
    for (i, segment) in segments.iter().enumerate() {
        if i > 0 {
            out.push(':');
        }
        let _ = write!(out, "{segment:x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use {
        super::jdk_host_address,
        std::net::{IpAddr, Ipv4Addr, Ipv6Addr},
    };

    #[test]
    fn matches_jdk_text_form() {
        let cases: &[(IpAddr, &str)] = &[
            (IpAddr::V4(Ipv4Addr::LOCALHOST), "127.0.0.1"),
            (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), "10.0.0.5"),
            (IpAddr::V6(Ipv6Addr::LOCALHOST), "0:0:0:0:0:0:0:1"),
            (IpAddr::V6(Ipv6Addr::UNSPECIFIED), "0:0:0:0:0:0:0:0"),
            (
                IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 5)),
                "2001:db8:0:0:0:0:0:5",
            ),
            (
                IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0005)),
                "10.0.0.5",
            ),
        ];
        for (ip, expected) in cases {
            assert2::assert!(jdk_host_address(*ip) == *expected);
        }
    }
}
