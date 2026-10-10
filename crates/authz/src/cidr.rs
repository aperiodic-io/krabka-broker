//! CIDR-based ACL host patterns (KIP-1276).
//!
//! Kafka 4.4 lets an ACL host be a CIDR range such as `10.0.0.0/8` or
//! `2001:db8::/32`, not only a single address or the `*` wildcard. This module
//! is the parser [`CreateAcls`] validation uses to reject a malformed CIDR up
//! front, and the range test the authorizer uses to match a peer address
//! against a stored CIDR ACL. Two call sites, one implementation, so they
//! cannot drift.
//!
//! No CIDR crate is a dependency of this workspace, so the range test is
//! hand-rolled from [`std::net`] address bytes rather than pulling one in for
//! a single prefix-mask comparison.
//!
//! [`CreateAcls`]: https://kafka.apache.org/protocol.html#The_Messages_CreateAcls

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// A parsed `<ip>/<prefix-len>` CIDR host pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cidr {
    ip: IpAddr,
    prefix_len: u8,
}

impl Cidr {
    /// Parses `host` as a CIDR pattern the way Kafka's `CidrUtils.validate`
    /// does: a pattern containing `:` is IPv6 and goes to commons-net
    /// `SubnetUtils6`, anything else is IPv4 and goes to `SubnetUtils`
    /// (commons-net 3.13.0, the version Kafka trunk pins).
    ///
    /// # Errors
    ///
    /// Returns the commons-net `IllegalArgumentException` message; the caller
    /// prefixes it with `Invalid CIDR notation '<host>': ` to match Kafka's
    /// `CreateAcls` message. An IPv4-mapped IPv6 range (`::ffff:10.0.0.0/104`)
    /// is refused, as Java resolves that literal to an `Inet4Address` and
    /// `SubnetUtils6` rejects it; the operator writes the plain IPv4 form.
    pub fn parse(host: &str) -> Result<Self, String> {
        if host.contains(':') {
            parse_v6(host)
        } else {
            parse_v4(host)
        }
    }

    /// True when `ip` falls within this range.
    ///
    /// `ip` is canonicalized first ([`IpAddr::to_canonical`]), so an
    /// IPv4-mapped IPv6 peer -- what a dual-stack listener hands the request
    /// path -- unwraps to its IPv4 form before comparison and matches a plain
    /// IPv4 CIDR. The CIDR itself is not canonicalized: `CreateAcls` only
    /// ever stores the form an operator typed (`10.0.0.0/8`,
    /// `2001:db8::/32`), never an IPv4-mapped IPv6 literal, so a mismatched
    /// pair here is a genuine address-family mismatch and answers `false`,
    /// the same as a literal host comparison would.
    ///
    /// An IPv4 range never contains `0.0.0.0`, even `0.0.0.0/0`: commons-net
    /// `SubnetUtils.SubnetInfo.isInRange(int)` refuses the zero address before
    /// it compares. `SubnetUtils6` has no such rule, so `::/0` contains `::`.
    #[must_use]
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.ip, ip.to_canonical()) {
            (IpAddr::V4(_), IpAddr::V4(candidate)) if candidate.is_unspecified() => false,
            (IpAddr::V4(range), IpAddr::V4(candidate)) => {
                let mask = v4_mask(self.prefix_len);
                u32::from(range) & mask == u32::from(candidate) & mask
            }
            (IpAddr::V6(range), IpAddr::V6(candidate)) => {
                let mask = v6_mask(self.prefix_len);
                u128::from(range) & mask == u128::from(candidate) & mask
            }
            (IpAddr::V4(_), IpAddr::V6(_)) | (IpAddr::V6(_), IpAddr::V4(_)) => false,
        }
    }
}

/// `SubnetUtils(String)`: the whole pattern must match
/// `(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})/(\d{1,2})`, then each octet is
/// range-checked to `[0,255]` and the prefix to `[0,32]`, in that order.
fn parse_v4(cidr: &str) -> Result<Cidr, String> {
    let parse_fail = || format!("Could not parse [{cidr}]");
    let (address, prefix) = cidr.split_once('/').ok_or_else(parse_fail)?;
    let octets: Vec<&str> = address.split('.').collect();
    if octets.len() != 4
        || !octets.iter().all(|octet| ascii_digits(octet, 3))
        || !ascii_digits(prefix, 2)
    {
        return Err(parse_fail());
    }
    let mut bytes = [0_u8; 4];
    for (byte, octet) in bytes.iter_mut().zip(&octets) {
        let value: u32 = octet.parse().map_err(|_| parse_fail())?;
        *byte = u8::try_from(value).map_err(|_| format!("Value [{value}] not in range [0,255]"))?;
    }
    let prefix_len: u8 = prefix.parse().map_err(|_| parse_fail())?;
    if prefix_len > 32 {
        return Err(format!("Value [{prefix_len}] not in range [0,32]"));
    }
    Ok(Cidr {
        ip: IpAddr::V4(Ipv4Addr::from(bytes)),
        prefix_len,
    })
}

/// `SubnetUtils6(String)`: split at the first `/`, parse the prefix as a Java
/// `int` and range-check it to `[0,128]`, then resolve the address with
/// `InetAddress.getByName`, which must give an `Inet6Address`.
fn parse_v6(cidr: &str) -> Result<Cidr, String> {
    let Some((address, prefix)) = cidr.split_once('/') else {
        return Err(format!("Could not parse [{cidr}] - missing prefix length"));
    };
    let prefix_len = java_parse_int(prefix)
        .ok_or_else(|| format!("Could not parse [{cidr}] - invalid prefix length"))?;
    let prefix_len = u8::try_from(prefix_len)
        .ok()
        .filter(|len| *len <= 128)
        .ok_or_else(|| {
            format!("Could not parse [{cidr}] - prefix length must be between 0 and 128")
        })?;
    // `getByName` strips one pair of brackets around an IPv6 literal.
    let literal = address
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(address);
    let ip: Ipv6Addr = literal
        .parse()
        .map_err(|_| format!("Could not parse [{address}]"))?;
    if ip.to_ipv4_mapped().is_some() {
        return Err(format!("Could not parse [{address}] - not an IPv6 address"));
    }
    Ok(Cidr {
        ip: IpAddr::V6(ip),
        prefix_len,
    })
}

/// True when `text` is one to `max` ASCII digits, as the regex `\d{1,max}`.
fn ascii_digits(text: &str, max: usize) -> bool {
    (1..=max).contains(&text.len()) && text.bytes().all(|b| b.is_ascii_digit())
}

/// `Integer.parseInt` over ASCII: an optional sign, then one or more digits,
/// within `i32`.
fn java_parse_int(text: &str) -> Option<i32> {
    // Rust's signed decimal parser already requires an optional ASCII sign
    // followed by a nonempty ASCII digit run and checks the i32 bounds.
    text.parse().ok()
}

/// A `/prefix_len` mask for a 32-bit address, high bits set. `prefix_len` is
/// already bounds-checked to `0..=32` by [`Cidr::parse`], so the only edge
/// case here is `prefix_len == 0`, where a plain `u32::MAX << 32` would be a
/// shift-by-width (undefined behavior in C, a panic in debug Rust).
fn v4_mask(prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix_len))
    }
}

/// The 128-bit analog of [`v4_mask`].
fn v6_mask(prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        0
    } else {
        u128::MAX << (128 - u32::from(prefix_len))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use assert2::check;

    use super::*;

    #[test]
    fn parse_accepts_valid_cidrs() {
        let cases: &[(&str, IpAddr, u8)] = &[
            ("10.0.0.0/8", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8),
            (
                "192.168.0.0/24",
                IpAddr::V4(Ipv4Addr::new(192, 168, 0, 0)),
                24,
            ),
            ("10.0.0.5/32", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), 32),
            (
                "2001:db8::/32",
                IpAddr::V6("2001:db8::".parse().unwrap()),
                32,
            ),
            // `\d{1,3}` octets and a `\d{1,2}` prefix take leading zeros,
            // read as decimal.
            ("010.0.0.1/08", IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8),
            // `InetAddress.getByName` strips brackets; `parseInt` takes a
            // sign.
            (
                "[2001:db8::]/+32",
                IpAddr::V6("2001:db8::".parse().unwrap()),
                32,
            ),
        ];
        for (input, ip, prefix_len) in cases {
            check!(
                Cidr::parse(input)
                    == Ok(Cidr {
                        ip: *ip,
                        prefix_len: *prefix_len
                    })
            );
        }
    }

    #[test]
    fn parse_rejects_malformed_cidrs() {
        // commons-net 3.13.0 `SubnetUtils` / `SubnetUtils6` messages.
        let cases: &[(&str, &str)] = &[
            ("not-an-ip/8", "Could not parse [not-an-ip/8]"),
            ("10.0.0.0/thirty", "Could not parse [10.0.0.0/thirty]"),
            ("10.0.0.0/33", "Value [33] not in range [0,32]"),
            ("10.0.0.0/100", "Could not parse [10.0.0.0/100]"),
            ("10.0.256.0/8", "Value [256] not in range [0,255]"),
            ("10.0.0/8", "Could not parse [10.0.0/8]"),
            ("10.0.0.0/8/8", "Could not parse [10.0.0.0/8/8]"),
            (
                "2001:db8::/129",
                "Could not parse [2001:db8::/129] - prefix length must be between 0 and 128",
            ),
            (
                "2001:db8::/-1",
                "Could not parse [2001:db8::/-1] - prefix length must be between 0 and 128",
            ),
            (
                "2001:db8::/x",
                "Could not parse [2001:db8::/x] - invalid prefix length",
            ),
            (
                "2001:db8::/32/5",
                "Could not parse [2001:db8::/32/5] - invalid prefix length",
            ),
            (
                "2001:db8::",
                "Could not parse [2001:db8::] - missing prefix length",
            ),
            ("2001:zz8::/32", "Could not parse [2001:zz8::]"),
            (
                "::ffff:10.0.0.0/104",
                "Could not parse [::ffff:10.0.0.0] - not an IPv6 address",
            ),
        ];
        for (input, reason) in cases {
            check!(
                Cidr::parse(input) == Err((*reason).to_owned()),
                "input {input}"
            );
        }
    }

    fn check_membership(cidr: &Cidr, cases: &[(IpAddr, bool)]) {
        for (ip, expected) in cases {
            check!(cidr.contains(*ip) == *expected, "ip {ip}");
        }
    }

    #[test]
    fn contains_matches_ipv4_ranges() {
        let cidr = Cidr::parse("10.0.0.0/8").unwrap();
        let cases: &[(IpAddr, bool)] = &[
            (IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), true),
            (IpAddr::V4(Ipv4Addr::new(10, 255, 255, 255)), true),
            (IpAddr::V4(Ipv4Addr::new(11, 0, 0, 1)), false),
        ];
        check_membership(&cidr, cases);
    }

    #[test]
    fn contains_matches_ipv6_ranges() {
        let cidr = Cidr::parse("2001:db8::/32").unwrap();
        let cases: &[(IpAddr, bool)] = &[
            (IpAddr::V6("2001:db8::5".parse().unwrap()), true),
            (
                IpAddr::V6("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()),
                true,
            ),
            (IpAddr::V6("2001:db9::5".parse().unwrap()), false),
        ];
        check_membership(&cidr, cases);
    }

    #[test]
    fn contains_matches_ipv4_mapped_ipv6_peer_against_ipv4_cidr() {
        let v4_cidr = Cidr::parse("10.0.0.0/8").unwrap();
        let v4_mapped_peer: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
        check!(v4_cidr.contains(v4_mapped_peer));

        let v4_mapped_miss: IpAddr = "::ffff:11.0.0.1".parse().unwrap();
        check!(!v4_cidr.contains(v4_mapped_miss));
    }

    #[test]
    fn zero_prefix_len_matches_everything_in_family() {
        let v4_any = Cidr::parse("0.0.0.0/0").unwrap();
        check!(v4_any.contains(IpAddr::V4(Ipv4Addr::BROADCAST)));
        check!(!v4_any.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));

        let v6_any = Cidr::parse("::/0").unwrap();
        check!(v6_any.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    /// commons-net `SubnetUtils.SubnetInfo.isInRange(int)` answers `false`
    /// for the zero address in every IPv4 range, including one that holds it
    /// numerically. `SubnetUtils6.SubnetInfo.isInRange` has no such rule.
    #[test]
    fn unspecified_address_matches_only_ipv6_ranges() {
        let cases: &[(&str, IpAddr, bool)] = &[
            ("0.0.0.0/0", IpAddr::V4(Ipv4Addr::UNSPECIFIED), false),
            ("0.0.0.0/8", IpAddr::V4(Ipv4Addr::UNSPECIFIED), false),
            ("0.0.0.0/32", IpAddr::V4(Ipv4Addr::UNSPECIFIED), false),
            ("0.0.0.0/8", IpAddr::V4(Ipv4Addr::new(0, 0, 0, 1)), true),
            (
                "0.0.0.0/0",
                IpAddr::V6(Ipv4Addr::UNSPECIFIED.to_ipv6_mapped()),
                false,
            ),
            ("::/0", IpAddr::V6(Ipv6Addr::UNSPECIFIED), true),
            ("::/128", IpAddr::V6(Ipv6Addr::UNSPECIFIED), true),
        ];
        for (cidr, ip, expected) in cases {
            check!(
                Cidr::parse(cidr).unwrap().contains(*ip) == *expected,
                "{cidr} contains {ip}"
            );
        }
    }
}
