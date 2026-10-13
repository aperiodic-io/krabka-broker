//! The Subject DN of an mTLS peer certificate in the form Kafka's
//! `DefaultKafkaPrincipalBuilder` feeds to `ssl.principal.mapping.rules`:
//! `X500Principal.getName()`, which is RFC 2253.
//!
//! RFC 2253 writes the RDNs last to first, separated by `,` with no space, and
//! the attributes of a multi-valued RDN in encoding order joined by `+`. An
//! attribute whose type is one of RFC 2253's keywords and whose value is a
//! directory string is written `KEYWORD=value` with `,=+<>#;"\` escaped and
//! leading or trailing spaces escaped. Any other type is written as its dotted
//! OID, and its value, like any value that is not a directory string, as `#`
//! followed by the hex of the value's DER encoding, which is how
//! `emailAddress` comes out.
//!
//! The certificate reaches this module after rustls has verified it, so a
//! small DER reader over the few structures a Subject needs is enough.

/// Returns the Subject DN of the DER certificate `cert_der` in the RFC 2253
/// form `X500Principal.getName()` gives, or `None` when the bytes are not a
/// certificate.
#[must_use]
pub fn subject_dn_rfc2253(cert_der: &[u8]) -> Option<String> {
    let certificate = Tlv::read_exact(cert_der, SEQUENCE)?;
    let mut tbs = Tlv::read(certificate.value, SEQUENCE)?.tlv.value;
    // TBSCertificate: [0] version (optional), serialNumber, signature, issuer,
    // validity, subject.
    if tbs.first() == Some(&EXPLICIT_VERSION) {
        tbs = Tlv::read(tbs, EXPLICIT_VERSION)?.rest;
    }
    for tag in [INTEGER, SEQUENCE, SEQUENCE, SEQUENCE] {
        tbs = Tlv::read(tbs, tag)?.rest;
    }
    let subject = Tlv::read(tbs, SEQUENCE)?.tlv;
    name_rfc2253(subject.value)
}

/// Renders the contents of a DER `Name` (the RDN sequence) in RFC 2253 form.
fn name_rfc2253(mut rdn_sequence: &[u8]) -> Option<String> {
    let mut rdns = Vec::new();
    while !rdn_sequence.is_empty() {
        let TlvAndRest { tlv: set, rest } = Tlv::read(rdn_sequence, SET)?;
        rdn_sequence = rest;
        let mut attributes = Vec::new();
        let mut members = set.value;
        while !members.is_empty() {
            let TlvAndRest {
                tlv: attribute,
                rest,
            } = Tlv::read(members, SEQUENCE)?;
            members = rest;
            attributes.push(attribute_rfc2253(attribute.value)?);
        }
        rdns.push(attributes.join("+"));
    }
    rdns.reverse();
    Some(rdns.join(","))
}

/// Renders one `AttributeTypeAndValue`.
fn attribute_rfc2253(type_and_value: &[u8]) -> Option<String> {
    let TlvAndRest { tlv: oid, rest } = Tlv::read(type_and_value, OBJECT_IDENTIFIER)?;
    let value = Tlv::read_exact(rest, None)?;
    let oid = dotted_oid(oid.value)?;
    let keyword = RFC2253_KEYWORDS
        .iter()
        .find_map(|entry| (entry.oid == oid).then_some(entry.keyword));
    // Java's `AVA.toRFC2253String` hex-encodes a value whose type is written
    // as an OID, or whose value is not a directory string.
    Some(match (keyword, directory_string(value.tag, value.value)) {
        (Some(keyword), Some(text)) => format!("{keyword}={}", escape_value(&text)),
        (keyword, _) => {
            let mut rendered = format!("{}=#", keyword.unwrap_or(oid.as_str()));
            for byte in value.whole {
                rendered.push(char::from_digit(u32::from(byte >> 4), 16)?);
                rendered.push(char::from_digit(u32::from(byte & 0x0F), 16)?);
            }
            rendered
        }
    })
}

/// The attribute types RFC 2253 writes by keyword, as Java's `AVAKeyword`
/// table marks them.
const RFC2253_KEYWORDS: [Rfc2253Keyword; 9] = [
    Rfc2253Keyword::new("2.5.4.3", "CN"),
    Rfc2253Keyword::new("2.5.4.6", "C"),
    Rfc2253Keyword::new("2.5.4.7", "L"),
    Rfc2253Keyword::new("2.5.4.8", "ST"),
    Rfc2253Keyword::new("2.5.4.9", "STREET"),
    Rfc2253Keyword::new("2.5.4.10", "O"),
    Rfc2253Keyword::new("2.5.4.11", "OU"),
    Rfc2253Keyword::new("0.9.2342.19200300.100.1.25", "DC"),
    Rfc2253Keyword::new("0.9.2342.19200300.100.1.1", "UID"),
];

/// An attribute type written by keyword: its dotted OID and the keyword.
struct Rfc2253Keyword {
    oid: &'static str,
    keyword: &'static str,
}

impl Rfc2253Keyword {
    const fn new(oid: &'static str, keyword: &'static str) -> Self { Self { oid, keyword } }
}

/// The text of a value Java's `AVA.isDerString` accepts, decoded with the
/// charset `AVA.getCharset` picks for its tag, or `None` for any other value
/// type, which RFC 2253 writes in hex.
fn directory_string(tag: u8, bytes: &[u8]) -> Option<String> {
    match tag {
        UTF8_STRING => Some(String::from_utf8_lossy(bytes).into_owned()),
        PRINTABLE_STRING | T61_STRING | IA5_STRING | GENERAL_STRING => {
            Some(bytes.iter().map(|&b| char::from(b)).collect())
        }
        BMP_STRING => {
            let units: Vec<u16> = bytes
                .chunks(2)
                .map(|pair| match pair {
                    [high, low] => u16::from_be_bytes([*high, *low]),
                    // A trailing odd octet decodes as U+FFFD, as Java's
                    // UTF-16BE decoder replaces it.
                    _ => 0xFFFD,
                })
                .collect();
            Some(String::from_utf16_lossy(&units))
        }
        _ => None,
    }
}

/// Java's `AVA.toRFC2253String` escaping of a string value.
fn escape_value(text: &str) -> String {
    const ESCAPEES: &str = ",=+<>#;\"\\";
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if ESCAPEES.contains(c) {
            escaped.push('\\');
            escaped.push(c);
        } else if c == '\0' {
            escaped.push_str("\\00");
        } else {
            escaped.push(c);
        }
    }
    let chars: Vec<char> = escaped.chars().collect();
    let is_padding = |c: &char| *c == ' ' || *c == '\r';
    let lead = chars.iter().take_while(|c| is_padding(c)).count();
    let trail = chars.len() - chars.iter().rev().take_while(|c| is_padding(c)).count();
    let mut out = String::with_capacity(escaped.len() + 2);
    for (i, c) in chars.into_iter().enumerate() {
        if i < lead || i >= trail {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// The dotted-decimal form of an OID's content octets.
fn dotted_oid(content: &[u8]) -> Option<String> {
    let mut arcs = Vec::new();
    let mut arc: u128 = 0;
    for (i, &byte) in content.iter().enumerate() {
        arc = arc.checked_mul(128)? | u128::from(byte & 0x7F);
        if byte & 0x80 != 0 {
            if i + 1 == content.len() {
                return None;
            }
            continue;
        }
        if arcs.is_empty() {
            let first = (arc / 40).min(2);
            arcs.push(first);
            arcs.push(arc - first * 40);
        } else {
            arcs.push(arc);
        }
        arc = 0;
    }
    (!arcs.is_empty()).then(|| {
        arcs.iter()
            .map(u128::to_string)
            .collect::<Vec<_>>()
            .join(".")
    })
}

const INTEGER: u8 = 0x02;
const OBJECT_IDENTIFIER: u8 = 0x06;
const UTF8_STRING: u8 = 0x0C;
const PRINTABLE_STRING: u8 = 0x13;
const T61_STRING: u8 = 0x14;
const IA5_STRING: u8 = 0x16;
const GENERAL_STRING: u8 = 0x1B;
const BMP_STRING: u8 = 0x1E;
const SEQUENCE: u8 = 0x30;
const SET: u8 = 0x31;
const EXPLICIT_VERSION: u8 = 0xA0;

/// One DER tag-length-value with a single-octet tag.
#[derive(Debug, Clone, Copy)]
struct Tlv<'a> {
    tag: u8,
    /// The content octets.
    value: &'a [u8],
    /// The whole encoding, tag and length included.
    whole: &'a [u8],
}

/// A TLV read from the start of some bytes, and the bytes after it.
struct TlvAndRest<'a> {
    tlv: Tlv<'a>,
    rest: &'a [u8],
}

impl<'a> Tlv<'a> {
    /// Reads the TLV at the start of `input`, which must carry `tag`, and
    /// returns it with the bytes after it.
    fn read(input: &'a [u8], tag: impl Into<Option<u8>>) -> Option<TlvAndRest<'a>> {
        let (&found, rest) = input.split_first()?;
        if tag.into().is_some_and(|tag| tag != found) || found & 0x1F == 0x1F {
            return None;
        }
        let (&first, mut rest) = rest.split_first()?;
        let length = if first & 0x80 == 0 {
            usize::from(first)
        } else {
            let octets = usize::from(first & 0x7F);
            if octets == 0 || octets > std::mem::size_of::<usize>() || rest.len() < octets {
                return None;
            }
            let (length, after) = rest.split_at(octets);
            rest = after;
            length
                .iter()
                .fold(0usize, |acc, &b| (acc << 8) | usize::from(b))
        };
        if rest.len() < length {
            return None;
        }
        let (value, after) = rest.split_at(length);
        let header = input.len() - rest.len();
        let tlv = Self {
            tag: found,
            value,
            whole: &input[..header + length],
        };
        Some(TlvAndRest { tlv, rest: after })
    }

    /// Reads a TLV that must fill all of `input`.
    fn read_exact(input: &'a [u8], tag: impl Into<Option<u8>>) -> Option<Self> {
        let TlvAndRest { tlv, rest } = Self::read(input, tag)?;
        rest.is_empty().then_some(tlv)
    }
}

#[cfg(test)]
mod tests;
