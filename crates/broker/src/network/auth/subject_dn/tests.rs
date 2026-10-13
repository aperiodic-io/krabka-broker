//! RFC 2253 rendering checked against the strings Java's
//! `X500Principal.getName()` gives for the same DER.

use {
    super::{name_rfc2253, subject_dn_rfc2253},
    assert2::check,
    rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair},
};

const CN: &[u8] = &[0x55, 0x04, 0x03];
const C: &[u8] = &[0x55, 0x04, 0x06];
const O: &[u8] = &[0x55, 0x04, 0x0A];
const SERIAL_NUMBER: &[u8] = &[0x55, 0x04, 0x05];
const UID: &[u8] = &[0x09, 0x92, 0x26, 0x89, 0x93, 0xF2, 0x2C, 0x64, 0x01, 0x01];
const EMAIL_ADDRESS: &[u8] = &[0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x09, 0x01];

const UTF8: u8 = 0x0C;
const PRINTABLE: u8 = 0x13;
const IA5: u8 = 0x16;
const INTEGER: u8 = 0x02;

fn tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let length = u8::try_from(content.len()).expect("short test value");
    assert2::assert!(length < 0x80);
    let mut out = vec![tag, length];
    out.extend_from_slice(content);
    out
}

/// One attribute: `(oid content, value tag, value content)`.
type Attribute<'a> = (&'a [u8], u8, &'a [u8]);

/// One RDN: its attributes in encoding order.
type Rdn<'a> = &'a [Attribute<'a>];

/// The contents of a DER `Name` whose RDNs are `rdns`, in encoding order.
fn rdn_sequence(rdns: &[Rdn<'_>]) -> Vec<u8> {
    rdns.iter()
        .flat_map(|rdn| {
            let attributes: Vec<u8> = rdn
                .iter()
                .flat_map(|(oid, tag, value)| {
                    let mut attribute = tlv(0x06, oid);
                    attribute.extend(tlv(*tag, value));
                    tlv(0x30, &attribute)
                })
                .collect();
            tlv(0x31, &attributes)
        })
        .collect()
}

#[test]
fn renders_names_as_x500_principal_get_name() {
    // (case, RDNs in encoding order, expected RFC 2253 string)
    let cases: [(&str, &[Rdn<'_>], &str); 10] = [
        ("single RDN", &[&[(CN, UTF8, b"alice")]], "CN=alice"),
        (
            "RDNs come out last first",
            &[&[(C, PRINTABLE, b"US")], &[(O, UTF8, b"krabka")], &[(
                CN, UTF8, b"alice",
            )]],
            "CN=alice,O=krabka,C=US",
        ),
        (
            "special characters are escaped",
            &[&[(CN, UTF8, b"a,b+c=d<e>f;g\"h\\i#j")]],
            "CN=a\\,b\\+c\\=d\\<e\\>f\\;g\\\"h\\\\i\\#j",
        ),
        (
            "leading and trailing spaces are escaped",
            &[&[(CN, UTF8, b"  a b ")]],
            "CN=\\ \\ a b\\ ",
        ),
        (
            "multi-valued RDN joins with plus in encoding order",
            &[&[(O, UTF8, b"krabka")], &[
                (CN, UTF8, b"alice"),
                (UID, UTF8, b"42"),
            ]],
            "CN=alice+UID=42,O=krabka",
        ),
        (
            "emailAddress has no RFC 2253 keyword",
            &[&[(EMAIL_ADDRESS, IA5, b"a@b")], &[(CN, UTF8, b"alice")]],
            "CN=alice,1.2.840.113549.1.9.1=#1603614062",
        ),
        (
            "serialNumber has no RFC 2253 keyword",
            &[&[(SERIAL_NUMBER, PRINTABLE, b"123")]],
            "2.5.4.5=#1303313233",
        ),
        (
            "a keyword with a non-string value is hex",
            &[&[(CN, INTEGER, &[0x05])]],
            "CN=#020105",
        ),
        (
            "NUL is escaped as \\00",
            &[&[(CN, UTF8, b"a\0b")]],
            "CN=a\\00b",
        ),
        ("empty name", &[], ""),
    ];
    for (case, rdns, expected) in cases {
        check!(
            name_rfc2253(&rdn_sequence(rdns)).as_deref() == Some(expected),
            "{case}"
        );
    }
}

#[test]
fn renders_the_subject_of_a_certificate() {
    // (subject attributes in encoding order, expected RFC 2253 string)
    let cases: [(&[(DnType, &str)], &str); 3] = [
        (
            &[
                (DnType::CountryName, "US"),
                (DnType::OrganizationName, "krabka"),
                (DnType::CommonName, "alice"),
            ],
            "CN=alice,O=krabka,C=US",
        ),
        (
            &[
                (DnType::OrganizationName, "krabka"),
                (DnType::OrganizationalUnitName, "integration"),
                (DnType::CommonName, "test-client"),
            ],
            "CN=test-client,OU=integration,O=krabka",
        ),
        // One CN whose value holds commas is not a three-RDN DN.
        (
            &[(DnType::CommonName, "test-client,OU=integration,O=krabka")],
            "CN=test-client\\,OU\\=integration\\,O\\=krabka",
        ),
    ];
    let key = KeyPair::generate().expect("key pair");
    for (attributes, expected) in cases {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        let mut subject = DistinguishedName::new();
        for (dn_type, value) in attributes {
            subject.push(dn_type.clone(), *value);
        }
        params.distinguished_name = subject;
        let certificate = params.self_signed(&key).expect("self-signed certificate");
        check!(
            subject_dn_rfc2253(certificate.der()).as_deref() == Some(expected),
            "{expected}"
        );
    }
}

#[test]
fn refuses_bytes_that_are_not_a_certificate() {
    for input in [&b""[..], b"not-a-cert", &[0x30, 0x05, 0x02]] {
        check!(subject_dn_rfc2253(input) == None, "{input:?}");
    }
}
