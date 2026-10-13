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

/// One attribute: the OID content, the value tag and the value content.
struct Attribute<'a> {
    oid: &'a [u8],
    tag: u8,
    value: &'a [u8],
}

/// An [`Attribute`] of type `oid` holding `value` under `tag`.
const fn attr<'a>(oid: &'a [u8], tag: u8, value: &'a [u8]) -> Attribute<'a> {
    Attribute { oid, tag, value }
}

/// One subject attribute of a generated certificate.
struct SubjectAttribute<'a> {
    dn_type: DnType,
    value: &'a str,
}

/// A [`SubjectAttribute`] of type `dn_type` holding `value`.
const fn dn_attr(dn_type: DnType, value: &str) -> SubjectAttribute<'_> {
    SubjectAttribute { dn_type, value }
}

/// One RDN: its attributes in encoding order.
type Rdn<'a> = &'a [Attribute<'a>];

/// The contents of a DER `Name` whose RDNs are `rdns`, in encoding order.
fn rdn_sequence(rdns: &[Rdn<'_>]) -> Vec<u8> {
    rdns.iter()
        .flat_map(|rdn| {
            let attributes: Vec<u8> = rdn
                .iter()
                .flat_map(|Attribute { oid, tag, value }| {
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
    /// A case name, its RDNs in encoding order and the expected RFC 2253
    /// string.
    struct NameCase<'a> {
        case: &'a str,
        rdns: &'a [Rdn<'a>],
        expected: &'a str,
    }
    let cases: [NameCase<'_>; 10] = [
        NameCase {
            case: "single RDN",
            rdns: &[&[attr(CN, UTF8, b"alice")]],
            expected: "CN=alice",
        },
        NameCase {
            case: "RDNs come out last first",
            rdns: &[
                &[attr(C, PRINTABLE, b"US")],
                &[attr(O, UTF8, b"krabka")],
                &[attr(CN, UTF8, b"alice")],
            ],
            expected: "CN=alice,O=krabka,C=US",
        },
        NameCase {
            case: "special characters are escaped",
            rdns: &[&[attr(CN, UTF8, b"a,b+c=d<e>f;g\"h\\i#j")]],
            expected: "CN=a\\,b\\+c\\=d\\<e\\>f\\;g\\\"h\\\\i\\#j",
        },
        NameCase {
            case: "leading and trailing spaces are escaped",
            rdns: &[&[attr(CN, UTF8, b"  a b ")]],
            expected: "CN=\\ \\ a b\\ ",
        },
        NameCase {
            case: "multi-valued RDN joins with plus in encoding order",
            rdns: &[&[attr(O, UTF8, b"krabka")], &[
                attr(CN, UTF8, b"alice"),
                attr(UID, UTF8, b"42"),
            ]],
            expected: "CN=alice+UID=42,O=krabka",
        },
        NameCase {
            case: "emailAddress has no RFC 2253 keyword",
            rdns: &[&[attr(EMAIL_ADDRESS, IA5, b"a@b")], &[attr(
                CN, UTF8, b"alice",
            )]],
            expected: "CN=alice,1.2.840.113549.1.9.1=#1603614062",
        },
        NameCase {
            case: "serialNumber has no RFC 2253 keyword",
            rdns: &[&[attr(SERIAL_NUMBER, PRINTABLE, b"123")]],
            expected: "2.5.4.5=#1303313233",
        },
        NameCase {
            case: "a keyword with a non-string value is hex",
            rdns: &[&[attr(CN, INTEGER, &[0x05])]],
            expected: "CN=#020105",
        },
        NameCase {
            case: "NUL is escaped as \\00",
            rdns: &[&[attr(CN, UTF8, b"a\0b")]],
            expected: "CN=a\\00b",
        },
        NameCase {
            case: "empty name",
            rdns: &[],
            expected: "",
        },
    ];
    for NameCase {
        case,
        rdns,
        expected,
    } in cases
    {
        check!(
            name_rfc2253(&rdn_sequence(rdns)).as_deref() == Some(expected),
            "{case}"
        );
    }
}

#[test]
fn renders_the_subject_of_a_certificate() {
    /// Subject attributes in encoding order and the expected RFC 2253 string.
    struct SubjectCase<'a> {
        attributes: &'a [SubjectAttribute<'a>],
        expected: &'a str,
    }
    let cases: [SubjectCase<'_>; 3] = [
        SubjectCase {
            attributes: &[
                dn_attr(DnType::CountryName, "US"),
                dn_attr(DnType::OrganizationName, "krabka"),
                dn_attr(DnType::CommonName, "alice"),
            ],
            expected: "CN=alice,O=krabka,C=US",
        },
        SubjectCase {
            attributes: &[
                dn_attr(DnType::OrganizationName, "krabka"),
                dn_attr(DnType::OrganizationalUnitName, "integration"),
                dn_attr(DnType::CommonName, "test-client"),
            ],
            expected: "CN=test-client,OU=integration,O=krabka",
        },
        // One CN whose value holds commas is not a three-RDN DN.
        SubjectCase {
            attributes: &[dn_attr(
                DnType::CommonName,
                "test-client,OU=integration,O=krabka",
            )],
            expected: "CN=test-client\\,OU\\=integration\\,O\\=krabka",
        },
    ];
    let key = KeyPair::generate().expect("key pair");
    for SubjectCase {
        attributes,
        expected,
    } in cases
    {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        let mut subject = DistinguishedName::new();
        for SubjectAttribute { dn_type, value } in attributes {
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
