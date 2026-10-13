/// The result of selecting the dedup identity of one record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdentityLookup<'a> {
    /// The record has an identity. An empty slice is an identity.
    Identity(&'a [u8]),
    /// Key mode, and the record has a null Kafka key. Dedup does not apply to the record: the broker admits it and does not index it.
    NoIdentity,
    /// Header mode, and the record has no usable occurrence of the configured header. The broker rejects the whole request and does not fall back to the Kafka key, because that fallback would silently defeat the choice of a header as the identity.
    MissingHeader,
}

/// Selects the dedup identity of one record.
///
/// With `key_header` unset (key mode), the identity is the record's Kafka key, and a null key gives [`IdentityLookup::NoIdentity`]. With `key_header` set (header mode), the identity is the value of the first header whose name equals `key_header`. The first match makes repeated header names deterministic. The Kafka key is never read in header mode. A header with a null value gives [`IdentityLookup::MissingHeader`], the same as an absent header.
///
/// `find_header` answers the header lookup the way [`first_header`] does: the value of the first header with the name it is given, `Some(None)` for a null value, and `None` for no such header. A caller whose record format can look a header up without decoding every name passes that lookup instead.
pub fn identity_of<'a>(
    key: Option<&'a [u8]>,
    find_header: impl FnOnce(&str) -> Option<Option<&'a [u8]>>,
    key_header: Option<&str>,
) -> IdentityLookup<'a> {
    let Some(key_header) = key_header else {
        return key.map_or(IdentityLookup::NoIdentity, IdentityLookup::Identity);
    };
    // A null value is not an empty one. Digesting it as empty would give every
    // record with a null-valued header the same identity and drop unrelated
    // records against each other, so the record has no identity instead.
    find_header(key_header)
        .flatten()
        .map_or(IdentityLookup::MissingHeader, IdentityLookup::Identity)
}

/// The value of the first of `headers` named `name`: `Some(None)` for a null value, and `None` when no header has the name.
pub fn first_header<'a>(
    headers: impl IntoIterator<Item = (&'a str, Option<&'a [u8]>)>,
    name: &str,
) -> Option<Option<&'a [u8]>> {
    headers
        .into_iter()
        .find(|(header, _)| *header == name)
        .map(|(_, value)| value)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    const HEADER: &str = "dedup-key";

    struct Case {
        name: &'static str,
        key: Option<&'static [u8]>,
        headers: Vec<(&'static str, Option<&'static [u8]>)>,
        key_header: Option<&'static str>,
        expected: IdentityLookup<'static>,
    }

    fn cases() -> Vec<Case> {
        vec![
            Case {
                name: "key mode: a present key is the identity",
                key: Some(b"k"),
                headers: vec![(HEADER, Some(b"ignored"))],
                key_header: None,
                expected: IdentityLookup::Identity(b"k"),
            },
            Case {
                name: "key mode: an empty key is an identity",
                key: Some(b""),
                headers: vec![],
                key_header: None,
                expected: IdentityLookup::Identity(b""),
            },
            Case {
                name: "key mode: a null key has no identity",
                key: None,
                headers: vec![(HEADER, Some(b"ignored"))],
                key_header: None,
                expected: IdentityLookup::NoIdentity,
            },
            Case {
                name: "header mode: the header value is the identity",
                key: Some(b"partition-key"),
                headers: vec![("other", Some(b"x")), (HEADER, Some(b"id-1"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::Identity(b"id-1"),
            },
            Case {
                name: "header mode: the first of repeated headers wins",
                key: Some(b"k"),
                headers: vec![(HEADER, Some(b"first")), (HEADER, Some(b"second"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::Identity(b"first"),
            },
            Case {
                name: "header mode: a null first occurrence is missing, not skipped",
                key: Some(b"k"),
                headers: vec![(HEADER, None), (HEADER, Some(b"second"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::MissingHeader,
            },
            Case {
                name: "header mode: an absent header is missing",
                key: Some(b"k"),
                headers: vec![("other", Some(b"x"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::MissingHeader,
            },
            Case {
                name: "header mode: no headers is missing",
                key: Some(b"k"),
                headers: vec![],
                key_header: Some(HEADER),
                expected: IdentityLookup::MissingHeader,
            },
            Case {
                name: "header mode: a null value is missing",
                key: Some(b"k"),
                headers: vec![(HEADER, None)],
                key_header: Some(HEADER),
                expected: IdentityLookup::MissingHeader,
            },
            Case {
                name: "header mode: an empty value is an identity",
                key: Some(b"k"),
                headers: vec![(HEADER, Some(b""))],
                key_header: Some(HEADER),
                expected: IdentityLookup::Identity(b""),
            },
            Case {
                name: "header mode: a null key does not matter",
                key: None,
                headers: vec![(HEADER, Some(b"id-1"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::Identity(b"id-1"),
            },
            Case {
                name: "header mode: header names match exactly",
                key: Some(b"k"),
                headers: vec![("Dedup-Key", Some(b"x")), ("dedup-key-2", Some(b"y"))],
                key_header: Some(HEADER),
                expected: IdentityLookup::MissingHeader,
            },
        ]
    }

    #[test]
    fn identity_selection() {
        for case in cases() {
            let actual = identity_of(
                case.key,
                |name| first_header(case.headers.iter().copied(), name),
                case.key_header,
            );
            assert!(actual == case.expected, "{}", case.name);
        }
    }
}
