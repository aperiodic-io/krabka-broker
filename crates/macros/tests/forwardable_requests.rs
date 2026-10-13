//! Envelope dispatch selects one decoder while allowing bytes after its body.

use {assert2::assert, std::sync::Mutex};

type ApiKeyCode = i16;
type ApiVersion = i16;

#[derive(Debug, PartialEq)]
struct DecodeObservation {
    key: ApiKeyCode,
    version: ApiVersion,
    remaining: usize,
}

static OBSERVATIONS: Mutex<Vec<DecodeObservation>> = Mutex::new(Vec::new());

struct Decoder<const KEY: i16>;

impl<const KEY: i16> Decoder<KEY> {
    fn decode(cursor: &mut &[u8], version: ApiVersion) -> Result<Self, &'static str> {
        OBSERVATIONS.lock().unwrap().push(DecodeObservation {
            key: KEY,
            version,
            remaining: cursor.len(),
        });
        if version != 3 {
            return Err("unsupported version");
        }
        if cursor.first() != Some(&u8::try_from(KEY).expect("test key fits one byte")) {
            return Err("missing body marker");
        }
        *cursor = &cursor[1..];
        Ok(Self)
    }
}

mod requests {
    pub(super) type Create = super::Decoder<19>;
    pub(super) type Delete = super::Decoder<20>;
}

krabka_macros::forwardable_requests! {
    20 => requests::Delete,
    19 => requests::Create,
}

#[test]
fn key_order_decode_selection_versions_and_trailing_bytes_match_envelope_behavior() {
    assert!(FORWARDABLE_API_KEYS == [20, 19]);
    assert!(embedded_body_decodes(19, 3, &[19, 99, 100]));
    assert!(embedded_body_decodes(20, 3, &[20]));
    assert!(!embedded_body_decodes(19, 4, &[19]));
    assert!(!embedded_body_decodes(20, 3, &[19]));
    assert!(!embedded_body_decodes(19, 3, &[]));
    assert!(!embedded_body_decodes(58, 3, &[19]));
    assert!(
        *OBSERVATIONS.lock().unwrap()
            == [
                DecodeObservation {
                    key: 19,
                    version: 3,
                    remaining: 3
                },
                DecodeObservation {
                    key: 20,
                    version: 3,
                    remaining: 1
                },
                DecodeObservation {
                    key: 19,
                    version: 4,
                    remaining: 1
                },
                DecodeObservation {
                    key: 20,
                    version: 3,
                    remaining: 1
                },
                DecodeObservation {
                    key: 19,
                    version: 3,
                    remaining: 0
                },
            ]
    );
}
