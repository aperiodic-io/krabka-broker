//! What `throttle_probes!` generates, driven through stand-in response
//! schemas: each entry is keyed by its response module's `API_KEY`, a
//! `throttled` probe sets the sentinel, an `unthrottled` one cannot, and a
//! `legacy_split` probe switches schema at its canonical version.

use {assert2::assert, std::collections::BTreeMap};

type ApiVersion = i16;
type Probe = fn(ApiVersion) -> ThrottlePosition;

const SENTINEL: i32 = 0x5EED_0219;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThrottlePosition {
    Leading,
    Buried,
    Absent,
}

/// The stand-in encoder: a body as its int32 fields, in wire order.
trait Response {
    fn encode(&self, version: ApiVersion) -> Vec<i32>;
}

/// Stands in for the generated `default_json`, which tells a buried field
/// from an absent one.
pub struct Json {
    throttle_time_ms: bool,
}

fn position<R: Response>(response: &R, version: ApiVersion, json: &Json) -> ThrottlePosition {
    if response.encode(version).first() == Some(&SENTINEL) {
        ThrottlePosition::Leading
    } else if json.throttle_time_ms {
        ThrottlePosition::Buried
    } else {
        ThrottlePosition::Absent
    }
}

/// A response schema with an `error_code` and either no `throttle_time_ms`, or
/// one that the schema gains at `$from` and puts first when `$leads`.
macro_rules! response {
    ($module:ident, $ty:ident, $(api_key: $api_key:literal,)? throttle: $from:literal, $leads:literal) => {
        pub mod $module {
            $(pub const API_KEY: i16 = $api_key;)?

            #[derive(Default)]
            pub struct $ty {
                pub error_code: i16,
                pub throttle_time_ms: i32,
            }

            impl crate::Response for $ty {
                fn encode(&self, version: i16) -> Vec<i32> {
                    let error_code = i32::from(self.error_code);
                    match (version >= $from, $leads) {
                        (false, _) => vec![error_code],
                        (true, true) => vec![self.throttle_time_ms, error_code],
                        (true, false) => vec![error_code, self.throttle_time_ms],
                    }
                }
            }

            pub fn default_json(version: i16) -> crate::Json {
                crate::Json {
                    throttle_time_ms: version >= $from,
                }
            }
        }
    };
    ($module:ident, $ty:ident, api_key: $api_key:literal, no_throttle) => {
        pub mod $module {
            pub const API_KEY: i16 = $api_key;

            #[derive(Default)]
            pub struct $ty {
                pub error_code: i16,
            }

            impl crate::Response for $ty {
                fn encode(&self, _: i16) -> Vec<i32> {
                    vec![i32::from(self.error_code)]
                }
            }

            pub fn default_json(_: i16) -> crate::Json {
                crate::Json {
                    throttle_time_ms: false,
                }
            }
        }
    };
}

mod krabka_protocol {
    pub mod owned {
        // Leads with `ThrottleTimeMs` from v3, and has none before.
        response!(metadata_response, MetadataResponse, api_key: 3, throttle: 3, true);
        // Has no `ThrottleTimeMs` at any version.
        response!(sasl_handshake_response, SaslHandshakeResponse, api_key: 17, no_throttle);
        // Leads with `ThrottleTimeMs` at every version.
        response!(produce_response, ProduceResponse, api_key: 0, throttle: 0, true);
    }

    pub mod kafka_3_6_2 {
        pub mod owned {
            // Carries `ThrottleTimeMs` behind another field.
            response!(produce_response, ProduceResponse, throttle: 0, false);
        }
    }
}

/// Where one API's response puts the throttle at one version.
#[derive(Debug, PartialEq, Eq)]
struct Observation {
    api_key: i16,
    version: ApiVersion,
    position: ThrottlePosition,
}

fn probes() -> BTreeMap<i16, Probe> {
    krabka_macros::throttle_probes! {
        throttled: Metadata;
        unthrottled: SaslHandshake;
        legacy_split: Produce = 3;
    }
    .into_iter()
    .collect()
}

#[test]
fn each_probe_reports_where_its_schema_puts_the_throttle() {
    let probes = probes();
    let observed: Vec<Observation> = probes
        .iter()
        .flat_map(|(&api_key, probe)| {
            [2, 3].map(|version| Observation {
                api_key,
                version,
                position: probe(version),
            })
        })
        .collect();

    let expected = [
        (0, 2, ThrottlePosition::Buried),
        (0, 3, ThrottlePosition::Leading),
        (3, 2, ThrottlePosition::Absent),
        (3, 3, ThrottlePosition::Leading),
        (17, 2, ThrottlePosition::Absent),
        (17, 3, ThrottlePosition::Absent),
    ]
    .map(|(api_key, version, position)| Observation {
        api_key,
        version,
        position,
    });
    assert!(observed == expected);
}
