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

/// The version at which a throttle field appears, and its position in the schema.
#[derive(Default)]
pub struct TimedSchema<const FROM: i16, const LEADING: bool> {
    pub error_code: i16,
    pub throttle_time_ms: i32,
}

impl<const FROM: i16, const LEADING: bool> Response for TimedSchema<FROM, LEADING> {
    fn encode(&self, version: ApiVersion) -> Vec<i32> {
        let error_code = i32::from(self.error_code);
        match (version >= FROM, LEADING) {
            (false, _) => vec![error_code],
            (true, true) => vec![self.throttle_time_ms, error_code],
            (true, false) => vec![error_code, self.throttle_time_ms],
        }
    }
}

impl<const FROM: i16, const LEADING: bool> TimedSchema<FROM, LEADING> {
    fn default_json(version: ApiVersion) -> Json {
        Json {
            throttle_time_ms: version >= FROM,
        }
    }
}

#[derive(Default)]
pub struct UnthrottledSchema {
    pub error_code: i16,
}

impl Response for UnthrottledSchema {
    fn encode(&self, _: ApiVersion) -> Vec<i32> { vec![i32::from(self.error_code)] }
}

mod krabka_protocol {
    pub mod owned {
        pub mod metadata_response {
            use crate::{Json, TimedSchema};
            pub const API_KEY: i16 = 3;
            // Leads with `ThrottleTimeMs` from v3, and has none before.
            pub type MetadataResponse = TimedSchema<3, true>;
            pub fn default_json(version: i16) -> Json { MetadataResponse::default_json(version) }
        }

        pub mod sasl_handshake_response {
            use crate::{Json, UnthrottledSchema};
            pub const API_KEY: i16 = 17;
            // Has no `ThrottleTimeMs` at any version.
            pub type SaslHandshakeResponse = UnthrottledSchema;
            pub fn default_json(_: i16) -> Json {
                Json {
                    throttle_time_ms: false,
                }
            }
        }

        pub mod produce_response {
            use crate::{Json, TimedSchema};
            pub const API_KEY: i16 = 0;
            // Leads with `ThrottleTimeMs` at every version.
            pub type ProduceResponse = TimedSchema<0, true>;
            pub fn default_json(version: i16) -> Json { ProduceResponse::default_json(version) }
        }
    }

    pub mod kafka_3_6_2 {
        pub mod owned {
            pub mod produce_response {
                use crate::{Json, TimedSchema};
                // Carries `ThrottleTimeMs` behind another field.
                pub type ProduceResponse = TimedSchema<0, false>;
                pub fn default_json(version: i16) -> Json { ProduceResponse::default_json(version) }
            }
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
