//! `throttle_probes!`: see the crate documentation.

use moxy::{ast::ParseError, token::TokenStream};

use crate::api_names::{Section, parse_sections, section};

/// Expands `throttle_probes! { ... }` into an array of `(API_KEY, Probe)`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let sections: Vec<Section> =
        parse_sections(tokens, "=", &["throttled", "unthrottled", "legacy_split"])?;

    let mut probes = Vec::new();
    for (label, sets_sentinel) in [("throttled", true), ("unthrottled", false)] {
        for entry in section(&sections, label) {
            if let Some(value) = &entry.value {
                return Err(ParseError::new(
                    value.span(),
                    format!("`{label}` entries take no `= version`"),
                ));
            }
            let module = &entry.names.response_module;
            let response = &entry.names.response_type;
            probes.push(probe_entry(
                module,
                &moxy::template! {
                                @if sets_sentinel {
                                    let response = schema::{{ response }} {
                                        throttle_time_ms: SENTINEL,
                                        ..Default::default()
                                    };
                                } @else {
                                    let response = schema::{{ response }}::default();
                                }
                                position(&response, version, &schema::default_json(version))
                },
            ));
        }
    }
    for entry in section(&sections, "legacy_split") {
        let Some(canonical_from) = &entry.value else {
            return Err(ParseError::new(
                entry.names.api.span(),
                "`legacy_split` entries need `= <first canonical version>`",
            ));
        };
        let module = &entry.names.response_module;
        let response = &entry.names.response_type;
        probes.push(probe_entry(
            module,
            &moxy::template! {
                        use krabka_protocol::kafka_3_6_2::owned::{{ module }} as legacy;
                        if version < {{ canonical_from }} {
                            let response = legacy::{{ response }} {
                                throttle_time_ms: SENTINEL,
                                ..Default::default()
                            };
                            position(&response, version, &legacy::default_json(version))
                        } else {
                            let response = schema::{{ response }} {
                                throttle_time_ms: SENTINEL,
                                ..Default::default()
                            };
                            position(&response, version, &schema::default_json(version))
                        }
            },
        ));
    }

    Ok(moxy::template! {
        [
            @for probe in &probes {
                {{ probe }},
            }
        ]
    })
}

/// Both canonical and legacy probes share the registry entry and schema alias.
fn probe_entry(module: &moxy::token::Ident, body: &TokenStream) -> TokenStream {
    moxy::template! {
        (krabka_protocol::owned::{{ module }}::API_KEY, {
            fn probe(version: ApiVersion) -> ThrottlePosition {
                use krabka_protocol::owned::{{ module }} as schema;
                {{ body }}
            }
            probe as Probe
        })
    }
}
