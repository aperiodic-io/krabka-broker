//! Kafka api names: the section-table grammar that `dispatch_table!` and
//! `throttle_probes!` read, and the identifiers each name stands for.
//!
//! A table is a list of sections, each a label, a colon, and comma-separated
//! entries closed by a semicolon:
//!
//! ```text
//! label: Name, Name <separator> tokens, ...;
//! ```
//!
//! An entry is a `CamelCase` api name, optionally followed by the separator
//! the macro asks for and the tokens up to the next top-level `,` or `;`.

use {
    moxy::{
        ast::ParseError,
        token::{Ident, Span, TokenStream, TokenTree},
    },
    std::collections::BTreeSet,
};

/// The identifiers derived from one `CamelCase` Kafka api name, the way
/// `krabka-protocol` and the broker's handler modules spell them.
#[derive(Debug, Clone)]
pub(crate) struct ApiNames {
    /// The name as written, which is also the `ApiKey` variant: `CreateAcls`.
    pub(crate) api: Ident,
    /// `create_acls`, the handler module.
    pub(crate) snake: Ident,
    /// `create_acls_request`.
    pub(crate) request_module: Ident,
    /// `create_acls_response`.
    pub(crate) response_module: Ident,
    /// `CreateAclsRequest`.
    pub(crate) request_type: Ident,
    /// `CreateAclsResponse`.
    pub(crate) response_type: Ident,
}

impl ApiNames {
    pub(crate) fn new(api: &Ident) -> Self {
        let snake = api.to_snake_case();
        Self {
            api: api.clone(),
            request_module: snake.suffixed("_request"),
            response_module: snake.suffixed("_response"),
            request_type: api.suffixed("Request"),
            response_type: api.suffixed("Response"),
            snake,
        }
    }
}

/// Appends text to an identifier, keeping its span so that an error in the
/// generated code points at the table entry it came from.
pub(crate) trait Suffixed {
    fn suffixed(&self, suffix: &str) -> Ident;
}

impl Suffixed for Ident {
    fn suffixed(&self, suffix: &str) -> Ident {
        Ident::new(format!("{}{suffix}", self.text())).with_span(self.span())
    }
}

/// One table entry: an api name and the tokens after its separator, if any.
pub(crate) struct Entry {
    pub(crate) names: ApiNames,
    pub(crate) value: Option<TokenStream>,
}

/// One `label: entries;` section of a table.
pub(crate) struct Section {
    pub(crate) label: Ident,
    pub(crate) entries: Vec<Entry>,
}

/// Whether `tokens[at..]` starts with the punctuation characters of
/// `separator`, such as `=>`, which reaches a proc macro as two `Punct`s.
fn starts_with_separator(tokens: &[TokenTree], at: usize, separator: &str) -> bool {
    separator.chars().enumerate().all(|(offset, c)| {
        tokens
            .get(at + offset)
            .and_then(TokenTree::as_punct)
            .is_some_and(|punct| punct.as_str().starts_with(c))
    })
}

fn is_comma(token: &TokenTree) -> bool { token.is_punct_comma() }

fn is_semi(token: &TokenTree) -> bool { token.is_punct_semi() }

fn error_at(tokens: &[TokenTree], at: usize, message: &str) -> ParseError {
    let span = tokens.get(at).map_or_else(Span::call_site, TokenTree::span);
    ParseError::new(span, message)
}

/// Parses a whole table. `separator` is what may follow an api name to give
/// the entry a value (`=>` or `=`); `labels` are the section labels the macro
/// accepts. A label may appear at most once, and an api name at most once in
/// the whole table.
pub(crate) fn parse_sections(
    input: TokenStream,
    separator: &str,
    labels: &[&str],
) -> Result<Vec<Section>, ParseError> {
    let tokens = input.into_inner();
    let mut sections: Vec<Section> = Vec::new();
    let mut seen_apis = BTreeSet::new();
    let mut at = 0;

    while at < tokens.len() {
        let Some(label) = tokens[at].as_ident().cloned() else {
            return Err(error_at(&tokens, at, "expected a section label"));
        };
        if !labels.contains(&label.text()) {
            return Err(ParseError::new(
                label.span(),
                format!(
                    "unknown section `{}`; expected one of: {}",
                    label.text(),
                    labels.join(", ")
                ),
            ));
        }
        if sections
            .iter()
            .any(|section| section.label == *label.text())
        {
            return Err(ParseError::new(
                label.span(),
                format!("section `{}` appears twice", label.text()),
            ));
        }
        at += 1;
        if !tokens.get(at).is_some_and(TokenTree::is_punct_colon) {
            return Err(error_at(
                &tokens,
                at,
                "expected `:` after the section label",
            ));
        }
        at += 1;

        let mut entries = Vec::new();
        loop {
            if tokens.get(at).is_some_and(is_semi) {
                at += 1;
                break;
            }
            let Some(api) = tokens.get(at).and_then(TokenTree::as_ident).cloned() else {
                return Err(error_at(&tokens, at, "expected an api name or `;`"));
            };
            if !seen_apis.insert(api.text().to_owned()) {
                return Err(ParseError::new(
                    api.span(),
                    format!("`{}` appears twice in the table", api.text()),
                ));
            }
            at += 1;

            let value = if starts_with_separator(&tokens, at, separator) {
                at += separator.len();
                let start = at;
                while tokens.get(at).is_some_and(|t| !is_comma(t) && !is_semi(t)) {
                    at += 1;
                }
                if at == start {
                    return Err(error_at(
                        &tokens,
                        at,
                        &format!("expected tokens after `{separator}`"),
                    ));
                }
                Some(TokenStream::from(&tokens[start..at]))
            } else {
                None
            };
            entries.push(Entry {
                names: ApiNames::new(&api),
                value,
            });

            match tokens.get(at) {
                Some(token) if is_comma(token) => at += 1,
                Some(token) if is_semi(token) => {}
                _ => {
                    return Err(error_at(
                        &tokens,
                        at,
                        &format!("expected `,`, `;` or `{separator}` after an api name"),
                    ));
                }
            }
        }
        sections.push(Section { label, entries });
    }

    Ok(sections)
}

/// The entries of the section labelled `label`, or none when the table leaves
/// it out.
pub(crate) fn section<'a>(sections: &'a [Section], label: &str) -> &'a [Entry] {
    sections
        .iter()
        .find(|section| section.label == *label)
        .map_or(&[], |section| &section.entries)
}

#[cfg(test)]
mod tests {
    use {
        super::{ApiNames, Ident, TokenStream, parse_sections, section},
        assert2::assert,
    };

    fn texts(names: &ApiNames) -> [String; 6] {
        [
            &names.api,
            &names.snake,
            &names.request_module,
            &names.response_module,
            &names.request_type,
            &names.response_type,
        ]
        .map(|ident| ident.text().to_owned())
    }

    #[test]
    fn derives_every_name_the_tables_use() {
        for (api, snake) in [
            ("CreateAcls", "create_acls"),
            ("AddPartitionsToTxn", "add_partitions_to_txn"),
            ("ApiVersions", "api_versions"),
            ("OffsetForLeaderEpoch", "offset_for_leader_epoch"),
            (
                "DescribeUserScramCredentials",
                "describe_user_scram_credentials",
            ),
            ("GetTelemetrySubscriptions", "get_telemetry_subscriptions"),
            (
                "StreamsGroupTopologyDescriptionUpdate",
                "streams_group_topology_description_update",
            ),
            ("Metadata", "metadata"),
        ] {
            let names = ApiNames::new(&Ident::new(api));
            assert!(
                texts(&names)
                    == [
                        api.to_owned(),
                        snake.to_owned(),
                        format!("{snake}_request"),
                        format!("{snake}_response"),
                        format!("{api}Request"),
                        format!("{api}Response"),
                    ]
            );
        }
    }

    /// One parsed section: its label and its entries.
    #[derive(Debug, PartialEq)]
    struct ParsedSection {
        label: String,
        entries: Vec<ParsedEntry>,
    }

    /// One parsed entry: the API name and the value text, empty when absent.
    #[derive(Debug, PartialEq)]
    struct ParsedEntry {
        api: String,
        value: String,
    }

    /// A parsed table, or the parse error.
    type Parsed = Result<Vec<ParsedSection>, String>;

    fn parse(source: &str) -> Parsed {
        let tokens: TokenStream = source.parse().map_err(|e| format!("{e:?}"))?;
        let sections = parse_sections(tokens, "=>", &["a", "b", "c"]).map_err(|e| e.to_string())?;
        Ok(sections
            .iter()
            .map(|s| {
                let entries = s
                    .entries
                    .iter()
                    .map(|e| {
                        let value = e.value.as_ref().map(ToString::to_string);
                        ParsedEntry {
                            api: e.names.api.text().to_owned(),
                            value: value.unwrap_or_default(),
                        }
                    })
                    .collect();
                ParsedSection {
                    label: s.label.text().to_owned(),
                    entries,
                }
            })
            .collect())
    }

    #[test]
    fn parses_sections_and_values() {
        let parsed = parse("a: One, Two => x::y::handle; b: ;");
        assert!(
            parsed
                == Ok(vec![
                    ParsedSection {
                        label: "a".to_owned(),
                        entries: vec![
                            ParsedEntry {
                                api: "One".to_owned(),
                                value: String::new(),
                            },
                            ParsedEntry {
                                api: "Two".to_owned(),
                                value: "x :: y :: handle".to_owned(),
                            },
                        ],
                    },
                    ParsedSection {
                        label: "b".to_owned(),
                        entries: vec![],
                    },
                ])
        );
    }

    #[test]
    fn rejects_malformed_tables() {
        for source in [
            "d: One;",
            "a: One; a: Two;",
            "a: One, One;",
            "a: One; b: One;",
            "a One;",
            "a: One",
            "a: One =>;",
            "a: One Two;",
        ] {
            assert!(parse(source).is_err(), "{source}");
        }
    }

    #[test]
    fn a_missing_section_has_no_entries() {
        let tokens: TokenStream = "a: One;".parse().unwrap();
        let sections = parse_sections(tokens, "=>", &["a", "b"]).unwrap();
        assert!(section(&sections, "a").len() == 1);
        assert!(section(&sections, "b").is_empty());
    }
}
