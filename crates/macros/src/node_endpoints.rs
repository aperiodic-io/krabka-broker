//! Move a locally owned endpoint into each explicitly listed wire response.

use moxy::ast::{ItemStruct, ParseError};

struct Targets {
    paths: Vec<moxy::ast::Path>,
}

impl moxy::ast::FromMeta for Targets {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        use moxy::{
            ast::{List, MetaContent, Parser, Path, PathArguments, Token},
            token::{Spanner, ToTokenStream},
        };
        let MetaContent::List(group) = &meta.content else {
            return Err(ParseError::new(
                meta.span(),
                "expected `targets(Type, ...)`",
            ));
        };
        if !group.delim.is_paren() {
            return Err(ParseError::new(
                group.span(),
                "endpoint targets need parentheses",
            ));
        }
        let paths = List::<Path, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
        let mut targets = Vec::new();
        for path in paths {
            if path
                .iter()
                .any(|segment| !matches!(segment.args, PathArguments::None))
                || targets.iter().any(|target: &Path| {
                    target.to_token_stream().to_string() == path.to_token_stream().to_string()
                })
            {
                return Err(ParseError::new(
                    path.span(),
                    "endpoint targets must be distinct type paths without arguments",
                ));
            }
            targets.push(path);
        }
        if targets.is_empty() {
            return Err(ParseError::new(
                group.span(),
                "list at least one endpoint target",
            ));
        }
        Ok(Self { paths: targets })
    }
}

#[derive(moxy::FromMeta)]
struct EndpointArgs {
    targets: Targets,
}

pub(crate) fn expand(item: ItemStruct) -> Result<moxy::token::TokenStream, ParseError> {
    use {
        crate::meta::{field_ident, named_fields},
        moxy::ast::Attributed,
    };
    let fields = named_fields(&item, "NodeEndpoints")?;
    let names = fields
        .iter()
        .map(|field| field_ident(field, "NodeEndpoints").cloned())
        .collect::<Result<Vec<_>, _>>()?;
    if !item.generics.params.is_empty()
        || names.len() != 4
        || ["node_id", "host", "port", "rack"]
            .iter()
            .any(|name| !names.iter().any(|ident| ident.text() == *name))
    {
        return Err(ParseError::new(
            item.ident.span(),
            "`NodeEndpoints` needs a non-generic struct with node_id, host, port and rack",
        ));
    }
    let targets = item
        .parse_meta::<EndpointArgs>("node_endpoints")?
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`NodeEndpoints` needs `#[node_endpoints(targets(Type, ...))]`",
            )
        })?
        .targets
        .paths;
    let ident = item.ident;
    Ok(moxy::template! {
        @for target in &targets {
            impl ::core::convert::From<{{ ident }}> for {{ target }} {
                fn from(endpoint: {{ ident }}) -> Self {
                    Self {
                        @for name in &names {
                            {{ name }}: endpoint.{{ name }},
                        }
                        ..Self::default()
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{EndpointArgs, expand},
        crate::meta::{RejectedStruct, assert_rejected_structs, compact},
        assert2::assert,
        moxy::ast::{FromMeta, ItemStruct, Meta},
    };

    #[test]
    fn emits_one_owned_conversion_per_explicit_target() {
        let item: ItemStruct = moxy::parse!("#[node_endpoints(targets(fetch::NodeEndpoint, produce::NodeEndpoint))] struct Endpoint { rack: Option<String>, port: i32, host: String, node_id: i32 }").unwrap();
        let output = compact(&expand(item).unwrap());
        assert!(
            output
                .matches("impl::core::convert::From<Endpoint>")
                .count()
                == 2
        );
        assert!(output.contains("forfetch::NodeEndpoint"));
        assert!(output.contains("forproduce::NodeEndpoint"));
        assert!(output.matches("..Self::default()").count() == 2);
    }

    #[yare::parameterized(
        empty = { "node_endpoints(targets())" },
        duplicate_paths = { "node_endpoints(targets(wire::Endpoint, wire::Endpoint))" },
        generic_path = { "node_endpoints(targets(Endpoint<T>))" },
        bracketed = { "node_endpoints(targets[wire::Endpoint])" },
        assignment = { "node_endpoints(targets = wire::Endpoint)" },
    )]
    fn target_metadata_requires_distinct_plain_paths_in_parentheses(input: &str) {
        let metadata: Meta = moxy::parse!({ input }).unwrap();
        assert2::assert!(EndpointArgs::from_meta(&metadata).is_err(), "{input}");
    }

    #[test]
    fn source_struct_requires_endpoint_fields_and_its_target_attribute() {
        assert_rejected_structs(
            &[
                RejectedStruct {
                    label: "missing attribute",
                    declaration: "struct Endpoint { node_id: i32, host: String, port: i32, rack: Option<String> }",
                },
                RejectedStruct {
                    label: "missing field",
                    declaration: "#[node_endpoints(targets(wire::Endpoint))] struct Endpoint { node_id: i32, host: String, port: i32 }",
                },
                RejectedStruct {
                    label: "generic source",
                    declaration: "#[node_endpoints(targets(wire::Endpoint))] struct Endpoint<T> { node_id: i32, host: String, port: i32, rack: T }",
                },
            ],
            expand,
        );
    }
}
