//! Flat share-state envelopes with native field and value expressions.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups},
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{Parse, ParseError, Parser, Path, PathArguments, Token},
        token::{Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

struct Clause {
    name: &'static str,
    fields: bool,
}

fn header(input: &TokenStream) -> Result<Vec<Path>, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut types = vec![Path::parse(&parser)?];
    <Token![,]>::parse(&parser)?;
    types.push(Path::parse(&parser)?);
    if <Token![,]>::peek(parser.cursor()) {
        <Token![,]>::parse(&parser)?;
        types.push(Path::parse(&parser)?);
    }
    if types.iter().any(|path| {
        path.iter()
            .any(|segment| !matches!(segment.args, PathArguments::None))
    }) {
        return Err(parser.error("expected share-state schema paths without generic arguments"));
    }
    <Token![;]>::parse(&parser)?;
    let clauses = if types.len() == 3 {
        &[
            Clause {
                name: "group",
                fields: false,
            },
            Clause {
                name: "topic",
                fields: false,
            },
            Clause {
                name: "fields",
                fields: true,
            },
        ][..]
    } else {
        &[
            Clause {
                name: "group",
                fields: false,
            },
            Clause {
                name: "topic",
                fields: false,
            },
            Clause {
                name: "partitions",
                fields: false,
            },
        ][..]
    };
    for Clause { name, fields } in clauses {
        expect_name(&parser, name, &format!("expected {name} clause"))?;
        let value = grouped_value(&parser, "expected grouped share-state value")?;
        if (*fields && !value.delim.is_brace())
            || (!*fields && (!value.delim.is_bracket() || value.tokens.is_empty()))
        {
            return Err(ParseError::new(
                value.span(),
                "expected braced fields or a nonempty bracketed value",
            ));
        }
        <Token![;]>::parse(&parser)?;
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected share-state envelope tokens"));
    }
    Ok(types)
}

fn scaffold(types: &[Path]) -> TokenStream {
    let envelope = &types[0];
    let topic = &types[1];
    if types.len() == 3 {
        let partition = &types[2];
        moxy::template! {
            {{ envelope }} {
                group_id: __share_group.to_string(),
                topics: vec![{{ topic }} {
                    topic_id: krabka_protocol::primitives::uuid::Uuid(*__share_topic.as_bytes()),
                    partitions: vec![{{ partition }} { __share_fields ..Default::default() }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    } else {
        moxy::template! {
            {{ envelope }} {
                group_id: __share_group.to_string(),
                topics: vec![{{ topic }} {
                    topic_id: __share_topic,
                    partitions: __share_partitions,
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let types = header(&input.clone().into())?;
    let names: &[&str] = if types.len() == 3 {
        &["__share_group", "__share_topic", "__share_fields"]
    } else {
        &["__share_group", "__share_topic", "__share_partitions"]
    };
    let mut names = names.iter();
    let values: Vec<_> = native_groups(input)
        .map(|group| {
            let name = names.next().expect("validated share-state clause");
            let delimiter = if *name == "__share_fields" {
                Delimiter::None
            } else {
                Delimiter::Parenthesis
            };
            let mut native = Group::new(delimiter, group.stream());
            native.set_span(group.span());
            SpliceValue {
                name: (*name).to_owned(),
                group: native,
            }
        })
        .collect();
    Ok(splice(scaffold(&types).into(), &values, &[]))
}

#[cfg(test)]
mod tests {
    use {
        super::{header, scaffold},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn validates_typed_envelopes_and_retains_rustc_owned_expressions() {
        let request = "Request, Topic, Partition; group [group()]; topic [topic()]; fields { partition: choose!(|&Row { x, y }| x + y), };".parse().unwrap();
        let types = header(&request).unwrap();
        assert!(types.len() == 3);
        assert!(compact(&scaffold(&types)).contains("group_id:__share_group.to_string()"));
        let rows = "Request, Topic; group [group]; topic [id]; partitions [rows()];"
            .parse()
            .unwrap();
        let types = header(&rows).unwrap();
        assert!(compact(&scaffold(&types)).contains("partitions:__share_partitions"));
    }

    #[yare::parameterized(
        empty = { "" },
        missing_topic = { "Request; group [g];" },
        generic_request = { "Request<T>, Topic; group [g]; topic [id]; partitions [rows];" },
        wrong_clause = { "Request, Topic; wrong [g]; topic [id]; partitions [rows];" },
        ungrouped = { "Request, Topic; group g; topic [id]; partitions [rows];" },
        parenthesized = { "Request, Topic; group (g); topic [id]; partitions [rows];" },
        empty_topic = { "Request, Topic; group [g]; topic []; partitions [rows];" },
        empty_partitions = { "Request, Topic; group [g]; topic [id]; partitions [];" },
        empty_group = { "Request, Topic, Partition; group []; topic [id]; fields {};" },
        empty_fields = { "Request, Topic, Partition; group [g]; topic [id]; fields [];" },
        trailing = { "Request, Topic, Partition; group [g]; topic [id]; fields {}; extra" },
    )]
    fn rejects_malformed_constructor_headers(invalid: &str) {
        assert2::assert!(header(&invalid.parse().unwrap()).is_err());
    }
}
