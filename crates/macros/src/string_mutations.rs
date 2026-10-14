//! Independent required/optional request-string mutation tables.

use moxy::{
    ast::{Parse, ParseError, Parser, Token},
    token::{Delim, Ident, IntSuffix, LitInt, LitStr, Span, TokenStream},
};

enum Access {
    Field(Ident),
    Index(LitInt),
}

#[derive(moxy::ToTokens)]
#[moxy(template {
    @for access in &self.parts {
        @match (access) {
            Access::Field(field) => { .{{ field }} }
            Access::Index(index) => { [{{ index }}] }
        }
    }
})]
struct Selector {
    parts: Vec<Access>,
}

struct Mutation {
    label: LitStr,
    optional: bool,
    selector: Selector,
}

fn selector(parser: &Parser) -> Result<Selector, ParseError> {
    let mut parts = vec![Access::Field(Ident::parse(parser)?)];
    while !parser.is_empty() {
        if <Token![.]>::peek(parser.cursor()) {
            <Token![.]>::parse(parser)?;
            parts.push(Access::Field(Ident::parse(parser)?));
        } else {
            let index = parser.parse_group(Delim::Bracket)?;
            let value = LitInt::parse(&index)?;
            if !index.is_empty() || !matches!(value.suffix(), IntSuffix::None) {
                return Err(index.error("mutation indexes need an unsuffixed integer"));
            }
            parts.push(Access::Index(value));
        }
    }
    Ok(Selector { parts })
}

fn declarations(input: &TokenStream) -> Result<Vec<Mutation>, ParseError> {
    let parser = Parser::from_tokens(input);
    let offsets = if Ident::peek(parser.cursor()) {
        let policy = Ident::parse(&parser)?;
        if policy.text() != "offsets" {
            return Err(ParseError::new(
                policy.span(),
                "expected offsets; or labelled mutations",
            ));
        }
        <Token![;]>::parse(&parser)?;
        true
    } else {
        false
    };
    let mut mutations = Vec::new();
    while !parser.is_empty() {
        let label = LitStr::parse(&parser)?;
        <Token![=>]>::parse(&parser)?;
        let kind = Ident::parse(&parser)?;
        let optional = match kind.text() {
            "required" => false,
            "optional" => true,
            _ => {
                return Err(ParseError::new(
                    kind.span(),
                    "expected required or optional",
                ));
            }
        };
        let path = parser.parse_group(Delim::Bracket)?;
        mutations.push(Mutation {
            label,
            optional,
            selector: selector(&path)?,
        });
        if parser.is_empty() {
            break;
        }
        <Token![,]>::parse(&parser)?;
    }
    if offsets {
        // Extra fields precede the shared offset fields, as in the original independent table.
        for entry in [
            "\"group id\" => required[group_id]",
            "\"member id\" => required[member_id]",
            "\"instance id\" => optional[group_instance_id]",
            "\"topic name\" => required[topics[0].name]",
            "\"metadata\" => optional[topics[0].partitions[0].committed_metadata]",
        ] {
            mutations.extend(declarations(
                &entry.parse().expect("fixed offset mutation"),
            )?);
        }
    }
    if mutations.is_empty() {
        return Err(parser.error("declare at least one string mutation"));
    }
    Ok(mutations)
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let mutations = declarations(input)?;
    let request = Ident::new("request").with_span(Span::mixed_site());
    let value = Ident::new("value").with_span(Span::mixed_site());
    Ok(moxy::template! {
        &[
            @for mutation in &mutations {
                Field {
                    name: {{ mutation.label }},
                    set: |{{ request }}, {{ value }}| {
                        {{ request }}{{ mutation.selector }} =
                            @if mutation.optional { Some({{ value }}) }
                            @else { {{ value }} };
                    },
                },
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{declarations, expand},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn explicit_and_offset_mutations_keep_order_and_optional_wrapping() {
        let input = "offsets; \"transactional id\" => required[transactional_id]"
            .parse()
            .unwrap();
        let entries = declarations(&input).unwrap();
        assert!(entries.len() == 6 && entries[0].label.value() == "transactional id");
        let output = compact(&expand(&input).unwrap());
        assert!(output.contains(".topics[0].partitions[0].committed_metadata=Some(value)"));
        assert!(output.contains(".transactional_id=value"));
        assert!(declarations(&"offsets;".parse().unwrap()).unwrap().len() == 5);
    }

    #[yare::parameterized(
        empty = { "" },
        unknown_policy = { "other;" },
        mutable_policy = { "\"x\" => mutable[x]" },
        empty_selector = { "\"x\" => required[]" },
        method_call = { "\"x\" => required[x()]" },
        nested_index = { "\"x\" => required[x[index]]" },
        non_usize_index = { "\"x\" => required[x[0u8]]" },
        trailing_tokens = { "\"x\" => required[x] trailing" },
    )]
    fn rejects_non_field_selectors_and_unknown_policies(input: &str) {
        assert2::assert!(declarations(&input.parse().unwrap()).is_err(), "{input}");
    }
}
