//! Exhaustive wire constructors with protocol-only fields supplied once.

use {
    moxy::{
        ast::{Expr, Parse, ParseError, Parser, Token},
        token::{Spanner, TokenStream, TokenTree},
    },
    proc_macro::{Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

pub(crate) fn expand_native(
    input: NativeTokenStream,
    unthrottled: bool,
) -> Result<NativeTokenStream, ParseError> {
    let parsed = TokenStream::from(input.clone());
    let needs_comma = validate(&parsed)?;
    let mut tokens: Vec<_> = input.into_iter().collect();
    let Some(NativeTokenTree::Group(body)) = tokens.pop() else {
        return Err(ParseError::new(parsed.span(), "expected a struct body"));
    };
    let prefix: NativeTokenStream = moxy::template! {
        @if unthrottled { throttle_time_ms: 0, }
    }
    .into();
    let suffix: NativeTokenStream = moxy::template! {
        @if needs_comma { , }
        unknown_tagged_fields: ::krabka_protocol::UnknownTaggedFields::default(),
    }
    .into();
    // Keep native nested groups: Moxy combines their opening and closing delimiter spans.
    let fields = prefix
        .into_iter()
        .chain(body.stream())
        .chain(suffix)
        .collect();
    let mut replacement = Group::new(body.delimiter(), fields);
    replacement.set_span(body.span());
    tokens.push(NativeTokenTree::Group(replacement));
    Ok(tokens.into_iter().collect())
}

fn validate(input: &TokenStream) -> Result<bool, ParseError> {
    let mut tokens = input.clone().into_inner();
    let Some(TokenTree::Group(body)) = tokens.last_mut() else {
        return Err(ParseError::new(input.span(), "expected a struct body"));
    };
    if !body.delim.is_brace() {
        return Err(ParseError::new(
            body.span(),
            "expected a brace-delimited struct body",
        ));
    }
    // Validate only the header; rustc handles closure patterns and other field expressions.
    let fields = std::mem::take(&mut body.tokens);
    let header = TokenStream::from(tokens);
    let parser = Parser::from_tokens(&header);
    let Expr::Struct(_) = Expr::parse(&parser)? else {
        return Err(parser.error("expected an exhaustive struct literal"));
    };
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after the struct literal"));
    }
    let field_parser = Parser::from_tokens(&fields);
    let mut field_start = true;
    while !field_parser.is_empty() {
        if field_start && <Token![..]>::peek(field_parser.cursor()) {
            return Err(field_parser
                .error("wire constructors require every field; struct updates are not allowed"));
        }
        field_start = field_parser
            .advance()
            .is_some_and(TokenTree::is_punct_comma);
    }

    Ok(fields
        .iter()
        .last()
        .is_some_and(|last| !last.is_punct_comma()))
}

#[cfg(test)]
mod tests {
    use {super::validate, assert2::assert};

    #[test]
    fn rejects_updates_and_non_struct_inputs() {
        for input in [
            "Response { ..other }",
            "Response { version: 2, ..other }",
            "Response",
            "Response {} extra",
            "42",
        ] {
            assert!(validate(&input.parse().unwrap()).is_err(), "{input}");
        }
    }

    #[test]
    fn detects_whether_the_explicit_fields_need_a_separator() {
        for (input, needs_comma) in [
            ("module::Response {}", false),
            ("module::Response { version: 2, error_code }", true),
            ("module::Response { version: 2, error_code, }", false),
        ] {
            assert!(validate(&input.parse().unwrap()).unwrap() == needs_comma);
        }
    }

    #[test]
    fn nested_struct_updates_are_field_expressions() {
        let input = "Response { value: Inner { ..other } }".parse().unwrap();
        assert!(validate(&input).is_ok());
    }
}
