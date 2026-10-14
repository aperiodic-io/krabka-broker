//! Ordered runtime field assignments without recursive macro expansion.

use moxy::{
    ast::{Expr, Parse, ParseError, Parser, Token},
    token::{Ident, Span, ToTokenStream, TokenStream},
};

struct Assignment {
    kind: Ident,
    field: Ident,
    path: Vec<Ident>,
    range: Option<Expr>,
}

#[derive(moxy::ToTokens)]
#[moxy(template {
    @for assignment in &self.fields {
        if let Some({{ self.binding }}) = {{ self.runtime }}.{{ assignment.field }} {
            {{ self.target }} @for part in &assignment.path { .{{ part }} } =
            @match (assignment.kind.text()) {
                "plain" => { {{ self.binding }} }
                "duration" => { positive_time(stringify!({{ assignment.field }}), {{ self.binding }})?.to_std() }
                _ => {
                    {{ assignment.kind }}(stringify!({{ assignment.field }}), {{ self.binding }}
                        @if let Some(range) = &assignment.range { , {{ range }} }
                    )?
                }
            };
        }
    }
})]
struct Assignments {
    runtime: Ident,
    target: Ident,
    fields: Vec<Assignment>,
    binding: Ident,
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let runtime = Ident::parse(&parser)?;
    <Token![=>]>::parse(&parser)?;
    let target = Ident::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    let mut fields = Vec::new();
    while !parser.is_empty() {
        let kind = Ident::parse(&parser)?;
        <Token![:]>::parse(&parser)?;
        loop {
            let field = Ident::parse(&parser)?;
            let mut path = vec![field.clone()];
            if <Token![=>]>::peek(parser.cursor()) {
                <Token![=>]>::parse(&parser)?;
                path = vec![Ident::parse(&parser)?];
                while <Token![.]>::peek(parser.cursor()) {
                    <Token![.]>::parse(&parser)?;
                    path.push(Ident::parse(&parser)?);
                }
            }
            let range = if <Token![in]>::peek(parser.cursor()) {
                <Token![in]>::parse(&parser)?;
                Some(Expr::parse(&parser)?)
            } else {
                None
            };
            if range.is_some() && (kind == "plain" || kind == "duration") {
                return Err(ParseError::new(
                    kind.span(),
                    "this assignment kind takes no range",
                ));
            }
            fields.push(Assignment {
                kind: kind.clone(),
                field,
                path,
                range,
            });
            if !<Token![,]>::peek(parser.cursor()) {
                break;
            }
            <Token![,]>::parse(&parser)?;
        }
        <Token![;]>::parse(&parser)?;
    }
    Ok(Assignments {
        runtime,
        target,
        fields,
        // Retain macro_rules hygiene when callers also name a binding `value`.
        binding: Ident::new("value").with_span(Span::mixed_site()),
    }
    .to_token_stream())
}

#[cfg(test)]
mod tests {
    use super::expand;

    #[yare::parameterized(
        missing_plain_value = { "runtime => cfg; plain:;" },
        missing_plain_terminator = { "runtime => cfg; plain: enabled" },
        plain_range = { "runtime => cfg; plain: enabled in 0..=1;" },
        duration_range = { "runtime => cfg; duration: timeout in 1..=10;" },
        nested_validation = { "runtime => cfg; validate: count => nested.;" },
    )]
    fn refuses_incomplete_tables_and_ranges_on_unvalidated_assignments(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
    }
}
