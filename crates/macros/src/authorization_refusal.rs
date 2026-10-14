//! A typed Kafka authorization refusal with an optional native error message.

use {
    crate::meta::{arguments, native_expression, plain_path},
    moxy::{
        ast::{Parse, ParseError, Parser, Path},
        token::{Ident, Span, TokenStream},
    },
    proc_macro::{Delimiter, TokenStream as NativeTokenStream, TokenTree},
};

struct Refusal {
    response: Path,
    output: Ident,
    version: Ident,
    error: Path,
}

fn declaration(input: TokenStream) -> Result<Refusal, ParseError> {
    let args = arguments(input, 4)?;
    let response = plain_path(
        &args[0],
        args[0].span(),
        "authorization refusal needs a concrete response type",
    )?;
    let error = plain_path(
        &args[3],
        args[3].span(),
        "authorization refusal needs an error-code path",
    )?;
    let mut names = Vec::new();
    for tokens in &args[1..3] {
        let parser = Parser::from_tokens(tokens);
        let ident = Ident::parse(&parser)?;
        if !parser.is_empty() {
            return Err(parser.error("authorization output and version need local names"));
        }
        names.push(ident);
    }
    Ok(Refusal {
        response,
        output: names.remove(0),
        version: names.remove(0),
        error,
    })
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let mut tokens: Vec<_> = input.into_iter().collect();
    let message = match tokens.last() {
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Brace => {
            let Some(TokenTree::Group(message)) = tokens.pop() else {
                unreachable!()
            };
            if message.stream().is_empty() {
                return Err(ParseError::new(
                    message.span().into(),
                    "authorization error message must be nonempty",
                ));
            }
            if !matches!(tokens.pop(), Some(TokenTree::Punct(comma)) if comma.as_char() == ',') {
                return Err(ParseError::new(
                    message.span().into(),
                    "separate the native message block with a comma",
                ));
            }
            Some(message)
        }
        _ => None,
    };
    let args = declaration(NativeTokenStream::from_iter(tokens).into())?;
    let Refusal {
        response,
        output,
        version,
        error,
    } = args;
    let code = Ident::new("error_code").with_span(Span::mixed_site());
    let text = Ident::new("error_message").with_span(Span::mixed_site());
    let mut body: NativeTokenStream = moxy::template! { let {{ code }} = {{ error }}; }.into();
    let has_message = message.is_some();
    if let Some(message) = message {
        body.extend(NativeTokenStream::from(
            moxy::template! { let {{ text }} = },
        ));
        body.extend([TokenTree::Group(message)]);
        body.extend(NativeTokenStream::from(moxy::template! { ; }));
    }
    body.extend(NativeTokenStream::from(moxy::template! {
        {{ response }} { error_code: {{ code }}, @if has_message { error_message: {{ text }}, } ..Default::default() }.encode(&mut {{ output }}, {{ version }})?
    }));
    Ok(native_expression(body))
}

#[cfg(test)]
mod tests {
    use super::declaration;

    #[yare::parameterized(
        missing_code = { "Response, output, version" },
        generic_response = { "Response<T>, output, version, DENIED" },
        borrowed_output = { "Response, &mut output, version, DENIED" },
        computed_version = { "Response, output, version + 1, DENIED" },
        computed_code = { "Response, output, version, DENIED + 1" },
    )]
    fn refusal_requires_a_response_code_and_two_local_bindings(input: &str) {
        assert2::assert!(declaration(input.parse().unwrap()).is_err(), "{input}");
    }
}
