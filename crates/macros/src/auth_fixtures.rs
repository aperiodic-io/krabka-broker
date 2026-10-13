//! SCRAM expression fixtures whose inferred phase type is private upstream.

use {
    crate::fixtures::name,
    moxy::{
        ast::{Cursor, Expr, List, Parse, ParseError, Parser, Token},
        token::TokenStream,
    },
};

struct Arguments(List<Expr, Token![,]>);

impl Parse for Arguments {
    fn peek(cursor: Cursor<'_>) -> bool { cursor.is_empty() || Expr::peek(cursor) }

    fn parse(parser: &Parser) -> Result<Self, ParseError> { List::parse_all(parser).map(Self) }

    fn skip(cursor: Cursor<'_>) -> Option<Cursor<'_>> {
        let parser = Parser::from_cursor(cursor);
        Self::parse(&parser).ok()?;
        Some(parser.cursor())
    }
}

pub(crate) fn scram_client_first(input: TokenStream) -> Result<TokenStream, ParseError> {
    let span = input.span();
    let arguments = moxy::parse!({ input } as Arguments)?.0;
    let expressions: Vec<_> = arguments.iter().collect();
    let [user, password, mechanism] = expressions.as_slice() else {
        return Err(ParseError::new(
            span,
            "expected user, password, mechanism expressions",
        ));
    };
    Ok(moxy::template! {
        ::krabka_security::ScramClientExchange::new(
            ({{ user }}).to_string(),
            ({{ password }}).as_bytes().to_vec(),
            {{ mechanism }},
        )
        .client_first()
        .map_err(|e| ::std::io::Error::other(format!("scram client_first: {e:?}")))
    })
}

pub(crate) fn scram_client_proof(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(client_key: &[u8], signature: &[u8]) -> Vec<u8> {
            client_key.iter().zip(signature).map(|(key, signed)| key ^ signed).collect()
        }
    })
}
