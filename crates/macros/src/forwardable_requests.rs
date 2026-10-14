//! One explicit key-to-request table for envelope admission and decoding.

use {
    crate::meta::parse_skip,
    moxy::{
        ast::{Cursor, List, Parse, ParseError, Parser, Path, Token},
        token::{IntSuffix, LitInt, TokenStream},
    },
    std::collections::HashSet,
};

struct ForwardingEntry {
    key: LitInt,
    request: Path,
}

impl Parse for ForwardingEntry {
    fn peek(cursor: Cursor<'_>) -> bool { LitInt::peek(cursor) }

    fn skip(cursor: Cursor<'_>) -> Option<Cursor<'_>> { parse_skip::<Self>(cursor) }

    fn parse(parser: &Parser) -> Result<Self, ParseError> {
        let key = LitInt::parse(parser)?;
        if i16::try_from(key.value()).is_err()
            || !matches!(key.suffix(), IntSuffix::None | IntSuffix::I16)
        {
            return Err(ParseError::new(
                key.span(),
                "expected a nonnegative i16 api key",
            ));
        }
        <Token![=>]>::parse(parser)?;
        let request = Path::parse(parser)?;
        Ok(Self { key, request })
    }
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let requests: List<ForwardingEntry, Token![,]> = List::parse_all(&parser)?;
    if requests.is_empty() {
        return Err(parser.error("expected at least one forwardable request"));
    }
    let mut keys = HashSet::new();
    for request in &requests {
        if !keys.insert(request.key.value()) {
            return Err(ParseError::new(
                request.key.span(),
                "duplicate forwardable api key",
            ));
        }
    }
    Ok(moxy::template! {
        /// `ApiKeys.forwardable`, read out of `kafka-clients-4.3.1.jar`, plus
        /// `UnregisterController`, which Kafka trunk's `ApiKeys` declares
        /// forwardable.
        ///
        /// KIP-590 lets a broker wrap only these api keys, and
        /// `EnvelopeUtils.handleEnvelopeRequest` refuses any other embedded key
        /// with `InvalidRequestException`. Accepting a key outside the set would
        /// let a forwarding hop launder a request the client could not have sent
        /// itself.
        const FORWARDABLE_API_KEYS: &[ApiKeyCode] = &[
            @for row in &requests { {{ row.key }}, }
        ];

        /// Whether `body` decodes as the request of `api_key` at `version`.
        ///
        /// `RequestContext.parseRequest` decodes the body and nothing checks
        /// what is left after it, so neither does this. A key outside the
        /// forwardable set decodes as nothing.
        fn embedded_body_decodes(api_key: ApiKeyCode, version: ApiVersion, body: &[u8]) -> bool {
            let mut cur = body;
            match api_key {
                @for row in &requests { {{ row.key }} => {{ row.request }}::decode(&mut cur, version).is_ok(), }
                _ => false,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::compact,
        assert2::assert,
        moxy::ast::{ItemConst, ItemFn, Parse, Parser},
    };

    #[test]
    fn emitted_catalog_and_decoder_keep_key_order_and_the_is_ok_boundary() {
        let output = expand(&"20 => wire::Delete, 19 => wire::Create".parse().unwrap()).unwrap();
        let parser = Parser::from_tokens(&output);
        let catalog = ItemConst::parse(&parser).unwrap();
        let decoder = ItemFn::parse(&parser).unwrap();
        assert!(parser.is_empty());
        assert!(catalog.ident == "FORWARDABLE_API_KEYS");
        assert!(decoder.sig.ident == "embedded_body_decodes");
        assert!(decoder.sig.params.inner.inputs.len() == 3);
        let rendered = compact(&output);
        assert!(rendered.contains("&[20,19,]"));
        assert!(
            rendered.find("20=>wire::Delete::decode").unwrap()
                < rendered.find("19=>wire::Create::decode").unwrap()
        );
        assert!(rendered.contains("decode(&mutcur,version).is_ok()"));
        assert!(rendered.contains("_=>false"));
    }

    #[yare::parameterized(
        empty = { "" },
        missing_value = { "19 =>" },
        trailing_identifier = { "19 => ForwardingEntry extra" },
        semicolon = { "19 => ForwardingEntry;" },
        missing_arrow = { "19 ForwardingEntry" },
        string_key = { "\"19\" => ForwardingEntry" },
        negative_key = { "-1 => ForwardingEntry" },
        above_i16 = { "32768 => ForwardingEntry" },
        wrong_suffix = { "19u32 => ForwardingEntry" },
        repeated_key = { "19 => ForwardingEntry, 19 => Other" },
        repeated_value = { "1 => ForwardingEntry, 0x1 => Other" },
    )]
    fn rejects_empty_malformed_out_of_range_and_duplicate_tables(source: &str) {
        assert2::assert!(expand(&source.parse().unwrap()).is_err(), "{source}");
    }

    #[test]
    fn accepts_one_explicit_i16_suffix_and_hexadecimal_keys() {
        assert!(
            expand(
                &"19i16 => requests::Create, 0x14 => requests::Delete,"
                    .parse()
                    .unwrap()
            )
            .is_ok()
        );
    }
}
