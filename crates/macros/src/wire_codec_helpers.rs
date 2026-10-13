//! Flat declarations for test request encoders, response decoders and contexts.

use {
    crate::meta::edit_native_body,
    moxy::{
        ast::{Parse, ParseError, Parser, Token, Type, Visibility},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{
        Delimiter, Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree,
    },
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Encode,
    Decode,
    Context,
    Version,
}

struct Declaration {
    kind: Kind,
    visibility: Visibility,
    ty: Option<Type>,
}

fn declarations(input: &TokenStream) -> Result<Vec<Declaration>, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut declarations = Vec::new();
    while !parser.is_empty() {
        let visibility = Visibility::parse(&parser)?;
        let name = Ident::parse(&parser)?;
        let kind = match name.text() {
            "encode_request" => Kind::Encode,
            "decode_response" => Kind::Decode,
            "test_context" => Kind::Context,
            "version" => Kind::Version,
            _ => {
                return Err(ParseError::new(
                    name.span(),
                    "expected encode_request, decode_response, test_context or version",
                ));
            }
        };
        if declarations
            .iter()
            .any(|declaration: &Declaration| declaration.kind == kind)
        {
            return Err(ParseError::new(
                name.span(),
                "duplicate wire helper declaration",
            ));
        }
        let Some(token) = parser.advance() else {
            return Err(parser.error("expected grouped helper type or expression"));
        };
        let Some(group) = token.as_group() else {
            return Err(ParseError::new(
                token.span(),
                "expected grouped helper type or expression",
            ));
        };
        let typed = matches!(kind, Kind::Encode | Kind::Decode);
        if (typed && !group.delim.is_paren())
            || (!typed && !group.delim.is_bracket())
            || group.tokens.is_empty()
        {
            return Err(ParseError::new(
                group.span(),
                "expected a nonempty parenthesized codec type or bracketed expression",
            ));
        }
        if kind == Kind::Version && !matches!(visibility, Visibility::Inherited) {
            return Err(ParseError::new(
                name.span(),
                "version metadata has no visibility",
            ));
        }
        let ty = if typed {
            Some(moxy::parse!({ group.tokens.clone() } as Type)?)
        } else {
            None
        };
        let _: Token![;] = Parse::parse(&parser)?;
        declarations.push(Declaration {
            kind,
            visibility,
            ty,
        });
    }
    if declarations.is_empty()
        || declarations
            .iter()
            .all(|declaration| declaration.kind == Kind::Version)
    {
        return Err(ParseError::new(
            input.span(),
            "declare at least one wire helper",
        ));
    }
    if declarations
        .iter()
        .any(|declaration| declaration.kind == Kind::Version)
        && !declarations
            .iter()
            .any(|declaration| matches!(declaration.kind, Kind::Encode | Kind::Decode))
    {
        return Err(ParseError::new(
            input.span(),
            "a pinned version needs an encoder or decoder",
        ));
    }
    Ok(declarations)
}

fn function(declaration: &Declaration, pinned: bool) -> TokenStream {
    let visibility = &declaration.visibility;
    let ty = &declaration.ty;
    match declaration.kind {
        Kind::Encode => moxy::template! {
            {{ visibility }} fn encode_request(req: &{{ ty }} @if !pinned { , version: i16 }) -> ::bytes::Bytes {
                let __wire_codec_version = __wire_codec_value;
                crate::test_support::encode_request(req, __wire_codec_version)
            }
        },
        Kind::Decode => moxy::template! {
            {{ visibility }} fn decode_response(bytes: &::bytes::Bytes @if !pinned { , version: i16 }) -> {{ ty }} {
                let __wire_codec_version = __wire_codec_value;
                crate::test_support::decode_response(bytes, __wire_codec_version)
            }
        },
        Kind::Context => moxy::template! {
            {{ visibility }} fn test_context<'a>(principal: &'a ::krabka_security::Principal, peer: &'a ::std::net::SocketAddr) -> crate::handlers::RequestContext<'a> {
                let __wire_codec_client = __wire_codec_value;
                crate::test_support::request_context(principal, peer, __wire_codec_client)
            }
        },
        Kind::Version => unreachable!("version metadata is not a helper"),
    }
}

/// Each declaration has one native value group; visibility's groups precede its fixed name.
fn native_values(input: NativeTokenStream) -> Vec<(String, NativeTokenStream)> {
    let mut tokens = input.into_iter();
    let mut values = Vec::new();
    while let Some(token) = tokens.next() {
        if let NativeTokenTree::Ident(name) = token
            && matches!(
                name.to_string().as_str(),
                "encode_request" | "decode_response" | "test_context" | "version"
            )
        {
            let Some(NativeTokenTree::Group(group)) = tokens.next() else {
                unreachable!("validated helper value")
            };
            values.push((name.to_string(), group.stream()));
        }
    }
    values
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let declarations = declarations(&input.clone().into())?;
    let values = native_values(input);
    let version = values
        .iter()
        .find(|(name, _)| name == "version")
        .map(|(_, value)| value);
    let mut output = NativeTokenStream::new();
    for declaration in declarations
        .iter()
        .filter(|declaration| declaration.kind != Kind::Version)
    {
        let value = if declaration.kind == Kind::Context {
            values
                .iter()
                .find(|(name, _)| name == "test_context")
                .expect("validated context value")
                .1
                .clone()
        } else {
            version
                .cloned()
                .unwrap_or_else(|| moxy::template! { version }.into())
        };
        let item: NativeTokenStream = function(declaration, version.is_some()).into();
        output.extend(edit_native_body(item, |body| {
            body.into_iter().flat_map(|token| {
                if matches!(&token, NativeTokenTree::Ident(name) if name.to_string() == "__wire_codec_value") {
                    vec![NativeTokenTree::Group(Group::new(Delimiter::Parenthesis, value.clone()))]
                } else { vec![token] }
            }).collect()
        }));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{Kind, declarations, function},
        assert2::assert,
        moxy::token::TokenStream,
    };

    #[test]
    fn parses_flat_typed_helpers_and_both_version_signatures() {
        let declarations = declarations(&"pub(super) encode_request(Request); decode_response(Response); test_context [opaque::<1, 2>()]; version [chosen_version()];".parse().unwrap()).unwrap();
        assert!(declarations.len() == 4);
        for declaration in declarations
            .iter()
            .filter(|declaration| declaration.kind != Kind::Version)
        {
            for pinned in [false, true] {
                let body = function(declaration, pinned).to_string();
                if declaration.kind != Kind::Context {
                    assert!(body.contains("version : i16") == !pinned);
                }
                assert!(body.contains("__wire_codec_value"));
            }
        }
        for invalid in [
            "",
            "version [1];",
            "test_context [\"client\"]; version [1];",
            "unknown [1];",
            "decode_response();",
            "decode_response [Response];",
            "decode_response(Response); decode_response(Response);",
            "test_context [];",
            "test_context(\"client\");",
            "decode_response(Response)",
            "pub version [1]; encode_request(Request);",
            "encode_request(Request) extra;",
        ] {
            assert!(
                super::declarations(&invalid.parse::<TokenStream>().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }
}
