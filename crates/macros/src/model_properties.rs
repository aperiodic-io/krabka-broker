//! Ordered model property rows with native oracle blocks.

use {
    crate::meta::native_braced_bodies,
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token},
        token::{Group, Ident, LitStr, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group as NativeGroup, TokenStream as NativeTokenStream, TokenTree},
};

struct Property {
    kind: Ident,
    label: LitStr,
    binding: Ident,
}

struct PropertyDeclaration {
    state: Path,
    properties: Vec<Property>,
}

fn declaration(input: &TokenStream) -> Result<PropertyDeclaration, ParseError> {
    let parser = Parser::from_tokens(input);
    let state = Path::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    let mut properties = Vec::new();
    while !parser.is_empty() {
        let kind = Ident::parse(&parser)?;
        if !matches!(kind.text(), "always" | "sometimes" | "eventually") {
            return Err(ParseError::new(
                kind.span(),
                "expected always, sometimes or eventually property",
            ));
        }
        let label = LitStr::parse(&parser)?;
        <Token![=>]>::parse(&parser)?;
        <Token![|]>::parse(&parser)?;
        let binding = Ident::parse(&parser)?;
        <Token![|]>::parse(&parser)?;
        let oracle = Group::parse(&parser)?;
        if !oracle.delim.is_brace() || oracle.tokens.is_empty() {
            return Err(ParseError::new(
                oracle.span(),
                "property oracle needs a nonempty native block",
            ));
        }
        properties.push(Property {
            kind,
            label,
            binding,
        });
        if !parser.is_empty() {
            <Token![,]>::parse(&parser)?;
        }
    }
    if properties.is_empty() {
        return Err(parser.error("declare at least one model property"));
    }
    Ok(PropertyDeclaration { state, properties })
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let PropertyDeclaration { state, properties } = declaration(&input.clone().into())?;
    let oracles = native_braced_bodies(input);
    if properties.len() != oracles.len() {
        return Err(ParseError::new(
            state.span(),
            "each property needs its own native oracle block",
        ));
    }
    let mut entries = NativeTokenStream::new();
    let mut properties = properties.into_iter();
    for oracle in oracles {
        let Property {
            kind,
            label,
            binding,
        } = properties.next().expect("validated native oracle count");
        let mut arguments: NativeTokenStream =
            moxy::template! { {{ label }}, |_, {{ binding }}: &{{ state }}| }.into();
        arguments.extend([TokenTree::Group(oracle)]);
        entries.extend(NativeTokenStream::from(
            moxy::template! { ::stateright::Property::{{ kind }} },
        ));
        entries.extend([TokenTree::Group(NativeGroup::new(
            Delimiter::Parenthesis,
            arguments,
        ))]);
        entries.extend(NativeTokenStream::from(moxy::template! { , }));
    }
    let mut output: NativeTokenStream = moxy::template! { vec! }.into();
    output.extend([TokenTree::Group(NativeGroup::new(
        Delimiter::Bracket,
        entries,
    ))]);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {super::declaration, assert2::assert};

    #[test]
    fn property_order_and_native_blocks_are_parsed_without_expression_ast() {
        let input = "State; always \"first\" => |state| { opaque!(@state state) }, sometimes \"second\" => |next| { other::call::<Type>(next) },".parse().unwrap();
        let rows = declaration(&input).unwrap().properties;
        assert!(
            rows.iter().map(|row| row.label.value()).collect::<Vec<_>>() == ["first", "second"]
        );
    }

    #[yare::parameterized(
        no_properties = { "State;" },
        unknown_kind = { "State; never \"x\" => |s| { true }" },
        extra_binding = { "State; always \"x\" => |s, other| { true }" },
        missing_block = { "State; always \"x\" => |s| true" },
        empty_block = { "State; always \"x\" => |s| {}" },
        trailing_tokens = { "State; always \"x\" => |s| { true } extra" },
    )]
    fn rejects_unknown_kinds_missing_oracles_and_extra_bindings(input: &str) {
        assert2::assert!(declaration(&input.parse().unwrap()).is_err(), "{input}");
    }
}
