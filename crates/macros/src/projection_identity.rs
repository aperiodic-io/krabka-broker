//! Equality, hashing and optional debug output through a model's canonical projection.

use moxy::{
    ast::ParseError,
    token::{Ident, Span, TokenStream, TokenTree},
};

struct Arguments {
    name: Ident,
    projection: Ident,
    debug: bool,
}

fn arguments(input: TokenStream) -> Result<Arguments, ParseError> {
    let tokens = Vec::from(input);
    let (name, projection, debug) = match tokens.as_slice() {
        [TokenTree::Ident(name), comma, TokenTree::Ident(projection)] if comma.is_punct_comma() => {
            (name, projection, false)
        }
        [
            TokenTree::Ident(name),
            comma,
            TokenTree::Ident(projection),
            next_comma,
            TokenTree::Ident(flag),
        ] if comma.is_punct_comma() && next_comma.is_punct_comma() && flag == "debug" => {
            (name, projection, true)
        }
        _ => {
            return Err(ParseError::new(
                Span::call_site(),
                "projection_identity needs `Type, projection_method` and optional `, debug`",
            ));
        }
    };
    Ok(Arguments {
        name: name.clone(),
        projection: projection.clone(),
        debug,
    })
}

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let Arguments {
        name,
        projection,
        debug,
    } = arguments(input)?;
    Ok(moxy::template! {
        impl ::std::cmp::PartialEq for {{ name }} {
            fn eq(&self, other: &Self) -> bool {
                self.{{ projection }}() == other.{{ projection }}()
            }
        }
        impl ::std::cmp::Eq for {{ name }} {}
        impl ::std::hash::Hash for {{ name }} {
            fn hash<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(&self.{{ projection }}(), state);
            }
        }
        @if debug {
            impl ::std::fmt::Debug for {{ name }} {
                fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    ::std::fmt::Debug::fmt(&self.{{ projection }}(), formatter)
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::arguments, assert2::assert};

    #[test]
    fn parses_the_projection_and_optional_debug_flag() {
        for (input, debug) in [
            ("State, canonical", false),
            ("State, canonical, debug", true),
        ] {
            let args = arguments(input.parse().unwrap()).unwrap();
            assert!(args.name == "State");
            assert!(args.projection == "canonical");
            assert!(args.debug == debug);
        }
    }

    #[test]
    fn rejects_incomplete_arguments_and_unknown_flags() {
        for input in [
            "",
            "State",
            "State canonical",
            "State, 3",
            "State, canonical, other",
        ] {
            assert!(arguments(input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
