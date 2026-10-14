//! One ordered WORM admission rule for runtime and proof declarations.

use {
    crate::meta::is_plain_sync,
    moxy::{
        ast::{Attributes, FnParam, Parse, ParseError, Parser, Pattern, Signature, Visibility},
        token::{Ident, Spanner, TokenStream, TokenTree},
    },
    proc_macro::{Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Model,
    Runtime,
}

fn validate(input: &TokenStream) -> Result<Mode, ParseError> {
    let tokens = input.clone().into_inner();
    let [
        TokenTree::Ident(mode),
        first_semi,
        TokenTree::Group(header),
        TokenTree::Group(facts),
        second_semi,
        TokenTree::Ident(count_name),
        TokenTree::Group(count),
        TokenTree::Ident(listed_name),
        TokenTree::Group(listed),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            input.span(),
            "expected Mode; { function header } (facts); count [tokens] listed [tokens]",
        ));
    };
    let mode = match mode.text() {
        "Model" => Mode::Model,
        "Runtime" => Mode::Runtime,
        _ => return Err(ParseError::new(mode.span(), "expected Model or Runtime")),
    };
    if !first_semi.is_punct_semi()
        || !second_semi.is_punct_semi()
        || !header.delim.is_brace()
        || !facts.delim.is_paren()
        || !count.delim.is_bracket()
        || !listed.delim.is_bracket()
        || count_name.text() != "count"
        || listed_name.text() != "listed"
        || count.tokens.is_empty()
        || listed.tokens.is_empty()
    {
        return Err(ParseError::new(
            input.span(),
            "expected Mode; { function header } (facts); count [tokens] listed [tokens]",
        ));
    }
    let fact_tokens = facts.tokens.clone().into_inner();
    let [TokenTree::Ident(facts)] = fact_tokens.as_slice() else {
        return Err(ParseError::new(
            facts.span(),
            "expected one facts identifier",
        ));
    };
    validate_header(&header.tokens, facts)?;
    Ok(mode)
}

fn validate_header(header: &TokenStream, facts: &Ident) -> Result<(), ParseError> {
    let parser = Parser::from_tokens(header);
    let _: Attributes = Parse::parse(&parser)?;
    let _: Visibility = Parse::parse(&parser)?;
    let signature = Signature::parse(&parser)?;
    let inputs = &signature.params.inner.inputs;
    if !parser.is_empty()
        || !is_plain_sync(&signature)
        || inputs.len() != 1
        || !matches!(inputs.first(), Some(FnParam::Typed(parameter))
            if matches!(parameter.pat.as_ref(), Pattern::Ident(name)
                if name.ident.text() == facts.text() && name.by_ref.is_none() && name.subpat.is_none()))
        || !signature
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.path.is_ident("WormObjectSetDecision"))
    {
        return Err(ParseError::new(
            header.span(),
            "expected a synchronous function header with one facts parameter returning WormObjectSetDecision",
        ));
    }
    Ok(())
}

fn rule(mode: Mode) -> TokenStream {
    let decisions = moxy::template! {
        if __worm_count == 0 {
            WormObjectSetDecision::Empty
        } else if !__worm_facts.identity.unique_keys {
            WormObjectSetDecision::DuplicateKey
        } else if !__worm_facts.identity.coordinates_match {
            WormObjectSetDecision::CoordinateMismatch
        } else if !__worm_facts.availability.all_present {
            WormObjectSetDecision::MissingObject
        } else if __worm_count != __worm_listed {
            WormObjectSetDecision::CountMismatch
        } else if !__worm_facts.availability.sizes_match {
            WormObjectSetDecision::SizeMismatch
        } else if __worm_facts.digests.require_digests && !__worm_facts.digests.digests_match {
            WormObjectSetDecision::DigestMismatch
        } else {
            WormObjectSetDecision::Admit
        }
    };
    if mode == Mode::Model {
        moxy::template! { pearlite! { {{ decisions }} } }
    } else {
        decisions
    }
}

fn fill(
    generated: NativeTokenStream,
    facts: &NativeTokenStream,
    count: &NativeTokenStream,
    listed: &NativeTokenStream,
) -> NativeTokenStream {
    let mut output = NativeTokenStream::new();
    for token in generated {
        match token {
            NativeTokenTree::Ident(name) => match name.to_string().as_str() {
                "__worm_facts" => output.extend(facts.clone()),
                "__worm_count" => output.extend(count.clone()),
                "__worm_listed" => output.extend(listed.clone()),
                _ => output.extend([NativeTokenTree::Ident(name)]),
            },
            NativeTokenTree::Group(group) => {
                let mut replacement = Group::new(
                    group.delimiter(),
                    fill(group.stream(), facts, count, listed),
                );
                replacement.set_span(group.span());
                output.extend([NativeTokenTree::Group(replacement)]);
            }
            other => output.extend([other]),
        }
    }
    output
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let mode = validate(&input.clone().into())?;
    let tokens: Vec<_> = input.into_iter().collect();
    let [
        _,
        _,
        NativeTokenTree::Group(header),
        NativeTokenTree::Group(facts),
        _,
        _,
        NativeTokenTree::Group(count),
        _,
        NativeTokenTree::Group(listed),
    ] = tokens.as_slice()
    else {
        unreachable!("validated kernel groups")
    };
    // Substitute only in generated tokens; caller count/listed groups remain native and opaque.
    let body = fill(
        rule(mode).into(),
        &facts.stream(),
        &count.stream(),
        &listed.stream(),
    );
    let mut output = header.stream();
    output.extend([NativeTokenTree::Group(Group::new(
        proc_macro::Delimiter::Brace,
        body,
    ))]);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{Mode, rule, validate},
        assert2::assert,
    };

    #[test]
    fn proof_observations_stay_opaque_while_the_header_is_checked() {
        let input = "Model; { #[cfg(creusot)] #[cfg_attr(test, mutants::skip)] #[logic] fn model(facts: Facts) -> WormObjectSetDecision } (facts); count [facts.object_count@] listed [facts.listed_count@]".parse().unwrap();
        assert!(validate(&input).unwrap() == Mode::Model);
        let runtime = "Runtime; { #[must_use] pub fn decide(observed: Facts) -> WormObjectSetDecision } (observed); count [{ next_count() }] listed [next_listed()]".parse().unwrap();
        assert!(validate(&runtime).unwrap() == Mode::Runtime);
    }

    #[yare::parameterized(
        model_mode = { Mode::Model },
        runtime_mode = { Mode::Runtime },
    )]
    fn every_mode_emits_the_same_diagnostic_order_without_hoisting_counts(mode: Mode) {
        let output = rule(mode).to_string();
        let names = [
            "Empty",
            "DuplicateKey",
            "CoordinateMismatch",
            "MissingObject",
            "CountMismatch",
            "SizeMismatch",
            "DigestMismatch",
            "Admit",
        ];
        let positions: Vec<_> = names
            .iter()
            .map(|name| output.find(name).unwrap())
            .collect();
        assert2::assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        assert2::assert!(output.matches("__worm_count").count() == 2);
        assert2::assert!(output.matches("__worm_listed").count() == 1);
        assert2::assert!(output.contains("pearlite") == (mode == Mode::Model));
        assert2::assert!(!output.contains("let "));
    }

    #[yare::parameterized(
        unknown_mode = {
            "Other; { fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        wrong_observed_binding = {
            "Runtime; { fn f(facts: Facts) -> WormObjectSetDecision } (other); count [1] listed [1]",
        },
        function_body_in_header = {
            "Runtime; { fn f(facts: Facts) -> WormObjectSetDecision {} } (facts); count [1] listed [1]",
        },
        async_header = {
            "Runtime; { async fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        const_header = {
            "Runtime; { const fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        unsafe_header = {
            "Runtime; { unsafe fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        extern_header = {
            "Runtime; { extern fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        generic_header = {
            "Runtime; { fn f<T>(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        extra_parameter = {
            "Runtime; { fn f(facts: Facts, extra: bool) -> WormObjectSetDecision } (facts); count [1] listed [1]",
        },
        wrong_return_type = {
            "Runtime; { fn f(facts: Facts) -> bool } (facts); count [1] listed [1]",
        },
        empty_count = {
            "Runtime; { fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [] listed [1]",
        },
        empty_listed_count = {
            "Runtime; { fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed []",
        },
        trailing_metadata = {
            "Runtime; { fn f(facts: Facts) -> WormObjectSetDecision } (facts); count [1] listed [1] extra",
        },
    )]
    fn refuses_unknown_modes_malformed_headers_and_missing_observations(input: &str) {
        assert2::assert!(validate(&input.parse().unwrap()).is_err(), "{input}");
    }
}
