//! Typed signatures for the verified index searches and their proof contracts.

use {
    crate::meta::{compact, function_signature, is_plain_sync, parameter_ident, plain_path},
    moxy::{
        ast::ParseError,
        token::{Span, Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

fn sections(input: TokenStream, count: usize) -> Result<Vec<TokenStream>, ParseError> {
    let mut sections = vec![TokenStream::new()];
    for token in input {
        if token.is_punct_semi() {
            sections.push(TokenStream::new());
        } else {
            sections.last_mut().expect("one section").extend([token]);
        }
    }
    if sections.last().is_some_and(TokenStream::is_empty) {
        sections.pop();
    }
    if sections.len() != count || sections.iter().any(TokenStream::is_empty) {
        return Err(ParseError::new(
            Span::call_site(),
            "expected a signature; comparison; optional cursor;",
        ));
    }
    Ok(sections)
}

fn check_header(header: &TokenStream, expected: &[&str]) -> Result<(), ParseError> {
    let signature = function_signature(&moxy::template! { {{header}} {} })?;
    if !is_plain_sync(&signature) || signature.params.inner.inputs.len() != expected.len() {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected a synchronous concrete kernel signature",
        ));
    }
    for (parameter, expected) in signature.params.inner.inputs.iter().zip(expected) {
        if parameter_ident(parameter, "expected named kernel parameters")?.text() != *expected {
            return Err(ParseError::new(
                parameter.span(),
                format!("expected parameter `{expected}`"),
            ));
        }
    }
    Ok(())
}

fn comparison(input: &TokenStream) -> Result<(), ParseError> {
    if !matches!(compact(input).as_str(), "<" | "<=") {
        return Err(ParseError::new(input.span(), "expected `<` or `<=`"));
    }
    Ok(())
}

pub(crate) fn cursor(input: TokenStream) -> Result<TokenStream, ParseError> {
    let parts = sections(input, 2)?;
    let header = &parts[0];
    let before = &parts[1];
    check_header(header, &["entries", "target"])?;
    comparison(before)?;
    let view: TokenStream = "@".parse().expect("Creusot view operator");
    Ok(moxy::template! {
        #[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries{{view}}.len()
            ==> entries{{view}}[i].key{{view}} <= entries{{view}}[j].key{{view}})]
        #[ensures(result{{view}} <= entries{{view}}.len())]
        #[ensures(forall<i: Int> 0 <= i && i < result{{view}} ==> entries{{view}}[i].key{{view}} {{before}} target{{view}})]
        #[ensures(forall<i: Int> result{{view}} <= i && i < entries{{view}}.len()
            ==> !(entries{{view}}[i].key{{view}} {{before}} target{{view}}))]
        #[must_use]
        #[inline]
        {{header}} {
            let mut lo = 0usize;
            let mut hi = entries.len();
            #[invariant(lo{{view}} <= hi{{view}} && hi{{view}} <= entries{{view}}.len())]
            #[invariant(forall<i: Int> 0 <= i && i < lo{{view}} ==> entries{{view}}[i].key{{view}} {{before}} target{{view}})]
            #[invariant(forall<i: Int> hi{{view}} <= i && i < entries{{view}}.len()
                ==> !(entries{{view}}[i].key{{view}} {{before}} target{{view}}))]
            #[variant(hi - lo)]
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if entries[mid].key {{before}} target { lo = mid + 1; } else { hi = mid; }
            }
            lo
        }
    })
}

pub(crate) fn floor(input: TokenStream) -> Result<TokenStream, ParseError> {
    let parts = sections(input, 3)?;
    let header = &parts[0];
    let order = &parts[1];
    check_header(header, &["entries", "target"])?;
    comparison(order)?;
    let cursor = plain_path(&parts[2], parts[2].span(), "expected cursor function path")?;
    let view: TokenStream = "@".parse().expect("Creusot view operator");
    Ok(moxy::template! {
        #[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries{{view}}.len()
            ==> entries{{view}}[i].key{{view}} {{order}} entries{{view}}[j].key{{view}})]
        #[ensures((exists<i: Int> 0 <= i && i < entries{{view}}.len() && entries{{view}}[i].key{{view}} <= target{{view}})
            ==> exists<i: Int> 0 <= i && i < entries{{view}}.len()
                && entries{{view}}[i].key{{view}} <= target{{view}} && result{{view}} == entries{{view}}[i].coordinate{{view}}
                && (forall<j: Int> i < j && j < entries{{view}}.len() ==> entries{{view}}[j].key{{view}} > target{{view}}))]
        #[ensures((forall<i: Int> 0 <= i && i < entries{{view}}.len() ==> entries{{view}}[i].key{{view}} > target{{view}}) ==> result{{view}} == 0)]
        #[must_use]
        {{header}} {
            let lo = {{cursor}}(entries, target);
            if lo == 0 { 0 } else { entries[lo - 1].coordinate }
        }
    })
}

pub(crate) fn finished_contract(input: &TokenStream) -> Result<TokenStream, ParseError> {
    if !matches!(compact(input).as_str(), "latest" | "true" | "false") {
        return Err(ParseError::new(
            input.span(),
            "expected latest, true or false",
        ));
    }
    let view: TokenStream = "@".parse().expect("Creusot view operator");
    Ok(moxy::template! {
        #[requires(starts{{view}}.len() == ends{{view}}.len() && starts{{view}}.len() == finished{{view}}.len())]
        #[ensures(finished_selection(starts{{view}}, ends{{view}}, finished{{view}}, result, starts{{view}}.len(), {{input}}))]
    })
}

pub(crate) fn finished(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let meta: TokenStream = meta.into();
    let signature = function_signature(&TokenStream::from(item.clone()))?;
    let expected: &[&str] = if compact(&meta) == "latest" {
        &["starts", "ends", "finished", "latest"]
    } else {
        &["starts", "ends", "finished"]
    };
    if !is_plain_sync(&signature) || signature.params.inner.inputs.len() != expected.len() {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected parallel array selection signature",
        ));
    }
    for (parameter, expected) in signature.params.inner.inputs.iter().zip(expected) {
        if parameter_ident(parameter, "expected named selection parameter")?.text() != *expected {
            return Err(ParseError::new(
                parameter.span(),
                format!("expected parameter `{expected}`"),
            ));
        }
    }
    let mut output: NativeTokenStream = finished_contract(&meta)?.into();
    output.extend(item);
    Ok(output)
}

pub(crate) fn site_contract(input: &TokenStream) -> Result<TokenStream, ParseError> {
    if !matches!(compact(input).as_str(), "0" | "1") {
        return Err(ParseError::new(
            input.span(),
            "expected minimum replica count 0 or 1",
        ));
    }
    Ok(moxy::template! {
        // cargo-mutants: generated Creusot spec function; not compiled outside Creusot, so no test can tell.
        #[cfg(creusot)]
        #[cfg_attr(test, mutants::skip)]
        #[logic]
        #[requires(rf >= {{input}} && sites >= 1 && 0 <= site && site < sites)]
    })
}

pub(crate) fn site(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let signature = function_signature(&TokenStream::from(item.clone()))?;
    if !is_plain_sync(&signature) || signature.params.inner.inputs.len() != 3 {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected site-load lemma signature",
        ));
    }
    for (parameter, expected) in signature
        .params
        .inner
        .inputs
        .iter()
        .zip(["rf", "sites", "site"])
    {
        if parameter_ident(parameter, "expected named lemma parameter")?.text() != expected {
            return Err(ParseError::new(
                parameter.span(),
                format!("expected parameter `{expected}`"),
            ));
        }
    }
    let mut output: NativeTokenStream = site_contract(&meta.into())?.into();
    output.extend(item);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[yare::parameterized(
        strict_predecessor = { "<" },
        inclusive_predecessor = { "<=" },
    )]
    fn cursor_policies_preserve_named_entry_contracts_and_comparison(before: &str) {
        let source = format!(
            "pub(crate) fn search(entries: &[IndexEntry<u32, u32>], target: u32) -> usize; {before};"
        );
        let generated = compact(&cursor(source.parse().unwrap()).unwrap());
        assert2::assert!(generated.contains(&format!("entries[mid].key{before}target")));
        assert2::assert!(
            generated.contains("#[variant(hi-lo)]")
                && generated.contains("result@<=entries@.len()")
        );
    }

    #[yare::parameterized(
        wrong_slice_name = { "fn bad(items: &[Row], target: u32) -> usize; <;" },
        async_cursor = { "async fn bad(entries: &[Row], target: u32) -> usize; <;" },
        unsupported_comparison = { "fn bad(entries: &[Row], target: u32) -> usize; >;" },
        missing_semicolon = { "fn missing();" },
    )]
    fn rejects_invalid_cursor_signatures(source: &str) {
        assert2::assert!(cursor(source.parse().unwrap()).is_err());
    }

    #[test]
    fn floor_keeps_the_last_predecessor_and_zero_fallback() {
        let source = "pub fn lookup(entries: &[Row], target: i64) -> u32; <=; upper;";
        let generated = compact(&floor(source.parse().unwrap()).unwrap());
        assert!(
            generated.contains("letlo=upper(entries,target);")
                && generated.contains("iflo==0{0}else{entries[lo-1].coordinate}")
        );
        assert!(
            floor(
                "fn bad(entries: &[Row], target: i64) -> u32; <=; upper<u32>;"
                    .parse()
                    .unwrap()
            )
            .is_err()
        );
    }

    #[yare::parameterized(
        latest_selection = { "latest" },
        prefer_true = { "true" },
        prefer_false = { "false" },
    )]
    fn selection_contract_accepts_concrete_policies(choice: &str) {
        assert2::assert!(
            compact(&finished_contract(&choice.parse().unwrap()).unwrap())
                .contains(&format!("starts@.len(),{choice}"))
        );
        assert2::assert!(finished_contract(&"earliest".parse().unwrap()).is_err());
    }

    #[yare::parameterized(
        minimum_replication_zero = { "0" },
        minimum_replication_one = { "1" },
    )]
    fn site_contract_accepts_supported_replication_minimums(minimum: &str) {
        assert2::assert!(
            compact(&site_contract(&minimum.parse().unwrap()).unwrap())
                .contains(&format!("rf>={minimum}&&sites>=1"))
        );
        assert2::assert!(site_contract(&"2".parse().unwrap()).is_err());
    }
}
