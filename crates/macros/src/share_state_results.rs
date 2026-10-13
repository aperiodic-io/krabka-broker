//! Concurrent topic and partition operations with an untouched native per-row body.

use {
    crate::{
        meta::finish_braced_body,
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token},
        token::{Ident, Span, TokenStream},
    },
    proc_macro::{TokenStream as NativeTokenStream, TokenTree},
};

fn scaffold(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let request = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let result = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    <Token![|]>::parse(&parser)?;
    let group = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let topic_id = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let partition = Ident::parse(&parser)?;
    <Token![|]>::parse(&parser)?;
    finish_braced_body(
        &parser,
        "expected one final share-state partition operation",
    )?;
    let local = |name| Ident::new(name).with_span(Span::mixed_site());
    let owned = local("request");
    let topic = local("topic");
    let partitions = local("partitions");
    Ok(moxy::template! {
        {
            let {{ owned }} = {{ request }};
            let {{ group }} = {{ owned }}.group_id.as_str();
            futures_util::future::join_all({{ owned }}.topics.into_iter().map(|{{ topic }}| async move {
                let {{ topic_id }} = uuid::Uuid::from_bytes({{ topic }}.topic_id.0);
                let {{ partitions }} = futures_util::future::join_all(
                    {{ topic }}.partitions.into_iter().map(|{{ partition }}| async move __share_operation)
                ).await;
                {{ result }} { topic_id: {{ topic }}.topic_id, partitions: {{ partitions }}, ..Default::default() }
            })).await
        }
    })
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let generated = scaffold(&input.clone().into())?;
    let Some(TokenTree::Group(operation)) = input.into_iter().last() else {
        unreachable!("validated partition operation")
    };
    Ok(splice(
        generated.into(),
        &[SpliceValue {
            name: "__share_operation".into(),
            group: operation,
        }],
        &[],
    ))
}

#[cfg(test)]
mod tests {
    use {super::scaffold, crate::meta::compact, assert2::assert};

    #[test]
    fn preserves_both_join_levels_and_rejects_malformed_bindings() {
        let output = scaffold(&"req, Result, |g, t, p| {}".parse().unwrap()).unwrap();
        assert!(
            compact(&output)
                .matches("futures_util::future::join_all")
                .count()
                == 2
        );
        for source in [
            "",
            "req, Result, |g, t| {}",
            "req, Result, |g, t, p| ()",
            "req, Result, |g, t, p| {} extra",
        ] {
            assert!(scaffold(&source.parse().unwrap()).is_err());
        }
    }
}
