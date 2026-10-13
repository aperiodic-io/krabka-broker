//! Share the archive walk while keeping Creusot's bindings in one expansion context.

use {
    crate::{fixtures::name, meta::arguments},
    moxy::{
        ast::ParseError,
        token::{Ident, Span, TokenStream},
    },
};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut sections = vec![TokenStream::new()];
    for token in input {
        if token.is_punct_semi() && sections.len() < 5 {
            sections.push(TokenStream::new());
        } else {
            sections.last_mut().expect("one section").extend([token]);
        }
    }
    let [signature, bindings, invariants, valid, after]: [TokenStream; 5] =
        sections.try_into().map_err(|_| {
            ParseError::new(
                Span::call_site(),
                "expected signature; bindings; invariants; validity; result body",
            )
        })?;
    let names = arguments(bindings, 5)?;
    let names = names
        .into_iter()
        .map(name)
        .collect::<Result<Vec<Ident>, _>>()?;
    let [entries, index, previous, first, second]: [Ident; 5] =
        names.try_into().expect("five bindings");
    if signature.is_empty() || valid.is_empty() || after.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "archive walk needs a signature, validity expression and result body",
        ));
    }
    let view: TokenStream = "@".parse().expect("Creusot view operator");
    Ok(moxy::template! {
        {{ signature }} {
            let mut {{ index }} = 0usize;
            #[invariant({{ index }}{{ view }} <= {{ entries }}{{ view }}.len())]
            {{ invariants }}
            #[variant({{ entries }}{{ view }}.len() - {{ index }}{{ view }})]
            while {{ index }} < {{ entries }}.len() {
                let {{ previous }} = if {{ index }} == 0 { None } else { Some({{ entries }}[{{ index }} - 1]) };
                let ({{ first }}, {{ second }}) = {{ entries }}[{{ index }}];
                if !({{ valid }}) {
                    return Err(());
                }
                {{ index }} += 1;
            }
            {{ after }}
        }
    })
}
