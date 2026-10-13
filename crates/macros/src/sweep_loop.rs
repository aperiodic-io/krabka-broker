//! One cadence loop with native expressions and caller-owned sweep/shutdown blocks.

use {
    moxy::{
        ast::{Parse, ParseError, Parser, Token},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
};

const CLAUSES: [(&str, bool); 6] = [
    ("timer", false),
    ("cadence", false),
    ("shutdown", false),
    ("task", false),
    ("sweep", true),
    ("on_shutdown", true),
];

fn validate(input: &TokenStream) -> Result<(), ParseError> {
    let parser = Parser::from_tokens(input);
    for (expected, block) in CLAUSES {
        let name = Ident::parse(&parser)?;
        if name.text() != expected {
            return Err(ParseError::new(
                name.span(),
                format!("expected {expected} clause"),
            ));
        }
        let token = parser
            .advance()
            .ok_or_else(|| parser.error("expected grouped clause value"))?;
        let group = token
            .as_group()
            .ok_or_else(|| ParseError::new(token.span(), "expected grouped clause value"))?;
        if (block && !group.delim.is_brace())
            || (!block && (!group.delim.is_bracket() || group.tokens.is_empty()))
        {
            return Err(ParseError::new(
                group.span(),
                "expected a bracketed expression or a braced operation block",
            ));
        }
        let _: Token![;] = Parse::parse(&parser)?;
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected clause after on_shutdown"));
    }
    Ok(())
}

fn template() -> TokenStream {
    moxy::template! {
        {
            let timer = __sweep_timer;
            let cadence = __sweep_cadence;
            let shutdown = __sweep_shutdown;
            let task = __sweep_task;
            let Some(mut tick) = crate::time_util::arm(timer, cadence.0, task) else { return; };
            loop {
                ::tokio::select! {
                    outcome = &mut tick => {
                        if !crate::time_util::fired(outcome, task) { return; }
                        __sweep_body
                        let Some(next) = crate::time_util::arm(timer, cadence.1, task) else { return; };
                        tick = next;
                    }
                    () = shutdown.cancelled() => {
                        __sweep_on_shutdown
                        return;
                    }
                }
            }
        }
    }
}

/// Recurse only through generated scaffolding; inserted caller groups are opaque.
pub(crate) fn splice(
    scaffold: NativeTokenStream,
    values: &[(String, Group)],
    locals: &[&str],
) -> NativeTokenStream {
    scaffold
        .into_iter()
        .map(|token| match token {
            TokenTree::Ident(ref name)
                if values.iter().any(|(key, _)| *key == name.to_string()) =>
            {
                TokenTree::Group(
                    values
                        .iter()
                        .find(|(key, _)| *key == name.to_string())
                        .unwrap()
                        .1
                        .clone(),
                )
            }
            TokenTree::Group(group) => {
                let mut replacement =
                    Group::new(group.delimiter(), splice(group.stream(), values, locals));
                replacement.set_span(group.span());
                TokenTree::Group(replacement)
            }
            TokenTree::Ident(mut name) if locals.contains(&name.to_string().as_str()) => {
                name.set_span(proc_macro::Span::mixed_site());
                TokenTree::Ident(name)
            }
            token => token,
        })
        .collect()
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    validate(&input.clone().into())?;
    let groups = input.into_iter().filter_map(|token| match token {
        TokenTree::Group(group) => Some(group),
        _ => None,
    });
    let values: Vec<_> = [
        "__sweep_timer",
        "__sweep_cadence",
        "__sweep_shutdown",
        "__sweep_task",
        "__sweep_body",
        "__sweep_on_shutdown",
    ]
    .into_iter()
    .zip(groups)
    .map(|(name, group)| {
        let value = if group.delimiter() == Delimiter::Bracket {
            let mut value = Group::new(Delimiter::None, group.stream());
            value.set_span(group.span());
            value
        } else {
            group
        };
        (name.to_owned(), value)
    })
    .collect();
    Ok(splice(template().into(), &values, &[
        "timer", "cadence", "shutdown", "task", "tick", "outcome", "next",
    ]))
}

#[cfg(test)]
mod tests {
    use {
        super::{template, validate},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn validates_every_clause_without_parsing_native_operations() {
        let valid = "timer [timer::<1, 2>()]; cadence [(first(), later())]; shutdown [&shutdown]; task [TASK]; sweep { let image = cfg.current_image(); tick(image).await; } ; on_shutdown {};";
        assert!(validate(&valid.parse().unwrap()).is_ok());
        let output = compact(&template());
        assert!(output.contains("arm(timer,cadence.0,task)"));
        assert!(output.contains("arm(timer,cadence.1,task)"));
        assert!(output.contains("if!crate::time_util::fired(outcome,task){return;}"));
        for invalid in [
            "",
            "timer",
            "timer wrong;",
            "timer [];",
            "timer {};",
            "cadence [timer];",
            "timer [timer]",
            "timer [timer]; cadence [(0, 1)]; shutdown [&shutdown]; task [TASK]; sweep []; on_shutdown {};",
            "timer [timer]; cadence [(0, 1)]; shutdown [&shutdown]; task [TASK]; sweep {}; on_shutdown {}; extra",
        ] {
            assert!(validate(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
