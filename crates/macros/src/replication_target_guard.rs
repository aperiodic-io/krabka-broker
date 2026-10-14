//! Hold a replication target through the caller's mutation, refusing stale rows locally.

use moxy::{
    ast::{Parse, ParseError, Parser, Token},
    token::{Ident, LitStr, Span, TokenStream},
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let partition = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let config = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let message = LitStr::parse(&parser)?;
    if !parser.is_empty() {
        return Err(
            parser.error("expected partition and configuration bindings followed by one message")
        );
    }
    let guard = Ident::new("target_guard").with_span(Span::mixed_site());
    let error = Ident::new("target_error").with_span(Span::mixed_site());
    Ok(moxy::template! {
        {
            use crate::replicator::{task_replication_target, response::RowAction};
            match {{ partition }}.lock_replication_target(task_replication_target({{ config }})).await {
                Ok({{ guard }}) => {{ guard }},
                Err({{ error }}) => {
                    ::tracing::warn!(topic = %{{ config }}.topic, partition = {{ config }}.partition.get(), error = %{{ error }}, {{ message }});
                    return RowAction::Drop;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[yare::parameterized(
        missing_message = { "part, cfg" },
        borrowed_part = { "&part, cfg, \"message\"" },
        computed_message = { "part, cfg, compute()" },
        extra_argument = { "part, cfg, \"message\", extra" },
    )]
    fn guard_rejects_invalid_arguments(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
    }

    #[test]
    fn guard_keeps_native_caller_return_and_the_callsite_message() {
        let generated = expand(&"part, cfg, \"stale target\"".parse().unwrap()).unwrap();
        let output = compact(&generated);
        assert!(
            output.contains("part.lock_replication_target(task_replication_target(cfg)).await")
        );
        assert!(output.contains("returnRowAction::Drop"));
        assert!(generated.to_string().contains("\"stale target\""));
    }
}
