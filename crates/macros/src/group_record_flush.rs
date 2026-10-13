//! Append an actor delta before exposing its native cache update.

use {
    crate::{
        meta::{
            edit_native_body, expect_name, function_signature, grouped_value, is_plain_async,
            native_groups, parameter_ident,
        },
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{Parse, ParseError, Parser, Signature, Token},
        token::{Ident, Span, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

fn scaffold(metadata: &TokenStream, signature: &Signature) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(metadata);
    for label in ["group", "encode"] {
        expect_name(&parser, label, "expected group and encode clauses")?;
        let expression = grouped_value(&parser, "expected bracketed record expression")?;
        if !expression.delim.is_bracket() || expression.tokens.is_empty() {
            return Err(ParseError::new(
                expression.span(),
                "expected nonempty bracketed record expression",
            ));
        }
        <Token![;]>::parse(&parser)?;
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected record flush metadata"));
    }
    let inputs = &signature.params.inner.inputs;
    if !is_plain_async(signature)
        || inputs.len() != 5
        || signature.generics.where_clause.is_some()
        || signature.params.inner.variadic.is_some()
    {
        return Err(ParseError::new(
            signature.span(),
            "expected a concrete async flush with five named parameters",
        ));
    }
    for input in inputs {
        parameter_ident(input, "expected a named flush parameter")?;
    }
    let pending = parameter_ident(&inputs[1], "expected pending records")?;
    let log = parameter_ident(&inputs[2], "expected offsets log")?;
    let batch = Ident::new("record_batch").with_span(Span::mixed_site());
    Ok(moxy::template! {
        if {{ pending }}.is_empty() { return Ok(()); }
        let {{ batch }} = __flush_encode?;
        {{ log }}.append(__flush_group, {{ batch }}).await?;
        __flush_cache;
        Ok(())
    })
}

pub(crate) fn expand(
    metadata: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let signature = function_signature(&item.clone().into())?;
    let generated = scaffold(&metadata.clone().into(), &signature)?;
    let mut expressions = native_groups(metadata);
    let mut values = Vec::new();
    for name in ["__flush_group", "__flush_encode"] {
        let expression = expressions.next().expect("validated record expression");
        let mut group = Group::new(Delimiter::None, expression.stream());
        group.set_span(expression.span());
        values.push(SpliceValue {
            name: name.to_owned(),
            group,
        });
    }
    Ok(edit_native_body(item, |cache| {
        values.push(SpliceValue {
            name: "__flush_cache".to_owned(),
            group: Group::new(Delimiter::Brace, cache),
        });
        splice(generated.into(), &values, &[])
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::scaffold,
        crate::meta::{compact, function_signature},
        assert2::assert,
    };

    #[test]
    fn record_boundary_validates_its_signature_and_opaque_encoder_clauses() {
        let signature = function_signature(&"pub(super) async fn flush(state: &State, delta: Delta, log: &Log, coordinator: &Coordinator, now: i64) -> Result<(), Error> {}".parse().unwrap()).unwrap();
        let metadata = "group [&state.id]; encode [native!(delta, |&Row { value }| value)];"
            .parse()
            .unwrap();
        let output = compact(&scaffold(&metadata, &signature).unwrap());
        assert!(output.contains("ifdelta.is_empty(){returnOk(());}"));
        assert!(output.find("log.append").unwrap() < output.find("__flush_cache").unwrap());
        for invalid in [
            "",
            "group []; encode [encode()];",
            "group [id]; encode {};",
            "group [id]; encode [encode()]; extra",
        ] {
            assert!(scaffold(&invalid.parse().unwrap(), &signature).is_err());
        }
        for invalid in [
            "fn flush(state: &State, delta: Delta, log: &Log, coordinator: &Coordinator, now: i64) {}",
            "async fn flush(state: &State) {}",
            "async fn flush(_: &State, delta: Delta, log: &Log, coordinator: &Coordinator, now: i64) {}",
        ] {
            let signature = function_signature(&invalid.parse().unwrap()).unwrap();
            assert!(scaffold(&metadata, &signature).is_err());
        }
    }
}
