//! Supply the fixed decision to an explicitly declared topic-filter function.

use {
    crate::meta::{edit_native_body, function_signature, mode, parameter_ident},
    moxy::{
        ast::{GenericParam, ParseError},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

fn body(item: &TokenStream, decision: &str) -> Result<TokenStream, ParseError> {
    let function = function_signature(item)?;
    if function.asyncness.is_some()
        || function.constness.is_some()
        || function.safety.is_some()
        || function.abi.is_some()
        || function
            .generics
            .params
            .iter()
            .any(|param| !matches!(param, GenericParam::Lifetime(_)))
        || function.params.inner.inputs.len() != 5
        || !item
            .as_ref()
            .last()
            .and_then(|token| token.as_group())
            .is_some_and(|group| group.tokens.is_empty())
    {
        return Err(ParseError::new(
            function.span(),
            "expected an empty synchronous topic-filter function with five parameters",
        ));
    }
    let names = function
        .params
        .inner
        .inputs
        .iter()
        .map(|parameter| {
            parameter_ident(parameter, "topic-filter parameters must be identifiers").cloned()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let decision = Ident::new(decision);
    Ok(moxy::template! {
        topics_decided(@for name in &names { {{ name }}, } AuthorizationResult::{{ decision }})
    })
}

pub(crate) fn expand(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let decision = mode(meta, ["Allow", "Deny"])?;
    let generated = body(&item.clone().into(), decision)?;
    Ok(edit_native_body(item, |_| generated.into()))
}

#[cfg(test)]
mod tests {
    use {super::body, crate::meta::compact, assert2::assert};

    #[test]
    fn preserves_parameter_order_and_selects_the_requested_decision() {
        let item = "fn filter<'a>(auth: &A, image: &I, context: &C, operation: O, topics: impl IntoIterator<Item = &'a str>) -> Set {}".parse().unwrap();
        for decision in ["Allow", "Deny"] {
            assert!(
                compact(&body(&item, decision).unwrap())
                    == format!(
                        "topics_decided(auth,image,context,operation,topics,AuthorizationResult::{decision})"
                    )
            );
        }
    }

    #[test]
    fn rejects_operation_bodies_wrong_arity_and_destructured_parameters() {
        for item in [
            "fn filter(a: A, b: B, c: C, d: D, e: E) -> Set { todo!() }",
            "async fn filter(a: A, b: B, c: C, d: D, e: E) -> Set {}",
            "fn filter(a: A) -> Set {}",
            "fn filter<T>(a: A, b: B, c: C, d: D, e: E) -> Set {}",
            "fn filter((a, b): A, c: C, d: D, e: E, f: F) -> Set {}",
        ] {
            assert!(body(&item.parse().unwrap(), "Deny").is_err(), "{item}");
        }
    }
}
