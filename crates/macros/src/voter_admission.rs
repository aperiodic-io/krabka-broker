//! One admission step before an ordinary voter handler's operation body.

use {
    crate::meta::{Tokens, compact, edit_native_body, item_body},
    moxy::{
        ast::{FnParam, FromMeta, ItemFn, Meta, ParseError},
        token::{TokenStream, TokenTree},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

#[derive(moxy::FromMeta)]
struct Admission {
    request: Tokens,
    api: Tokens,
    gate: Tokens,
    refusals: Tokens,
}

fn prefix(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    let options: Meta = moxy::parse!({
        moxy::template! { voter_admission({{ meta }}) }
    })?;
    let Admission {
        request,
        api,
        gate,
        refusals,
    } = Admission::from_meta(&options)?;
    let (mut header, body) = item_body(item.clone(), "voter_admission", TokenTree::is_keyword_fn)?;
    if body + 1 != header.len() {
        return Err(ParseError::new(
            item.span(),
            "expected one async voter handler",
        ));
    }
    let TokenTree::Group(group) = &mut header[body] else {
        unreachable!()
    };
    group.tokens = TokenStream::new();
    let function: ItemFn = moxy::parse!({ TokenStream::from(header) })?;
    let inputs = &function.sig.params.inner.inputs;
    if function.sig.ident != "handle"
        || function.sig.asyncness.is_none()
        || inputs.len() != 4
        || !inputs
            .iter()
            .zip(["broker", "version", "req_bytes", "ctx"])
            .all(|(input, name)| {
                matches!(input, FnParam::Typed(parameter)
                if compact(&moxy::template! { {{ parameter.pat }} }) == name)
            })
    {
        return Err(ParseError::new(
            function.sig.ident.span(),
            "expected async fn handle(broker: ..., version: ..., req_bytes: ..., ctx: ...)",
        ));
    }
    let (request, api, gate, refusals) = (request.0, api.0, gate.0, refusals.0);
    Ok(moxy::template! {
        let crate::handlers::raft_voter::Admitted { req, image, quorum } =
            match crate::handlers::raft_voter::prelude::<{{ request }}, _>(
                broker, version, req_bytes, ctx, {{ api }}, {{ gate }}, {{ refusals }},
            ).await? {
                ::std::ops::ControlFlow::Break(answer) => return Ok(answer),
                ::std::ops::ControlFlow::Continue(admitted) => admitted,
            };
    })
}

pub(crate) fn expand(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let parsed = TokenStream::from(item.clone());
    let admission: NativeTokenStream = prefix(&meta.into(), &parsed)?.into();
    Ok(edit_native_body(item, |body| {
        admission.into_iter().chain(body).collect()
    }))
}

#[cfg(test)]
mod tests {
    use {super::prefix, assert2::assert};

    #[test]
    fn rejects_missing_options_and_incorrect_handler_shapes() {
        let meta = "request = Request, api = 80, gate = gate, refusals = refusals"
            .parse()
            .unwrap();
        let valid = "async fn handle(broker: &Broker, version: i16, req_bytes: &[u8], ctx: &Context) -> Result<Bytes, Error> {}".parse().unwrap();
        assert!(
            prefix(
                &"request = Request, api = 80, gate = gate".parse().unwrap(),
                &valid
            )
            .is_err()
        );
        assert!(prefix(&meta, &valid).is_ok());
        for invalid in [
            "fn handle(broker: &Broker, version: i16, req_bytes: &[u8], ctx: &Context) {}",
            "async fn handle(broker: &Broker) {}",
            "async fn handle(broker: &Broker, version: i16, body: &[u8], ctx: &Context) {}",
            "async fn other(broker: &Broker, version: i16, req_bytes: &[u8], ctx: &Context) {}",
            "struct Config {}",
            "async fn handle(broker: &Broker, version: i16, req_bytes: &[u8], ctx: &Context) {} fn extra() {}",
        ] {
            assert!(prefix(&meta, &invalid.parse().unwrap()).is_err());
        }
    }
}
