//! Decode token requests around their visible native authentication/audit operation.

use {
    crate::{
        meta::{
            compact, edit_native_body, function_signature, named_async_parameter_types,
            no_arguments,
        },
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{FnParam, ParseError},
        token::{Spanner, TokenStream},
    },
    proc_macro::{
        Delimiter, Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree,
    },
};

struct Adapter {
    header: TokenStream,
    body: TokenStream,
}

fn adapter(item: &TokenStream) -> Result<Adapter, ParseError> {
    let signature = function_signature(item)?;
    let types = named_async_parameter_types(
        &signature,
        &["broker", "req", "auth", "peer"],
        "expected ordinary async token adapter(broker, req, auth, peer)",
    )?;
    let request = &types[1];
    if types[0] != "&Broker"
        || types[2] != "&ConnectionAuth"
        || types[3] != "&SocketAddr"
        || !matches!(
            request.as_str(),
            "CreateDelegationTokenRequest"
                | "RenewDelegationTokenRequest"
                | "ExpireDelegationTokenRequest"
                | "DescribeDelegationTokenRequest"
        )
        || compact(&moxy::template! { {{ signature.output }} })
            != format!("->{}", request.replace("Request", "Response"))
    {
        return Err(ParseError::new(
            signature.span(),
            "expected token request/response and borrowed broker/authentication/peer types",
        ));
    }
    let FnParam::Typed(request) = &signature.params.inner.inputs[1] else {
        unreachable!()
    };
    let name = &signature.ident;
    let response = signature.output.as_type().expect("validated response type");
    Ok(Adapter {
        header: moxy::template! {
            fn {{ name }}<'a>(broker: &'a Broker, version: ApiVersion, _correlation_id: CorrelationId, body: &'a [u8], auth: &'a ConnectionAuth, peer: &'a SocketAddr) -> BoxFuture<'a, Result<Bytes, BrokerError>>
        },
        body: moxy::template! {
            Box::pin(async move {
                use krabka_protocol::Decode;
                let mut cursor = body;
                let req = <{{ request.ty }}>::decode(&mut cursor, version)?;
                let response: {{ response }} = __token_operation;
                crate::handlers::encode_response(&response, version)
            })
        },
    })
}

pub(crate) fn expand(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    no_arguments(meta, "token_dispatch")?;
    let generated = adapter(&item.clone().into())?;
    let header: NativeTokenStream = generated.header.into();
    let body: NativeTokenStream = generated.body.into();
    let item = edit_native_body(item, |operation| {
        splice(
            body,
            &[SpliceValue {
                name: "__token_operation".into(),
                group: Group::new(Delimiter::Brace, operation),
            }],
            &["cursor", "response"],
        )
    });
    let mut tokens: Vec<_> = item.into_iter().collect();
    let start = tokens
        .iter()
        .position(
            |token| matches!(token, NativeTokenTree::Ident(name) if name.to_string() == "async"),
        )
        .expect("validated async header");
    let body = tokens.pop().expect("validated function body");
    tokens.truncate(start);
    Ok(tokens.into_iter().chain(header).chain([body]).collect())
}

#[cfg(test)]
mod tests {
    use {super::adapter, crate::meta::compact, assert2::assert};

    #[test]
    fn four_operations_decode_before_native_auth_and_encode_without_trailing_validation() {
        for operation in ["Create", "Renew", "Expire", "Describe"] {
            let item = format!(
                "pub(super) async fn answer(broker: &Broker, req: {operation}DelegationTokenRequest, auth: &ConnectionAuth, peer: &SocketAddr) -> {operation}DelegationTokenResponse {{ if native::<1, 2>() {{ mutation().await }} else {{ refusal() }} }}"
            );
            let generated = adapter(&item.parse().unwrap()).unwrap();
            let body = compact(&generated.body);
            assert!(
                body.find("::decode(&mutcursor,version)?").unwrap()
                    < body.find("__token_operation").unwrap()
            );
            assert!(
                body.find("__token_operation").unwrap()
                    < body.find("encode_response(&response,version)").unwrap()
            );
            assert!(!body.contains("cursor.is_empty()"));
            assert!(
                compact(&generated.header).contains("_correlation_id:CorrelationId,body:&'a[u8]")
            );
        }
    }

    #[test]
    fn refuses_incompatible_or_extra_headers() {
        let valid = "async fn answer(broker: &Broker, req: RenewDelegationTokenRequest, auth: &ConnectionAuth, peer: &SocketAddr) -> RenewDelegationTokenResponse {}";
        for invalid in [
            valid.replace("async fn", "fn"),
            valid.replace("req:", "mut req:"),
            valid.replace("RenewDelegationTokenRequest", "OtherRequest"),
            valid.replace("RenewDelegationTokenResponse", "OtherResponse"),
            format!("{valid} fn other() {{}}"),
            valid.replace("&SocketAddr", "SocketAddr"),
        ] {
            assert!(adapter(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
