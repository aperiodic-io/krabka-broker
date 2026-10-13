//! Preserve the share handler's settings snapshot and ordered identity admission.

use {
    crate::meta::{
        compact, edit_native_body, function_signature, named_async_parameter_types, no_arguments,
    },
    moxy::{
        ast::ParseError,
        token::{Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

fn admission(item: &TokenStream) -> Result<TokenStream, ParseError> {
    let signature = function_signature(item)?;
    let message = "expected ordinary async handle(broker, req, version, ctx)";
    if signature.ident != "handle" {
        return Err(ParseError::new(signature.span(), message));
    }
    let types =
        named_async_parameter_types(&signature, &["broker", "req", "version", "ctx"], message)?;
    let response = match types[1].as_str() {
        "ShareFetchRequest" => moxy::template! { ShareFetchResponse },
        "ShareAcknowledgeRequest" => moxy::template! { ShareAcknowledgeResponse },
        _ => {
            return Err(ParseError::new(
                signature.span(),
                "expected an owned share fetch or acknowledge request",
            ));
        }
    };
    let output = compact(&moxy::template! { {{ signature.output }} });
    if types[0] != "&Broker"
        || types[2] != "ApiVersion"
        || types[3] != "&RequestContext<'_>"
        || output != format!("->Result<{},BrokerError>", compact(&response))
    {
        return Err(ParseError::new(
            signature.span(),
            "expected share handler types and its matching response result",
        ));
    }
    Ok(moxy::template! {
        let cfg = broker.config.share_group.clone();
        let image = broker.controller.current_image();
        let crate::handlers::share_fetch::ShareRequestIdentity { group, settings, lock_timeout_ms, member } =
            match crate::handlers::share_fetch::resolve_share_identity(
                broker, &image, ctx, &cfg, req.group_id.as_ref(), req.member_id.as_ref(),
            ) {
                Ok(identity) => identity,
                Err(code) => return Ok({{ response }}::error(code, None)),
            };
    })
}

pub(crate) fn expand(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    no_arguments(meta, "share_request_identity")?;
    let prefix: NativeTokenStream = admission(&item.clone().into())?.into();
    Ok(edit_native_body(item, |body| {
        prefix.into_iter().chain(body).collect()
    }))
}

#[cfg(test)]
mod tests {
    use {super::admission, crate::meta::compact, assert2::assert};

    #[test]
    fn both_handlers_retain_snapshot_and_ordered_early_refusal() {
        for api in ["Fetch", "Acknowledge"] {
            let input = format!(
                "pub(crate) async fn handle(broker: &Broker, req: Share{api}Request, version: ApiVersion, ctx: &RequestContext<'_>) -> Result<Share{api}Response, BrokerError> {{ if opaque::<1, 2>() {{ return operation(); }} else {{ operation()? }} }}"
            );
            let output = compact(&admission(&input.parse().unwrap()).unwrap());
            let clone = output.find("broker.config.share_group.clone()").unwrap();
            let image = output.find("broker.controller.current_image()").unwrap();
            let resolve = output.find("resolve_share_identity(").unwrap();
            assert!(clone < image && image < resolve);
            assert!(output.contains("req.group_id.as_ref(),req.member_id.as_ref()"));
            assert!(output.contains(&format!(
                "Err(code)=>returnOk(Share{api}Response::error(code,None))"
            )));
        }
    }

    #[test]
    fn refuses_wrong_request_response_and_header_shapes() {
        let valid = "async fn handle(broker: &Broker, req: ShareFetchRequest, version: ApiVersion, ctx: &RequestContext<'_>) -> Result<ShareFetchResponse, BrokerError> {}";
        for invalid in [
            valid.replace("async fn", "fn"),
            valid.replace("handle", "other"),
            valid.replace("req:", "mut req:"),
            valid.replace("ShareFetchRequest", "OtherRequest"),
            valid.replace("ShareFetchResponse", "ShareAcknowledgeResponse"),
            valid.replace("ApiVersion", "u8"),
            valid.replace("&Broker", "Broker"),
            valid.replace("&RequestContext<'_>", "&OtherContext"),
            format!("{valid} fn extra() {{}}"),
        ] {
            assert!(admission(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
