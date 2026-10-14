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
    use {super::admission, crate::meta::compact};

    #[yare::parameterized(fetch = { "Fetch" }, acknowledge = { "Acknowledge" })]
    fn both_handlers_retain_snapshot_and_ordered_early_refusal(api: &str) {
        let input = format!(
            "pub(crate) async fn handle(broker: &Broker, req: Share{api}Request, version: ApiVersion, ctx: &RequestContext<'_>) -> Result<Share{api}Response, BrokerError> {{ if opaque::<1, 2>() {{ return operation(); }} else {{ operation()? }} }}"
        );
        let output = compact(&admission(&input.parse().unwrap()).unwrap());
        let clone = output.find("broker.config.share_group.clone()").unwrap();
        let image = output.find("broker.controller.current_image()").unwrap();
        let resolve = output.find("resolve_share_identity(").unwrap();
        assert2::assert!(clone < image && image < resolve);
        assert2::assert!(output.contains("req.group_id.as_ref(),req.member_id.as_ref()"));
        assert2::assert!(output.contains(&format!(
            "Err(code)=>returnOk(Share{api}Response::error(code,None))"
        )));
    }

    #[yare::parameterized(
        sync = { "sync" },
        wrong_method = { "wrong_method" },
        mutable_request = { "mutable_request" },
        wrong_request = { "wrong_request" },
        wrong_response = { "wrong_response" },
        wrong_version = { "wrong_version" },
        owned_broker = { "owned_broker" },
        wrong_context = { "wrong_context" },
        extra_item = { "extra_item" },
    )]
    fn refuses_wrong_request_response_and_header_shapes(case: &str) {
        let valid = "async fn handle(broker: &Broker, req: ShareFetchRequest, version: ApiVersion, ctx: &RequestContext<'_>) -> Result<ShareFetchResponse, BrokerError> {}";
        let invalid = match case {
            "sync" => valid.replace("async fn", "fn"),
            "wrong_method" => valid.replace("handle", "other"),
            "mutable_request" => valid.replace("req:", "mut req:"),
            "wrong_request" => valid.replace("ShareFetchRequest", "OtherRequest"),
            "wrong_response" => valid.replace("ShareFetchResponse", "ShareAcknowledgeResponse"),
            "wrong_version" => valid.replace("ApiVersion", "u8"),
            "owned_broker" => valid.replace("&Broker", "Broker"),
            "wrong_context" => valid.replace("&RequestContext<'_>", "&OtherContext"),
            "extra_item" => format!("{valid} fn extra() {{}}"),
            _ => unreachable!(),
        };
        assert2::assert!(admission(&invalid.parse().unwrap()).is_err(), "{invalid}");
    }
}
