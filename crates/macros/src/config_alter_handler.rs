//! Ordered resource processing shared by the two config mutation APIs.

use {
    crate::meta::{Tokens, broker_handler_signature},
    moxy::{
        ast::{FromMeta, Meta, ParseError, Path},
        token::{LitStr, Spanner, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct Options {
    request: Tokens,
    response: Tokens,
    operation: String,
    audit: Tokens,
    process: Tokens,
    audits_in_process: bool,
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let meta: Meta = moxy::parse!({
        moxy::template! { config_alter_handler({{ input }}) }
    })?;
    let options = Options::from_meta(&meta)?;
    let request: Path = moxy::parse!({ options.request.tokens })?;
    let response: Path = moxy::parse!({ options.response.tokens })?;
    let audit: Path = moxy::parse!({ options.audit.tokens })?;
    let process: Path = moxy::parse!({ options.process.tokens })?;
    let operation = LitStr::new(&options.operation, meta.span());
    let audits_in_process =
        moxy::template! { @if options.audits_in_process { true } @else { false } };
    let signature = broker_handler_signature(&request, &response);
    Ok(moxy::template! {
        /// Both config APIs validate and audit resources in request order.
        {{ signature }} {
            let image = broker.controller.current_image();
            let mut responses = Vec::with_capacity(req.resources.len());
            let validate_only = req.validate_only;
            let mut audited: Vec<krabka_audit::AuditResource> = Vec::new();
            let duplicate_flags = crate::handlers::alter_configs::duplicate_resource_flags(
                req.resources.iter(),
            );
            for (resource, is_duplicate) in req.resources.into_iter().zip(duplicate_flags) {
                let named = {{ audit }}(&resource, &image);
                let response = {{ process }}(broker, &image, ctx, resource, validate_only, is_duplicate).await;
                // A dry run stores nothing and cannot claim a changed resource.
                if response.error_code == crate::codes::NONE && !validate_only && !{{ audits_in_process }} {
                    audited.extend(named);
                }
                responses.push(response);
            }
            crate::handlers::audit_admin_success(broker.audit_log.as_ref(), ctx, {{ operation }}, audited);
            Ok({{ response }} { responses, throttle_time_ms: 0, ..Default::default() })
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact};

    #[yare::parameterized(
        audit_in_process = { true },
        audit_after_process = { false },
    )]
    fn both_apis_keep_image_audit_process_and_response_order(audits_in_process: bool) {
        let input = format!(
            "request = wire::Request, response = wire::Response, operation = \"ConfigMutation\", audit = self::audit, process = self::process, audits_in_process = {audits_in_process}"
        );
        let output = compact(&expand(&input.parse().unwrap()).unwrap());
        let positions = [
            "broker.controller.current_image()",
            "duplicate_resource_flags(",
            "letnamed=self::audit(&resource,&image)",
            "self::process(broker,&image,ctx,resource,validate_only,is_duplicate).await",
            "audited.extend(named)",
            "responses.push(response)",
            "audit_admin_success(",
            "Ok(wire::Response{responses,throttle_time_ms:0,..Default::default()})",
        ]
        .map(|part| output.find(part).expect(part));
        assert2::assert!(
            positions
                .windows(2)
                .all(|positions| positions[0] < positions[1])
        );
        assert2::assert!(output.contains(&format!("!validate_only&&!{audits_in_process}")));
        assert2::assert!(output.contains("req:wire::Request"));
        assert2::assert!(output.contains("\"ConfigMutation\""));
    }

    const VALID: &str = "request = Request, response = Response, operation = \"AlterConfigs\", audit = audit, process = process, audits_in_process = true";

    #[yare::parameterized(
        empty = { String::new() },
        missing_option = { VALID.replace(", audits_in_process = true", "") },
        unknown_option = { format!("{VALID}, unknown = true") },
        nonpath_request = { VALID.replace("request = Request", "request = factory()") },
        invalid_response = { VALID.replace("response = Response", "response = 1") },
        nonpath_audit = { VALID.replace("audit = audit", "audit = audit()") },
        nonpath_process = { VALID.replace("process = process", "process = process()") },
        invalid_boolean = {
            VALID.replace("audits_in_process = true", "audits_in_process = \"yes\""),
        },
    )]
    fn rejects_missing_unknown_and_nonpath_operation_selectors(invalid: String) {
        assert2::assert!(expand(&invalid.parse().unwrap()).is_err(), "{invalid}");
    }
}
