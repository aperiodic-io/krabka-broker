//! Fixed wire fields shared by independent ACL-filter fixtures.

use moxy::{
    ast::{Parse, ParseError, Parser, Path, PathArguments, Token},
    token::{Ident, Spanner, TokenStream},
};

pub(crate) fn empty_match_acl_filter(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let target: Path = moxy::parse!({ input })?;
    Ok(moxy::template! {
        {{ target }} {
            resource_type_filter: crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: crate::handlers::acl_test_support::PATTERN_TYPE_MATCH,
            principal_filter: Some(String::new()),
            host_filter: None,
            operation: crate::handlers::acl_test_support::OPERATION_ANY,
            permission_type: crate::handlers::acl_test_support::PERMISSION_ANY,
            ..Default::default()
        }
    })
}

pub(crate) fn token_refusal_result(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let target = Path::parse(&parser)?;
    if !target.last().is_some_and(|segment| {
        matches!(
            segment.ident.text(),
            "RenewDelegationTokenResponse" | "ExpireDelegationTokenResponse"
        )
    }) || target
        .iter()
        .any(|segment| !matches!(segment.args, PathArguments::None))
    {
        return Err(ParseError::new(
            target.span(),
            "expected a concrete Renew or ExpireDelegationTokenResponse",
        ));
    }
    let _: Token![,] = Parse::parse(&parser)?;
    let case = Ident::parse(&parser)?;
    let _: Token![,] = Parse::parse(&parser)?;
    let actual = Ident::parse(&parser)?;
    if !parser.is_empty() {
        return Err(
            parser.error("expected response type, case binding and actual response binding")
        );
    }
    Ok(moxy::template! {
        crate::handlers::delegation_token_test_support::MutationResponse {
            actual: {{ actual }},
            expected: {{ target }} {
                error_code: {{ case }}.error.0,
                expiry_timestamp_ms: {{ case }}.expiry.0,
                ..Default::default()
            },
            case: {{ case }}.name,
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{empty_match_acl_filter, token_refusal_result},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn constructs_complete_pinned_refusals_for_both_token_responses() {
        for target in [
            "RenewDelegationTokenResponse",
            "wire::ExpireDelegationTokenResponse",
        ] {
            let output =
                token_refusal_result(&format!("{target}, case, response").parse().unwrap())
                    .unwrap();
            let rendered = compact(&output);
            assert!(rendered.contains(&format!("expected:{target}{{error_code:case.error.0,expiry_timestamp_ms:case.expiry.0,..Default::default()}}")));
            assert!(rendered.contains("actual:response"));
            assert!(rendered.contains("case:case.name"));
        }
        for invalid in [
            "",
            "OtherResponse, case, response",
            "RenewDelegationTokenResponse<u8>, case, response",
            "RenewDelegationTokenResponse case, response",
            "RenewDelegationTokenResponse, next(), response",
            "RenewDelegationTokenResponse, case, response.field",
            "RenewDelegationTokenResponse, case, response, extra",
        ] {
            assert!(
                token_refusal_result(&invalid.parse().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn generates_the_same_explicit_empty_axes_for_both_wire_shapes() {
        for target in ["DescribeAclsRequest", "DeleteAclsFilter"] {
            let output = empty_match_acl_filter(&target.parse().unwrap()).unwrap();
            let rendered = compact(&output);
            assert!(rendered.starts_with(target));
            for axis in [
                "resource_type_filter:crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC",
                "resource_name_filter:Some(String::new())",
                "pattern_type_filter:crate::handlers::acl_test_support::PATTERN_TYPE_MATCH",
                "principal_filter:Some(String::new())",
                "host_filter:None",
                "operation:crate::handlers::acl_test_support::OPERATION_ANY",
                "permission_type:crate::handlers::acl_test_support::PERMISSION_ANY",
                "..Default::default()",
            ] {
                assert!(rendered.contains(axis), "{axis}");
            }
        }
    }

    #[test]
    fn refuses_an_empty_target_or_extra_items() {
        for invalid in ["", "{}", "Filter; second", "Filter, Other"] {
            assert!(empty_match_acl_filter(&invalid.parse().unwrap()).is_err());
        }
    }
}
