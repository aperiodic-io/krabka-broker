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
    };

    #[yare::parameterized(
        renew = { "RenewDelegationTokenResponse" },
        expire = { "wire::ExpireDelegationTokenResponse" },
    )]
    fn constructs_complete_pinned_refusals_for_both_token_responses(target: &str) {
        let output =
            token_refusal_result(&format!("{target}, case, response").parse().unwrap()).unwrap();
        let rendered = compact(&output);
        assert2::assert!(rendered.contains(&format!("expected:{target}{{error_code:case.error.0,expiry_timestamp_ms:case.expiry.0,..Default::default()}}")));
        assert2::assert!(rendered.contains("actual:response"));
        assert2::assert!(rendered.contains("case:case.name"));
    }

    #[yare::parameterized(
        request = { "DescribeAclsRequest" },
        filter = { "DeleteAclsFilter" },
    )]
    fn generates_the_same_explicit_empty_axes_for_both_wire_shapes(target: &str) {
        let output = empty_match_acl_filter(&target.parse().unwrap()).unwrap();
        let rendered = compact(&output);
        assert2::assert!(rendered.starts_with(target));
        assert2::assert!(rendered.contains(
            "resource_type_filter:crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC"
        ));
        assert2::assert!(rendered.contains("resource_name_filter:Some(String::new())"));
        assert2::assert!(
            rendered.contains(
                "pattern_type_filter:crate::handlers::acl_test_support::PATTERN_TYPE_MATCH"
            )
        );
        assert2::assert!(rendered.contains("principal_filter:Some(String::new())"));
        assert2::assert!(rendered.contains("host_filter:None"));
        assert2::assert!(
            rendered.contains("operation:crate::handlers::acl_test_support::OPERATION_ANY")
        );
        assert2::assert!(
            rendered.contains("permission_type:crate::handlers::acl_test_support::PERMISSION_ANY")
        );
        assert2::assert!(rendered.contains("..Default::default()"));
    }

    #[yare::parameterized(
        empty = { "" },
        empty_group = { "{}" },
        trailing_item = { "Filter; second" },
        extra_argument = { "Filter, Other" },
    )]
    fn refuses_an_empty_target_or_extra_items(invalid: &str) {
        assert2::assert!(empty_match_acl_filter(&invalid.parse().unwrap()).is_err());
    }
}
