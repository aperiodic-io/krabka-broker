//! Apply the two SDK builders' existing connection policy in its original order.

use {
    moxy::{
        ast::ParseError,
        token::{Ident, TokenStream, TokenTree},
    },
    proc_macro::{
        Delimiter, Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree,
    },
};

fn configuration(input: &TokenStream) -> Result<Ident, ParseError> {
    let [builder @ .., comma, TokenTree::Ident(config)] = input.as_ref() else {
        return Err(ParseError::new(
            input.span(),
            "expected a builder expression, config identifier",
        ));
    };
    if builder.is_empty() || !comma.is_punct_comma() {
        return Err(ParseError::new(
            input.span(),
            "expected a builder expression, config identifier",
        ));
    }
    Ok(config.clone())
}

fn policy(config: &Ident) -> TokenStream {
    moxy::template! {
        .with_retry(retry_config({{ config }}.max_retries, {{ config }}.retry_timeout))
        .with_client_options(client_options(
            ClientOptions::new(), {{ config }}.allow_http,
            {{ config }}.request_timeout, {{ config }}.connect_timeout,
        ))
    }
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let config = configuration(&input.clone().into())?;
    let mut builder: Vec<_> = input.into_iter().collect();
    builder.truncate(builder.len() - 2);
    // Group only the opaque expression; nested compiler groups retain their native spans.
    let expression = Group::new(Delimiter::Parenthesis, builder.into_iter().collect());
    let mut output: NativeTokenStream = [NativeTokenTree::Group(expression)].into_iter().collect();
    output.extend(NativeTokenStream::from(policy(&config)));
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{configuration, policy},
        assert2::assert,
    };

    #[test]
    fn reads_only_the_trailing_config_and_leaves_the_builder_opaque() {
        let input = "selected::<1, 2>(&source).with_bucket_name(opaque::<Type>(&value)), config"
            .parse()
            .unwrap();
        let config = configuration(&input).unwrap();
        assert!(config == "config");
    }

    #[yare::parameterized(
        max_retries = { "max_retries" },
        retry_timeout = { "retry_timeout" },
        allow_http = { "allow_http" },
        request_timeout = { "request_timeout" },
        connect_timeout = { "connect_timeout" },
    )]
    fn policy_includes_each_supported_field(field: &str) {
        assert2::assert!(
            policy(&"config".parse().unwrap())
                .to_string()
                .contains(field)
        );
    }

    #[yare::parameterized(
        empty = { "" },
        no_config = { "builder" },
        missing_builder = { ", config" },
        missing_comma = { "builder config" },
        member_config = { "builder, config.member" },
        grouped_config = { "builder, (config)" },
        trailing_comma = { "builder, config," },
    )]
    fn rejects_invalid_builder_and_config_arguments(invalid: &str) {
        assert2::assert!(configuration(&invalid.parse().unwrap()).is_err());
    }
}
