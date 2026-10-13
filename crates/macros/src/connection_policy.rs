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
        let emitted = policy(&config).to_string();
        for field in [
            "max_retries",
            "retry_timeout",
            "allow_http",
            "request_timeout",
            "connect_timeout",
        ] {
            assert!(emitted.contains(field));
        }
        for invalid in [
            "",
            "builder",
            ", config",
            "builder config",
            "builder, config.member",
            "builder, (config)",
            "builder, config,",
        ] {
            assert!(configuration(&invalid.parse().unwrap()).is_err());
        }
    }
}
