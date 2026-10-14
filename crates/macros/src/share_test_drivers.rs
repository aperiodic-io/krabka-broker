//! First-acquisition schemas and native share-consumption readiness operations.

use {
    crate::{
        meta::{grouped_value, native_groups},
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{Attributes, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

pub(crate) fn acquisition(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let name = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let mode = Ident::parse(&parser)?;
    if !matches!(mode.text(), "Initial" | "Incremental") || !parser.is_empty() {
        return Err(ParseError::new(
            mode.span(),
            "expected Initial or Incremental acquisition policy",
        ));
    }
    Ok(moxy::template! {
        pub async fn {{ name }}(client: &::krabka_client_core::Client, session: crate::support::share::ShareSessionSetup<'_>)
            -> ::krabka_protocol::owned::share_fetch_response::PartitionData {
            crate::support::share::fetch_until_acquired(client, crate::support::share::share_fetch_req().session(session).call(), crate::support::share::FetchSessionMode::{{ mode }}).await
        }
    })
}

fn consumption_template(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let attributes = Attributes::parse(&parser)?;
    let name = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let join = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    <Token![|]>::parse(&parser)?;
    let broker = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let client = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let member = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let epoch = Ident::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let topic = Ident::parse(&parser)?;
    <Token![|]>::parse(&parser)?;
    let wait = grouped_value(&parser, "expected bracketed native readiness operation")?;
    if !wait.delim.is_bracket() || wait.tokens.is_empty() || !parser.is_empty() {
        return Err(ParseError::new(
            wait.span(),
            "expected one nonempty bracketed readiness operation",
        ));
    }
    Ok(moxy::template! {
        {{ attributes }}
        pub async fn {{ name }}({{ broker }}: &::krabka_broker::BrokerHandle, {{ client }}: &::krabka_client_core::Client, setup: crate::support::share::ConsumptionSetup) -> crate::support::share::MemberSession {
            let {{ topic }} = setup.topic_id;
            crate::support::share::bootstrap_share_state({{ broker }}, {{ client }}, "g1").await;
            crate::support::share::produce_n().client({{ client }}).topic_id({{ topic }}).count(setup.count.0).call().await;
            let crate::support::share::MemberSession { member: {{ member }}, epoch: {{ epoch }} } = {{ join }}({{ client }}, "g1", "t").await;
            __share_readiness.await;
            crate::support::share::MemberSession { member: {{ member }}, epoch: {{ epoch }} }
        }
    })
}

pub(crate) fn consumption(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let template = consumption_template(&input.clone().into())?;
    let wait = native_groups(input)
        .last()
        .expect("validated readiness expression");
    let mut group = Group::new(Delimiter::None, wait.stream());
    group.set_span(wait.span());
    Ok(splice(
        template.into(),
        &[SpliceValue {
            name: "__share_readiness".to_owned(),
            group,
        }],
        &[],
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::{acquisition, consumption_template},
        crate::meta::compact,
        assert2::assert,
    };

    #[yare::parameterized(initial = { "Initial" }, incremental = { "Incremental" })]
    fn acquisition_closes_the_policy_to_initial_and_incremental_sessions(mode: &str) {
        let output = compact(&acquisition(&format!("acquire, {mode}").parse().unwrap()).unwrap());
        assert2::assert!(output.contains(&format!("FetchSessionMode::{mode}")));
    }

    #[yare::parameterized(
        empty = { "" },
        missing_mode = { "acquire" },
        other_mode = { "acquire, Other" },
        trailing = { "acquire, Initial, extra" },
    )]
    fn acquisition_rejects_invalid_inputs(input: &str) {
        assert2::assert!(acquisition(&input.parse().unwrap()).is_err());
    }

    #[test]
    fn native_readiness_runs_only_after_bootstrap_produce_and_join() {
        let output = compact(
            &consumption_template(
                &"initialize, join, |broker, client, member, epoch, tid| [native!(|&row| row)]"
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        assert!(output.find("bootstrap_share_state").unwrap() < output.find("produce_n").unwrap());
        assert!(output.find("produce_n").unwrap() < output.find("join(client").unwrap());
        assert!(
            output.find("join(client").unwrap() < output.find("__share_readiness.await").unwrap()
        );
        assert!(
            output.contains(
                "letcrate::support::share::MemberSession{member:member,epoch:epoch}=join"
            )
        );
        assert!(
            output
                .rfind("MemberSession{member:member,epoch:epoch}")
                .unwrap()
                > output.find("__share_readiness.await").unwrap()
        );
    }

    #[yare::parameterized(
        empty = { "" },
        empty_brackets = { "initialize, join, |a,b,c,d,e| []" },
        empty_braces = { "initialize, join, |a,b,c,d,e| {}" },
        trailing = { "initialize, join, |a,b,c,d,e| [ready()] extra" },
    )]
    fn consumption_template_rejects_invalid_inputs(input: &str) {
        assert2::assert!(consumption_template(&input.parse().unwrap()).is_err());
    }
}
