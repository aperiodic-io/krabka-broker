//! Small expression contracts that retain caller-level returns and assertion policies.

use {
    crate::meta::{arguments, native_expression},
    moxy::{
        ast::{Parse, ParseError, Parser},
        token::{Group, Ident, Span, TokenStream},
    },
    proc_macro::{TokenStream as NativeTokenStream, TokenTree},
};

pub(crate) fn guard_response(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let values = arguments(input.clone(), 2)?;
    let client: Ident = moxy::parse!({ &values[0] })?;
    let request: Ident = moxy::parse!({ &values[1] })?;
    let response = Ident::new("checked_response").with_span(Span::mixed_site());
    Ok(moxy::template! {
        {
            let {{ response }} = {{ client }}.send({{ request }}).await.map_err(Failure::from)?;
            if {{ response }}.error_code != 0 {
                return Ok(report_error({{ response }}.error_code, {{ response }}.error_message.as_deref()));
            }
            {{ response }}
        }
    })
}

pub(crate) fn evicted_segments(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let values = arguments(input.clone(), 4)?;
    let assertion: Ident = moxy::parse!({ &values[0] })?;
    if !matches!(assertion.text(), "assert" | "check") {
        return Err(ParseError::new(
            assertion.span(),
            "eviction checks need assert or check",
        ));
    }
    let removed: Ident = moxy::parse!({ &values[1] })?;
    let exports: Ident = moxy::parse!({ &values[2] })?;
    let partition = &values[3];
    let log = Ident::new("retained_log").with_span(Span::mixed_site());
    let active = Ident::new("active_base").with_span(Span::mixed_site());
    let sealed = Ident::new("sealed").with_span(Span::mixed_site());
    Ok(moxy::template! {
        {
            {{ assertion }}!({{ removed }} == {{ exports }}.len());
            let mut {{ log }} = fixtures::partition_log_guard({{ partition }});
            let {{ active }} = {{ exports }}.last().unwrap().last_offset + 1;
            {{ assertion }}!({{ log }}.local_log_start_offset() == {{ active }});
            let {{ sealed }} = sealed_ranges(&mut {{ log }});
            {{ assertion }}!({{ sealed }} == vec![SealedRange::new({{ active }}, {{ log }}.log_end_offset() - 1)]);
        }
    })
}

fn check_sources(input: &TokenStream) -> Result<(), ParseError> {
    let parser = Parser::from_tokens(input);
    for expected in ["assignment", "builder"] {
        let label = Ident::parse(&parser)?;
        let body = Group::parse(&parser)?;
        if label.text() != expected || !body.delim.is_brace() || body.tokens.is_empty() {
            return Err(ParseError::new(
                label.span(),
                "expected nonempty assignment {} then builder {}",
            ));
        }
    }
    if !parser.is_empty() {
        return Err(parser.error("expected assignment and builder sources only"));
    }
    Ok(())
}

pub(crate) fn reassignment(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    check_sources(&input.clone().into())?;
    let source: Vec<_> = input.into_iter().collect();
    let [
        TokenTree::Ident(_),
        TokenTree::Group(assignment),
        TokenTree::Ident(_),
        TokenTree::Group(builder),
    ] = source.as_slice()
    else {
        unreachable!("validated named native sources")
    };
    let binding = Ident::new("assignment").with_span(Span::mixed_site());
    let mut body: NativeTokenStream = moxy::template! { let {{ binding }} = }.into();
    body.extend([TokenTree::Group(assignment.clone())]);
    body.extend(NativeTokenStream::from(moxy::template! { ; }));
    body.extend([TokenTree::Group(builder.clone())]);
    body.extend(NativeTokenStream::from(moxy::template! {
        .replicas({{ binding }}.replicas).isr({{ binding }}.isr)
        .adding({{ binding }}.adding).removing({{ binding }}.removing)
        .leader({{ binding }}.leader.0).directories({{ binding }}.directories)
    }));
    Ok(native_expression(body))
}

#[cfg(test)]
mod tests {
    use {
        super::{check_sources, evicted_segments, guard_response},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn checked_send_keeps_transport_error_before_refusal_and_local_return() {
        let output = compact(&guard_response(&"client, request".parse().unwrap()).unwrap());
        assert!(
            output.find("map_err(Failure::from)?").unwrap() < output.find("error_code!=0").unwrap()
        );
        assert!(output.contains("returnOk(report_error("));
        assert!(guard_response(&"client(), request".parse().unwrap()).is_err());
    }

    #[yare::parameterized(
        hard_assert = { "assert" },
        soft_check = { "check" },
    )]
    fn eviction_assertions_keep_first_check_before_lock_and_soft_check_policy(assertion: &str) {
        let output = compact(
            &evicted_segments(
                &format!("{assertion}, removed, exports, &partition")
                    .parse()
                    .unwrap(),
            )
            .unwrap(),
        );
        assert2::assert!(
            output.find("removed==exports.len()").unwrap()
                < output.find("partition_log_guard(&partition)").unwrap()
        );
        assert2::assert!(output.matches(&format!("{assertion}!")).count() == 3);
    }

    #[test]
    fn eviction_assertions_reject_an_unknown_policy() {
        assert!(evicted_segments(&"panic, removed, exports, &partition".parse().unwrap()).is_err());
    }

    #[test]
    fn reassignment_sources_are_ordered_and_keep_caller_blocks_opaque() {
        assert!(
            check_sources(
                &"assignment { caller!(@source); } builder { make::<T>() }"
                    .parse()
                    .unwrap()
            )
            .is_ok()
        );
    }

    #[yare::parameterized(
        reversed = { "builder { make() } assignment { assignment }" },
        empty_assignment = { "assignment {} builder { make() }" },
        nonblock_builder = { "assignment { value } builder (make())" },
        trailing_tokens = { "assignment { value } builder { make() } extra" },
    )]
    fn reassignment_sources_reject_invalid_layouts(input: &str) {
        assert2::assert!(check_sources(&input.parse().unwrap()).is_err(), "{input}");
    }
}
