//! Concrete wire projections shared by the handler fixtures and offset reads.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups},
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

struct OffsetRow {
    target: Path,
    missing: Ident,
    stable: Ident,
}

fn offset_rows(input: &TokenStream) -> Result<Vec<OffsetRow>, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut rows = Vec::new();
    while !parser.is_empty() {
        let target = Path::parse(&parser)?;
        let group = grouped_value(&parser, "expected named missing/stable row builders")?;
        if !group.delim.is_brace() {
            return Err(ParseError::new(group.span(), "row builders need braces"));
        }
        let fields = Parser::from_tokens(&group.tokens);
        expect_name(&fields, "missing", "expected missing builder")?;
        let _: Token![:] = Parse::parse(&fields)?;
        let missing = Ident::parse(&fields)?;
        let _: Token![,] = Parse::parse(&fields)?;
        expect_name(&fields, "stable", "expected stable builder")?;
        let _: Token![:] = Parse::parse(&fields)?;
        let stable = Ident::parse(&fields)?;
        if <Token![,]>::peek(fields.cursor()) {
            let _: Token![,] = Parse::parse(&fields)?;
        }
        if !fields.is_empty() {
            return Err(fields.error("unexpected row builder field"));
        }
        rows.push(OffsetRow {
            target,
            missing,
            stable,
        });
    }
    if rows.is_empty() {
        return Err(parser.error("expected at least one offset wire row"));
    }
    Ok(rows)
}

pub(crate) fn offset_fetch_rows(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let rows = offset_rows(input)?;
    Ok(moxy::template! {
        @for row in rows {
            pub(super) fn {{ row.missing }}(partition_index: i32, error_code: i16) -> {{ row.target }} {
                {{ row.target }} {
                    partition_index,
                    committed_offset: -1,
                    committed_leader_epoch: -1,
                    metadata: Some(String::new()),
                    error_code,
                    ..Default::default()
                }
            }
            pub(super) fn {{ row.stable }}(partition_index: i32, entry: &OffsetEntry) -> {{ row.target }} {
                {{ row.target }} {
                    partition_index,
                    committed_offset: entry.offset.0,
                    committed_leader_epoch: entry.leader_epoch,
                    metadata: Some(entry.metadata.clone()),
                    error_code: crate::codes::NONE,
                    ..Default::default()
                }
            }
        }
    })
}

pub(crate) fn share_group_denial(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let target: Path = moxy::parse!({ input })?;
    Ok(moxy::template! {
        {{ target }} {
            throttle_time_ms: 0,
            error_code: crate::codes::GROUP_AUTHORIZATION_FAILED,
            error_message: None,
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            ..Default::default()
        }
    })
}

fn acknowledgement_target(input: &TokenStream) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(input);
    let target = Path::parse(&parser)?;
    let _: Token![;] = Parse::parse(&parser)?;
    let group = grouped_value(&parser, "expected native acknowledgement batches")?;
    if !group.delim.is_bracket() || group.tokens.is_empty() {
        return Err(ParseError::new(
            group.span(),
            "expected nonempty bracketed batches",
        ));
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after acknowledgement batches"));
    }
    Ok(target)
}

fn acknowledgement_template(target: &Path) -> TokenStream {
    moxy::template! {
        __acknowledgement_input.into_iter().map(|batch| {{ target }} {
            first_offset: batch.first_offset.0,
            last_offset: batch.last_offset.0,
            acknowledge_types: batch.types.into_iter().map(|kind| kind.0).collect(),
            ..Default::default()
        }).collect()
    }
}

pub(crate) fn acknowledgement_batches(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let target = acknowledgement_target(&input.clone().into())?;
    let expression = native_groups(input)
        .last()
        .expect("validated batch expression");
    Ok(splice(
        acknowledgement_template(&target).into(),
        &[SpliceValue {
            name: "__acknowledgement_input".into(),
            group: Group::new(Delimiter::None, expression.stream()),
        }],
        &["batch", "kind"],
    ))
}

struct ControllerRoute {
    broker: Ident,
    context: Ident,
    request: Ident,
    version: Ident,
    operation: Path,
    refusal: Path,
}

fn controller_route(input: &TokenStream) -> Result<ControllerRoute, ParseError> {
    let parser = Parser::from_tokens(input);
    let broker = Ident::parse(&parser)?;
    let _: Token![,] = Parse::parse(&parser)?;
    let context = Ident::parse(&parser)?;
    let _: Token![,] = Parse::parse(&parser)?;
    let request = Ident::parse(&parser)?;
    let _: Token![,] = Parse::parse(&parser)?;
    let version = Ident::parse(&parser)?;
    let _: Token![;] = Parse::parse(&parser)?;
    let operation = Path::parse(&parser)?;
    let _: Token![,] = Parse::parse(&parser)?;
    let refusal = Path::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after controller routing"));
    }
    Ok(ControllerRoute {
        broker,
        context,
        request,
        version,
        operation,
        refusal,
    })
}
fn controller_template(route: &ControllerRoute) -> TokenStream {
    moxy::template! {
        match crate::handlers::forward_to_controller::controller_listener_dispatch(
            {{ route.broker }}, {{ route.context }}, {{ route.request }}, {{ route.version }},
            {{ route.operation }}, {{ route.refusal }},
        ).await {
            ::std::ops::ControlFlow::Break(answer) => return answer,
            ::std::ops::ControlFlow::Continue(request) => request,
        }
    }
}

pub(crate) fn controller_request(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let route = controller_route(&input.into())?;
    Ok(controller_template(&route).into())
}
#[cfg(test)]
mod tests {
    use {
        super::{
            acknowledgement_target, acknowledgement_template, controller_route,
            controller_template, offset_fetch_rows, share_group_denial,
        },
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn maps_both_offset_rows_with_their_declared_builder_names() {
        let output = offset_fetch_rows(&"wire::Legacy { missing: absent_legacy, stable: committed_legacy } wire::Group { missing: absent_group, stable: committed_group, }".parse().unwrap()).unwrap();
        let rendered = compact(&output);
        for name in [
            "absent_legacy",
            "committed_legacy",
            "absent_group",
            "committed_group",
        ] {
            assert!(rendered.contains(&format!("fn{name}(")));
        }
        assert!(rendered.contains("committed_offset:-1"));
        assert!(rendered.contains("committed_offset:entry.offset.0"));
        assert!(rendered.contains("metadata:Some(entry.metadata.clone())"));
        for invalid in [
            "",
            "Row(missing: a, stable: b)",
            "Row { stable: a, missing: b }",
            "Row { missing: a, stable: b, other: c }",
            "Row { missing: a(), stable: b }",
        ] {
            assert!(
                offset_fetch_rows(&invalid.parse().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn accepts_native_batch_control_flow_without_parsing_it_as_an_expression() {
        let target = acknowledgement_target(
            &"wire::Batch; [if ready::<1, 2>() { batches()? } else { return fail(); }]"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let output = compact(&acknowledgement_template(&target));
        assert!(output.contains("wire::Batch{first_offset:batch.first_offset.0"));
        assert!(
            output
                .contains("acknowledge_types:batch.types.into_iter().map(|kind|kind.0).collect()")
        );
        for invalid in [
            "",
            "Batch, [batches]",
            "Batch; []",
            "Batch; { batches }",
            "Batch; [batches] extra",
        ] {
            assert!(
                acknowledgement_target(&invalid.parse().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn pins_the_full_empty_group_denial_response() {
        for target in ["ShareFetchResponse", "wire::ShareAcknowledgeResponse"] {
            let output = compact(&share_group_denial(&target.parse().unwrap()).unwrap());
            assert!(output.contains("error_code:crate::codes::GROUP_AUTHORIZATION_FAILED"));
            assert!(output.contains("acquisition_lock_timeout_ms:0"));
            assert!(output.contains("node_endpoints:Vec::new(),..Default::default()"));
        }
        assert!(share_group_denial(&"Response; more".parse().unwrap()).is_err());
    }

    #[test]
    fn controller_routes_keep_typed_order_and_early_return() {
        let route = controller_route(
            &"broker, context, owned, version; self::handle, self::refusal"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let output = compact(&controller_template(&route));
        assert!(output.contains(
            "controller_listener_dispatch(broker,context,owned,version,self::handle,self::refusal,"
        ));
        assert!(output.contains("ControlFlow::Break(answer)=>returnanswer"));
        assert!(output.contains("ControlFlow::Continue(request)=>request"));
        for invalid in [
            "",
            "[route()]",
            "broker, ctx, req; handle, refusal",
            "broker, ctx, req, version; handle(), refusal",
            "broker, ctx, req, version; handle, refusal trailing",
        ] {
            assert!(
                controller_route(&invalid.parse().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }
}
