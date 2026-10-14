//! Full reconciliation method declarations with typed helpers and native operations.

use {
    crate::meta::{
        BracketedFunction, Tokens, bracketed_function, function_signature, is_plain_sync,
        parameter_ident,
    },
    moxy::{
        ast::{FnParam, FromMeta, Meta, Parse, ParseError, Parser, Path, Signature},
        token::{Ident, Span, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
};

#[derive(moxy::FromMeta)]
struct Rebuild {
    member: Tokens,
    built: Tokens,
}

#[derive(moxy::FromMeta)]
struct Actions {
    action: Tokens,
    lookup: Tokens,
    member: Tokens,
}

#[derive(moxy::FromMeta)]
struct Transition {
    action: Tokens,
    lookup: Tokens,
    rebuild: Tokens,
    check: Tokens,
    project: Tokens,
}

fn name(value: &Tokens) -> Result<Ident, ParseError> {
    let parser = Parser::from_tokens(&value.tokens);
    let ident = Ident::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("expected one operation binding"));
    }
    Ok(ident)
}

fn path(value: &Tokens) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(&value.tokens);
    let path = Path::parse(&parser)?;
    if !parser.is_empty() || path.is_empty() {
        return Err(parser.error("expected one operation helper path"));
    }
    Ok(path)
}

fn metadata<T: FromMeta>(tokens: &TokenStream) -> Result<T, ParseError> {
    let meta: Meta = moxy::parse!({
        moxy::template! { reconciliation({{ tokens }}) }
    })?;
    T::from_meta(&meta)
}

fn receiver(signature: &Signature) -> Result<Ident, ParseError> {
    if !is_plain_sync(signature) || signature.params.inner.inputs.len() != 3 {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected a concrete synchronous reconciliation method with three parameters",
        ));
    }
    let Some(FnParam::Receiver(receiver)) = signature.params.inner.inputs.first() else {
        return Err(ParseError::new(
            signature.ident.span(),
            "reconciliation method needs &self",
        ));
    };
    if receiver.reference.is_none() || receiver.mutability.is_some() {
        return Err(ParseError::new(
            receiver.span(),
            "reconciliation method needs immutable &self",
        ));
    }
    Ok(Ident::new("self").with_span(receiver.self_keyword.span()))
}

fn append_group(output: &mut NativeTokenStream, body: NativeTokenStream) {
    output.extend([TokenTree::Group(Group::new(Delimiter::Brace, body))]);
}

struct OperationDeclaration {
    header: NativeTokenStream,
    metadata: NativeTokenStream,
    body: Group,
    signature: Signature,
}

fn shell(input: NativeTokenStream, macro_name: &str) -> Result<OperationDeclaration, ParseError> {
    let BracketedFunction {
        header,
        metadata,
        body,
    } = bracketed_function(input, macro_name)?;
    let mut item = header.clone();
    append_group(&mut item, NativeTokenStream::new());
    let signature = function_signature(&item.into())?;
    Ok(OperationDeclaration {
        header,
        metadata,
        body,
        signature,
    })
}

struct RebuildParts {
    initial: TokenStream,
    loop_header: TokenStream,
    member_prefix: TokenStream,
    member_suffix: TokenStream,
    result: TokenStream,
}

fn rebuild_parts(args: &TokenStream, signature: &Signature) -> Result<RebuildParts, ParseError> {
    if !is_plain_sync(signature)
        || signature.params.inner.inputs.len() != 1
        || !signature
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.path.is_ident("GroupState"))
    {
        return Err(ParseError::new(
            signature.ident.span(),
            "group rebuilding needs fn(state: &Projection) -> GroupState",
        ));
    }
    let state = parameter_ident(
        &signature.params.inner.inputs[0],
        "group projection parameter needs a name",
    )?;
    let args: Rebuild = metadata(args)?;
    let member = name(&args.member)?;
    let built = name(&args.built)?;
    let group = Ident::new("group").with_span(Span::mixed_site());
    let now = Ident::new("now").with_span(Span::mixed_site());
    Ok(RebuildParts {
        initial: moxy::template! {
            let mut {{ group }} = GroupState::new("g");
            {{ group }}.group_epoch = {{ state }}.group_epoch;
            {{ group }}.record_metadata_hash({{ state }}.metadata_hash);
            {{ group }}.target.epoch = {{ state }}.target_epoch;
            let {{ now }} = Instant::now();
        },
        loop_header: moxy::template! { for {{ member }} in &{{ state }}.members },
        member_prefix: moxy::template! {
            let {{ built }} = modeled_member(&{{ member }}.id, {{ member }}.member_epoch, {{ member }}.assignment_state, &{{ member }}.assigned, &{{ member }}.pending_revocation, {{ now }});
            let {{ built }} =
        },
        member_suffix: moxy::template! { ; insert_modeled_member(&mut {{ group }}, {{ built }}, &{{ member }}.target); },
        result: moxy::template! { {{ group }} },
    })
}

pub(crate) fn rebuild(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let OperationDeclaration {
        mut header,
        metadata: args,
        body,
        signature,
    } = shell(input, "rebuild_reconciliation_group")?;
    let parts = rebuild_parts(&args.into(), &signature)?;
    let mut generated: NativeTokenStream = parts.initial.into();
    let mut loop_body: NativeTokenStream = parts.member_prefix.into();
    loop_body.extend([TokenTree::Group(body)]);
    loop_body.extend(NativeTokenStream::from(parts.member_suffix));
    generated.extend(NativeTokenStream::from(parts.loop_header));
    append_group(&mut generated, loop_body);
    generated.extend(NativeTokenStream::from(parts.result));
    append_group(&mut header, generated);
    Ok(header)
}

struct ActionParts {
    before: TokenStream,
    loop_header: TokenStream,
    member_prefix: TokenStream,
}

fn action_parts(args: &TokenStream, signature: &Signature) -> Result<ActionParts, ParseError> {
    let this = receiver(signature)?;
    if signature.ident.text() != "actions" || !signature.output.is_default() {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected the ordinary actions method without a return type",
        ));
    }
    let state = parameter_ident(
        &signature.params.inner.inputs[1],
        "state parameter needs a name",
    )?;
    let actions = parameter_ident(
        &signature.params.inner.inputs[2],
        "actions parameter needs a name",
    )?;
    let args: Actions = metadata(args)?;
    let action = path(&args.action)?;
    let lookup = path(&args.lookup)?;
    let member = name(&args.member)?;
    let under_cap = Ident::new("under_cap").with_span(Span::mixed_site());
    let id = Ident::new("id").with_span(Span::mixed_site());
    let movement = Ident::new("movement").with_span(Span::mixed_site());
    Ok(ActionParts {
        before: moxy::template! {
            let {{ under_cap }} = {{ state }}.group_epoch < {{ this }}.max_epoch;
            if {{ under_cap }} {
                for &{{ id }} in &{{ this }}.pool {
                    if {{ lookup }}({{ state }}, {{ id }}).is_none() { {{ actions }}.push({{ action }}::Join({{ id }}.to_string())); }
                }
            }
        },
        loop_header: moxy::template! { for {{ member }} in &{{ state }}.members },
        member_prefix: moxy::template! {
            if {{ under_cap }} {
                {{ actions }}.push({{ action }}::Leave({{ member }}.id.clone()));
                {{ actions }}.push({{ action }}::Heartbeat({{ member }}.id.clone()));
                {{ actions }}.push({{ action }}::Keepalive({{ member }}.id.clone()));
            }
            for {{ movement }} in client_moves(&{{ state }}.advertised, &{{ state }}.client_owned, &{{ member }}.id) {
                {{ actions }}.push(if {{ movement }}.add { {{ action }}::ClientAdd({{ member }}.id.clone(), {{ movement }}.partition) } else { {{ action }}::ClientRevoke({{ member }}.id.clone(), {{ movement }}.partition) });
            }
        },
    })
}

pub(crate) fn actions(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let OperationDeclaration {
        mut header,
        metadata: args,
        body,
        signature,
    } = shell(input, "reconciliation_actions")?;
    let parts = action_parts(&args.into(), &signature)?;
    let mut generated: NativeTokenStream = parts.before.into();
    let mut per_member: NativeTokenStream = parts.member_prefix.into();
    per_member.extend([TokenTree::Group(body)]);
    generated.extend(NativeTokenStream::from(parts.loop_header));
    append_group(&mut generated, per_member);
    append_group(&mut header, generated);
    Ok(header)
}

struct TransitionSections {
    setup: Group,
    actions: Group,
    project: Group,
}

fn validate_sections(input: &TokenStream) -> Result<(), ParseError> {
    let parser = Parser::from_tokens(input);
    for expected in ["setup", "actions", "project"] {
        let name = Ident::parse(&parser).map_err(|_| {
            ParseError::new(
                input.span(),
                "transition body needs setup {}, actions {} and project {} native sections",
            )
        })?;
        let group = moxy::token::Group::parse(&parser).map_err(|_| {
            ParseError::new(
                input.span(),
                "transition body needs setup {}, actions {} and project {} native sections",
            )
        })?;
        if name.text() != expected || !group.delim.is_brace() {
            return Err(ParseError::new(
                input.span(),
                "expected ordered setup/actions/project blocks",
            ));
        }
    }
    if !parser.is_empty() {
        return Err(parser
            .error("transition body needs setup {}, actions {} and project {} native sections"));
    }
    Ok(())
}

fn transition_sections(body: &Group) -> Result<TransitionSections, ParseError> {
    validate_sections(&body.stream().into())?;
    let tokens: Vec<_> = body.stream().into_iter().collect();
    let [
        TokenTree::Ident(_),
        TokenTree::Group(setup),
        TokenTree::Ident(_),
        TokenTree::Group(actions),
        TokenTree::Ident(_),
        TokenTree::Group(project),
    ] = tokens.as_slice()
    else {
        unreachable!("validated named native sections")
    };
    Ok(TransitionSections {
        setup: setup.clone(),
        actions: actions.clone(),
        project: project.clone(),
    })
}

struct TransitionParts {
    before_setup: TokenStream,
    before_match: TokenStream,
    match_prefix: TokenStream,
    match_suffix: TokenStream,
    after_match: TokenStream,
    before_extra: TokenStream,
    result: TokenStream,
}

fn transition_parts(
    args: &TokenStream,
    signature: &Signature,
    has_extra: bool,
) -> Result<TransitionParts, ParseError> {
    let this = receiver(signature)?;
    if signature.ident.text() != "next_state"
        || !signature
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.path.is_ident("Option"))
    {
        return Err(ParseError::new(
            signature.ident.span(),
            "expected the ordinary next_state method returning Option<Self::State>",
        ));
    }
    let last = parameter_ident(
        &signature.params.inner.inputs[1],
        "previous state parameter needs a name",
    )?;
    let action_value = parameter_ident(
        &signature.params.inner.inputs[2],
        "action parameter needs a name",
    )?;
    let args: Transition = metadata(args)?;
    let action = path(&args.action)?;
    let lookup = path(&args.lookup)?;
    let rebuild = path(&args.rebuild)?;
    let check = path(&args.check)?;
    let project = path(&args.project)?;
    let owned = Ident::new("owned").with_span(Span::mixed_site());
    let advertised = Ident::new("advertised").with_span(Span::mixed_site());
    let add = Ident::new("add").with_span(Span::mixed_site());
    let next = Ident::new("next").with_span(Span::mixed_site());
    let event = Ident::new("event").with_span(Span::mixed_site());
    let id = Ident::new("id").with_span(Span::mixed_site());
    let partition = Ident::new("partition").with_span(Span::mixed_site());
    let member = Ident::new("member").with_span(Span::mixed_site());
    let group = Ident::new("group").with_span(Span::mixed_site());
    let extra_value = Ident::new("projection_extra").with_span(Span::mixed_site());
    Ok(TransitionParts {
        before_setup: moxy::template! {
            let mut {{ owned }} = owned_map(&{{ last }}.client_owned);
            // std map collection requires key/value pairs at this runtime boundary.
            let mut {{ advertised }}: BTreeMap<_, _> = {{ last }}.advertised.iter().map(|row| (row.member_id.clone(), row.partitions.clone())).collect();
        },
        before_match: moxy::template! { let {{ add }} = matches!({{ action_value }}, {{ action }}::ClientAdd(..)); let {{ event }} = match {{ action_value }} },
        match_prefix: moxy::template! {
            {{ action }}::ClientAdd({{ id }}, {{ partition }}) | {{ action }}::ClientRevoke({{ id }}, {{ partition }}) => {
                let mut {{ next }} = {{ last }}.clone();
                {{ next }}.client_owned = apply_client_move(&{{ last }}.advertised, &mut {{ owned }}, {{ id }}, {{ partition }}, {{ add }})?;
                return Some({{ next }});
            },
        },
        match_suffix: moxy::template! { {{ event }} => {{ event }}.member_heartbeat(|{{ id }}| {{ lookup }}({{ last }}, {{ id }}).map(|{{ member }}| {{ member }}.member_epoch))?, },
        after_match: moxy::template! {
            ; let mut {{ group }} = {{ rebuild }}({{ last }});
            apply_member_heartbeat(&mut {{ group }}, &{{ this }}.metadata(), {{ event }}, &mut {{ owned }}, &mut {{ advertised }});
            {{ check }}({{ last }}, &{{ group }});
        },
        before_extra: moxy::template! { @if has_extra { let {{ extra_value }} = } },
        result: moxy::template! { Some({{ project }}(&{{ group }}, &{{ owned }}, &{{ advertised }} @if has_extra { , {{ extra_value }} })) },
    })
}

pub(crate) fn next_state(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let OperationDeclaration {
        mut header,
        metadata: args,
        body,
        signature,
    } = shell(input, "reconciliation_next_state")?;
    let has_extra = body.stream().into_iter().last().is_some_and(
        |token| matches!(token, TokenTree::Group(group) if !group.stream().is_empty()),
    );
    let parts = transition_parts(&args.into(), &signature, has_extra)?;
    let TransitionSections {
        setup,
        actions: custom_actions,
        project: extra,
    } = transition_sections(&body)?;
    let mut generated: NativeTokenStream = parts.before_setup.into();
    generated.extend(setup.stream());
    generated.extend(NativeTokenStream::from(parts.before_match));
    let mut arms: NativeTokenStream = parts.match_prefix.into();
    arms.extend(custom_actions.stream());
    arms.extend(NativeTokenStream::from(parts.match_suffix));
    append_group(&mut generated, arms);
    generated.extend(NativeTokenStream::from(parts.after_match));
    if has_extra {
        generated.extend(NativeTokenStream::from(parts.before_extra));
        generated.extend([TokenTree::Group(extra)]);
        generated.extend(NativeTokenStream::from(moxy::template! { ; }));
    }
    generated.extend(NativeTokenStream::from(parts.result));
    append_group(&mut header, generated);
    Ok(header)
}

#[cfg(test)]
mod tests {
    use {
        super::{action_parts, rebuild_parts, transition_parts, validate_sections},
        crate::meta::{compact, function_signature},
        assert2::assert,
    };

    #[test]
    fn rebuild_plan_preserves_initial_facts_and_native_member_insertion_point() {
        let signature = function_signature(
            &"fn rebuild(previous: &State) -> GroupState {}"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let parts = rebuild_parts(
            &"member = projected, built = actual".parse().unwrap(),
            &signature,
        )
        .unwrap();
        let initial = compact(&parts.initial);
        assert!(
            initial.find("group.group_epoch").unwrap()
                < initial.find("group.record_metadata_hash").unwrap()
        );
        assert!(
            initial.find("group.record_metadata_hash").unwrap()
                < initial.find("group.target.epoch").unwrap()
        );
        assert!(initial.matches("Instant::now()").count() == 1);
        assert!(compact(&parts.loop_header) == "forprojectedin&previous.members");
        assert!(compact(&parts.member_prefix).ends_with("letactual="));
        assert!(
            compact(&parts.member_suffix)
                .contains("insert_modeled_member(&mutgroup,actual,&projected.target)")
        );
        assert!(compact(&parts.result) == "group");
    }

    #[test]
    fn action_plan_preserves_join_then_member_then_named_client_move_order() {
        let signature = function_signature(
            &"fn actions(&self, snapshot: &State, events: &mut Vec<Action>) {}"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let parts = action_parts(
            &"action = Action, lookup = find_member, member = projected"
                .parse()
                .unwrap(),
            &signature,
        )
        .unwrap();
        assert!(compact(&parts.before).contains("events.push(Action::Join(id.to_string()))"));
        assert!(compact(&parts.loop_header) == "forprojectedin&snapshot.members");
        let body = compact(&parts.member_prefix);
        assert!(body.find("Action::Leave").unwrap() < body.find("Action::Heartbeat").unwrap());
        assert!(body.find("Action::Heartbeat").unwrap() < body.find("Action::Keepalive").unwrap());
        assert!(body.find("Action::Keepalive").unwrap() < body.find("client_moves").unwrap());
        assert!(body.contains("movement.add") && body.contains("movement.partition"));
    }

    #[yare::parameterized(
        no_extra_projection = { false },
        extra_projection = { true },
    )]
    fn transition_plan_checks_before_projection_and_conditionally_accepts_native_extra(
        has_extra: bool,
    ) {
        let signature = function_signature(&"fn next_state(&self, previous: &Self::State, command: Self::Action) -> Option<Self::State> {}".parse().unwrap()).unwrap();
        let args = "action = Action, lookup = find_member, rebuild = rebuild_group, check = assert_epoch, project = project".parse().unwrap();
        let parts = transition_parts(&args, &signature, has_extra).unwrap();
        assert2::assert!(
            compact(&parts.before_setup).contains("row.member_id.clone(),row.partitions.clone()")
        );
        assert2::assert!(
            compact(&parts.before_match).contains("matches!(command,Action::ClientAdd(..))")
        );
        assert2::assert!(compact(&parts.match_prefix).contains("returnSome(next)"));
        assert2::assert!(compact(&parts.match_suffix).contains("find_member(previous,id)"));
        let after = compact(&parts.after_match);
        assert2::assert!(
            after.find("rebuild_group").unwrap() < after.find("apply_member_heartbeat").unwrap()
        );
        assert2::assert!(
            after.find("apply_member_heartbeat").unwrap() < after.find("assert_epoch").unwrap()
        );
        assert2::assert!(parts.before_extra.is_empty() != has_extra);
        assert2::assert!(compact(&parts.result).contains("projection_extra") == has_extra);
    }

    #[yare::parameterized(
        async_rebuild = { "async fn rebuild(state: &State) -> GroupState {}" },
        missing_state = { "fn rebuild() -> GroupState {}" },
        wrong_return_type = { "fn rebuild(state: &State) -> bool {}" },
        anonymous_state = { "fn rebuild(_: &State) -> GroupState {}" },
    )]
    fn rejects_invalid_rebuild_signatures(header: &str) {
        let rebuild_args = "member = member, built = built".parse().unwrap();
        assert2::assert!(
            rebuild_parts(
                &rebuild_args,
                &function_signature(&header.parse().unwrap()).unwrap()
            )
            .is_err(),
            "{header}"
        );
    }

    #[yare::parameterized(
        owned_receiver = { "fn actions(self, state: &State, out: &mut Vec<Action>) {}" },
        mutable_receiver = { "fn actions(&mut self, state: &State, out: &mut Vec<Action>) {}" },
        wrong_receiver_name = {
            "fn actions(model: &Model, state: &State, out: &mut Vec<Action>) {}",
        },
        wrong_method_name = { "fn events(&self, state: &State, out: &mut Vec<Action>) {}" },
        return_type = { "fn actions(&self, state: &State, out: &mut Vec<Action>) -> bool {}" },
        missing_parameters = { "fn actions(&self) {}" },
    )]
    fn rejects_invalid_action_signatures(header: &str) {
        let actions_args = "action = Action, lookup = member, member = m"
            .parse()
            .unwrap();
        assert2::assert!(
            action_parts(
                &actions_args,
                &function_signature(&header.parse().unwrap()).unwrap()
            )
            .is_err(),
            "{header}"
        );
    }

    #[yare::parameterized(
        extra_member_expression = { "action = Action, lookup = member, member = m + extra" },
        extra_action_expression = { "action = Action + Other, lookup = member, member = m" },
        extra_lookup_expression = { "action = Action, lookup = member + extra, member = m" },
        missing_member = { "action = Action, lookup = member" },
    )]
    fn rejects_invalid_action_metadata(args: &str) {
        let actions_header = function_signature(
            &"fn actions(&self, state: &State, out: &mut Vec<Action>) {}"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert2::assert!(
            action_parts(&args.parse().unwrap(), &actions_header).is_err(),
            "{args}"
        );
    }

    #[yare::parameterized(
        wrong_method_name = { "fn step(&self, state: &State, action: Action) -> Option<State> {}" },
        wrong_return_type = { "fn next_state(&self, state: &State, action: Action) -> bool {}" },
        anonymous_state = { "fn next_state(&self, _: &State, action: Action) -> Option<State> {}" },
        anonymous_action = { "fn next_state(&self, state: &State, _: Action) -> Option<State> {}" },
    )]
    fn rejects_invalid_transition_signatures(header: &str) {
        let next_args =
            "action = Action, lookup = member, rebuild = rebuild, check = check, project = project"
                .parse()
                .unwrap();
        assert2::assert!(
            transition_parts(
                &next_args,
                &function_signature(&header.parse().unwrap()).unwrap(),
                false
            )
            .is_err(),
            "{header}"
        );
    }

    #[yare::parameterized(
        missing_project = { "setup {} actions {}" },
        wrong_order = { "setup {} project {} actions {}" },
        non_brace_section = { "setup () actions {} project {}" },
        trailing_tokens = { "setup {} actions {} project {} extra" },
        missing_section_body = { "setup {} actions missing project {}" },
    )]
    fn section_parser_requires_the_declared_order_and_brace_groups(input: &str) {
        assert2::assert!(
            validate_sections(&input.parse().unwrap()).is_err(),
            "{input}"
        );
    }

    #[test]
    fn section_parser_accepts_opaque_brace_groups() {
        let valid = "setup { caller!(@opaque state); } actions { Action::Extra => return None, } project { &committed }".parse().unwrap();
        assert!(validate_sections(&valid).is_ok());
    }
}
