//! Full reconciliation method declarations with typed helpers and native operations.

use {
    crate::meta::{Tokens, bracketed_function, function_signature, is_plain_sync, parameter_ident},
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
    let parser = Parser::from_tokens(&value.0);
    let ident = Ident::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("expected one operation binding"));
    }
    Ok(ident)
}

fn path(value: &Tokens) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(&value.0);
    let path = Path::parse(&parser)?;
    if !parser.is_empty() || path.is_empty() {
        return Err(parser.error("expected one operation helper path"));
    }
    Ok(path)
}

fn metadata<T: FromMeta>(tokens: &NativeTokenStream) -> Result<T, ParseError> {
    let tokens: TokenStream = tokens.clone().into();
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

fn shell(
    input: NativeTokenStream,
    macro_name: &str,
) -> Result<(NativeTokenStream, NativeTokenStream, Group, Signature), ParseError> {
    let (header, metadata, body) = bracketed_function(input, macro_name)?;
    let mut item = header.clone();
    append_group(&mut item, NativeTokenStream::new());
    let signature = function_signature(&item.into())?;
    Ok((header, metadata, body, signature))
}

pub(crate) fn rebuild(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let (mut header, args, body, signature) = shell(input, "rebuild_reconciliation_group")?;
    if !is_plain_sync(&signature)
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
    let args: Rebuild = metadata(&args)?;
    let member = name(&args.member)?;
    let built = name(&args.built)?;
    let group = Ident::new("group").with_span(Span::mixed_site());
    let now = Ident::new("now").with_span(Span::mixed_site());
    let mut generated: NativeTokenStream = moxy::template! {
        let mut {{ group }} = GroupState::new("g");
        {{ group }}.group_epoch = {{ state }}.group_epoch;
        {{ group }}.record_metadata_hash({{ state }}.metadata_hash);
        {{ group }}.target.epoch = {{ state }}.target_epoch;
        let {{ now }} = Instant::now();
    }
    .into();
    let mut loop_body: NativeTokenStream = moxy::template! {
        let {{ built }} = modeled_member(&{{ member }}.id, {{ member }}.member_epoch, {{ member }}.assignment_state, &{{ member }}.assigned, &{{ member }}.pending_revocation, {{ now }});
        let {{ built }} =
    }.into();
    loop_body.extend([TokenTree::Group(body)]);
    loop_body.extend(NativeTokenStream::from(moxy::template! { ; insert_modeled_member(&mut {{ group }}, {{ built }}, &{{ member }}.target); }));
    generated.extend(NativeTokenStream::from(
        moxy::template! { for {{ member }} in &{{ state }}.members },
    ));
    append_group(&mut generated, loop_body);
    generated.extend(NativeTokenStream::from(moxy::template! { {{ group }} }));
    append_group(&mut header, generated);
    Ok(header)
}

pub(crate) fn actions(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let (mut header, args, body, signature) = shell(input, "reconciliation_actions")?;
    let this = receiver(&signature)?;
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
    let args: Actions = metadata(&args)?;
    let action = path(&args.action)?;
    let lookup = path(&args.lookup)?;
    let member = name(&args.member)?;
    let under_cap = Ident::new("under_cap").with_span(Span::mixed_site());
    let id = Ident::new("id").with_span(Span::mixed_site());
    let add = Ident::new("add").with_span(Span::mixed_site());
    let partition = Ident::new("partition").with_span(Span::mixed_site());
    let mut generated: NativeTokenStream = moxy::template! {
        let {{ under_cap }} = {{ state }}.group_epoch < {{ this }}.max_epoch;
        if {{ under_cap }} {
            for &{{ id }} in &{{ this }}.pool {
                if {{ lookup }}({{ state }}, {{ id }}).is_none() { {{ actions }}.push({{ action }}::Join({{ id }}.to_string())); }
            }
        }
    }.into();
    let mut per_member: NativeTokenStream = moxy::template! {
        if {{ under_cap }} {
            {{ actions }}.push({{ action }}::Leave({{ member }}.id.clone()));
            {{ actions }}.push({{ action }}::Heartbeat({{ member }}.id.clone()));
            {{ actions }}.push({{ action }}::Keepalive({{ member }}.id.clone()));
        }
        for ({{ add }}, {{ partition }}) in client_moves(&{{ state }}.advertised, &{{ state }}.client_owned, &{{ member }}.id) {
            {{ actions }}.push(if {{ add }} { {{ action }}::ClientAdd({{ member }}.id.clone(), {{ partition }}) } else { {{ action }}::ClientRevoke({{ member }}.id.clone(), {{ partition }}) });
        }
    }.into();
    per_member.extend([TokenTree::Group(body)]);
    generated.extend(NativeTokenStream::from(
        moxy::template! { for {{ member }} in &{{ state }}.members },
    ));
    append_group(&mut generated, per_member);
    append_group(&mut header, generated);
    Ok(header)
}

fn transition_sections(body: &Group) -> Result<(Group, Group, Group), ParseError> {
    let tokens: Vec<_> = body.stream().into_iter().collect();
    let [
        TokenTree::Ident(setup_name),
        TokenTree::Group(setup),
        TokenTree::Ident(actions_name),
        TokenTree::Group(actions),
        TokenTree::Ident(project_name),
        TokenTree::Group(project),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            body.span().into(),
            "transition body needs setup {}, actions {} and project {} native sections",
        ));
    };
    if setup_name.to_string() != "setup"
        || actions_name.to_string() != "actions"
        || project_name.to_string() != "project"
        || [setup, actions, project]
            .iter()
            .any(|group| group.delimiter() != Delimiter::Brace)
    {
        return Err(ParseError::new(
            body.span().into(),
            "expected ordered setup/actions/project blocks",
        ));
    }
    Ok((setup.clone(), actions.clone(), project.clone()))
}

pub(crate) fn next_state(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let (mut header, args, body, signature) = shell(input, "reconciliation_next_state")?;
    let this = receiver(&signature)?;
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
    let args: Transition = metadata(&args)?;
    let action = path(&args.action)?;
    let lookup = path(&args.lookup)?;
    let rebuild = path(&args.rebuild)?;
    let check = path(&args.check)?;
    let project = path(&args.project)?;
    let (setup, custom_actions, extra) = transition_sections(&body)?;
    let owned = Ident::new("owned").with_span(Span::mixed_site());
    let advertised = Ident::new("advertised").with_span(Span::mixed_site());
    let add = Ident::new("add").with_span(Span::mixed_site());
    let next = Ident::new("next").with_span(Span::mixed_site());
    let event = Ident::new("event").with_span(Span::mixed_site());
    let id = Ident::new("id").with_span(Span::mixed_site());
    let partition = Ident::new("partition").with_span(Span::mixed_site());
    let member = Ident::new("member").with_span(Span::mixed_site());
    let group = Ident::new("group").with_span(Span::mixed_site());
    let mut generated: NativeTokenStream = moxy::template! {
        let mut {{ owned }} = owned_map(&{{ last }}.client_owned);
        let mut {{ advertised }}: BTreeMap<_, _> = {{ last }}.advertised.iter().cloned().collect();
    }
    .into();
    generated.extend(setup.stream());
    generated.extend(NativeTokenStream::from(moxy::template! { let {{ add }} = matches!({{ action_value }}, {{ action }}::ClientAdd(..)); let {{ event }} = match {{ action_value }} }));
    let mut arms: NativeTokenStream = moxy::template! {
        {{ action }}::ClientAdd({{ id }}, {{ partition }}) | {{ action }}::ClientRevoke({{ id }}, {{ partition }}) => {
            let mut {{ next }} = {{ last }}.clone();
            {{ next }}.client_owned = apply_client_move(&{{ last }}.advertised, &mut {{ owned }}, {{ id }}, {{ partition }}, {{ add }})?;
            return Some({{ next }});
        },
    }.into();
    arms.extend(custom_actions.stream());
    arms.extend(NativeTokenStream::from(moxy::template! {
        {{ event }} => {{ event }}.member_heartbeat(|{{ id }}| {{ lookup }}({{ last }}, {{ id }}).map(|{{ member }}| {{ member }}.member_epoch))?,
    }));
    append_group(&mut generated, arms);
    generated.extend(NativeTokenStream::from(moxy::template! {
        ;
        let mut {{ group }} = {{ rebuild }}({{ last }});
        apply_member_heartbeat(&mut {{ group }}, &{{ this }}.metadata(), {{ event }}, &mut {{ owned }}, &mut {{ advertised }});
        {{ check }}({{ last }}, &{{ group }});
    }));
    let extra_value = Ident::new("projection_extra").with_span(Span::mixed_site());
    if !extra.stream().is_empty() {
        generated.extend(NativeTokenStream::from(
            moxy::template! { let {{ extra_value }} = },
        ));
        generated.extend([TokenTree::Group(extra.clone())]);
        generated.extend(NativeTokenStream::from(moxy::template! { ; }));
    }
    let has_extra = !extra.stream().is_empty();
    generated.extend(NativeTokenStream::from(moxy::template! {
        Some({{ project }}(&{{ group }}, &{{ owned }}, &{{ advertised }} @if has_extra { , {{ extra_value }} }))
    }));
    append_group(&mut header, generated);
    Ok(header)
}
