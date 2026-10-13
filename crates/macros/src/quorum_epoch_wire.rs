//! Shared Begin/End quorum wire shapes with visible request types and encoder APIs.

use {
    crate::meta::{
        BracketedFunction, Tokens, bracketed_function, function_signature, is_plain_sync,
        native_expression, parameter_ident, plain_path,
    },
    moxy::{
        ast::{
            Attributes, FromMeta, List, Meta, Parse, ParseError, Parser, Path, Signature, Token,
            Visibility,
        },
        token::{Ident, Span, Spanner, ToTokenStream, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
    std::collections::HashSet,
};

pub(crate) fn requests(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let attrs = Attributes::parse(&parser)?;
    let visibility = Visibility::parse(&parser)?;
    <Token![trait]>::parse(&parser)?;
    let contract = Ident::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    let targets = List::<Path, Token![,]>::parse_all(&parser)?;
    if targets.is_empty() {
        return Err(ParseError::new(
            input.span(),
            "declare at least one quorum epoch request type",
        ));
    }
    let mut seen = HashSet::new();
    for target in &targets {
        let tokens = target.to_token_stream();
        plain_path(
            &tokens,
            target.span(),
            "quorum epoch request needs a concrete type path",
        )?;
        if !seen.insert(tokens.to_string()) {
            return Err(ParseError::new(
                target.span(),
                "quorum epoch request type appears more than once",
            ));
        }
    }
    Ok(moxy::template! {
        {{ attrs }}
        {{ visibility }} trait {{ contract }} {
            fn cluster_id(&self) -> Option<&str>;
            fn topics(&self) -> impl ExactSizeIterator<Item = RequestedTopic<'_>>;
            fn leader(&self) -> Option<RequestedLeader>;
            fn endpoints(&self) -> impl Iterator<Item = ListenerAddress<'_>>;
        }
        @for target in &targets {
            impl {{ contract }} for {{ target }} {
                fn cluster_id(&self) -> Option<&str> { self.cluster_id.as_deref() }
                fn topics(&self) -> impl ExactSizeIterator<Item = RequestedTopic<'_>> {
                    self.topics.iter().map(|topic| RequestedTopic {
                        name: topic.topic_name.as_str(),
                        partitions: topic.partitions.iter().map(|partition| partition.partition_index).collect(),
                    })
                }
                fn leader(&self) -> Option<RequestedLeader> {
                    self.topics.first()?.partitions.first().map(|partition| RequestedLeader { id: partition.leader_id, epoch: partition.leader_epoch })
                }
                fn endpoints(&self) -> impl Iterator<Item = ListenerAddress<'_>> {
                    self.leader_endpoints.iter().map(|endpoint| ListenerAddress { name: endpoint.name.as_str(), host: endpoint.host.as_str(), port: endpoint.port })
                }
            }
        }
    })
}

#[derive(moxy::FromMeta)]
struct EncoderArgs {
    response: Tokens,
    wire: Tokens,
}

fn response_body(metadata: &TokenStream, signature: &Signature) -> Result<TokenStream, ParseError> {
    if !is_plain_sync(signature)
        || signature.params.inner.inputs.len() != 4
        || !signature
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.path.is_ident("Bytes"))
    {
        return Err(ParseError::new(
            signature.ident.span(),
            "quorum epoch encoder needs a synchronous four-parameter function returning Bytes",
        ));
    }
    let args: Meta = moxy::parse!({
        moxy::template! { quorum_epoch({{ metadata }}) }
    })?;
    let args = EncoderArgs::from_meta(&args)?;
    let response_type = plain_path(
        &args.response.tokens,
        args.response.tokens.span(),
        "quorum response needs a concrete type path",
    )?;
    let wire = plain_path(
        &args.wire.tokens,
        args.wire.tokens.span(),
        "quorum response wire module needs a plain path",
    )?;
    let params = &signature.params.inner.inputs;
    let top_error = parameter_ident(&params[0], "top-level error parameter needs a name")?;
    let partition_error = parameter_ident(&params[1], "partition error parameter needs a name")?;
    let leader = parameter_ident(&params[2], "leader parameter needs a name")?;
    let version = parameter_ident(&params[3], "version parameter needs a name")?;
    let response = Ident::new("response").with_span(Span::mixed_site());
    Ok(moxy::template! {
        let {{ response }} = if {{ top_error }} == 0 {
            {{ response_type }} {
                topics: vec![{{ wire }}::TopicData {
                    topic_name: METADATA_TOPIC.to_string(),
                    partitions: vec![{{ wire }}::PartitionData {
                        partition_index: METADATA_PARTITION,
                        error_code: {{ partition_error }},
                        leader_id: {{ leader }}.leader_id_to_wire(),
                        leader_epoch: epoch_to_wire({{ leader }}.epoch),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                node_endpoints: {{ leader }}.node_endpoint().map(|NodeEndpointEntry { node_id, host, port }| {{ wire }}::NodeEndpoint { node_id, host, port, ..Default::default() }).into_iter().collect(),
                ..Default::default()
            }
        } else {
            {{ response_type }} { error_code: {{ top_error }}, ..Default::default() }
        };
        encode_body(&{{ response }}, {{ version }})
    })
}

pub(crate) fn encoder(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let BracketedFunction {
        mut header,
        metadata,
        body,
    } = bracketed_function(input, "quorum_epoch_encoder")?;
    if !body.stream().is_empty() {
        return Err(ParseError::new(
            body.span().into(),
            "quorum epoch encoder declaration needs an empty body",
        ));
    }
    let mut parsed = header.clone();
    parsed.extend([TokenTree::Group(Group::new(
        Delimiter::Brace,
        NativeTokenStream::new(),
    ))]);
    let signature = function_signature(&parsed.into())?;
    let generated = response_body(&metadata.into(), &signature)?;
    let mut replacement = Group::new(Delimiter::Brace, generated.into());
    replacement.set_span(body.span());
    header.extend([TokenTree::Group(replacement)]);
    Ok(header)
}

pub(crate) fn voter_endpoints(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let mut tokens = input.into_iter();
    let Some(TokenTree::Group(source)) = tokens.next() else {
        return Err(ParseError::new(
            Span::call_site(),
            "voter endpoints need one native listener block",
        ));
    };
    if source.delimiter() != Delimiter::Brace || tokens.next().is_some() {
        return Err(ParseError::new(
            source.span().into(),
            "voter endpoints need one native listener block",
        ));
    }
    let listeners = Ident::new("listeners").with_span(Span::mixed_site());
    let mut body: NativeTokenStream = moxy::template! { let {{ listeners }} = }.into();
    body.extend([TokenTree::Group(source)]);
    body.extend(NativeTokenStream::from(moxy::template! {
        ; {{ listeners }}.into_iter().map(|listener| VoterEndpoint {
            name: listener.name,
            host: listener.host,
            port: listener.port,
        })
    }));
    Ok(native_expression(body))
}

#[cfg(test)]
mod tests {
    use {
        super::{requests, response_body},
        crate::meta::{compact, function_signature},
        assert2::assert,
    };

    #[test]
    fn request_list_keeps_order_and_rejects_nonconcrete_or_repeated_types() {
        let output =
            compact(&requests(&"trait QuorumEpochRequest; Begin, End".parse().unwrap()).unwrap());
        assert!(output.find("forBegin").unwrap() < output.find("forEnd").unwrap());
        for input in [
            "trait QuorumEpochRequest;",
            "trait QuorumEpochRequest; Begin, Begin",
            "trait QuorumEpochRequest; Begin<T>",
            "trait QuorumEpochRequest; Begin + End",
        ] {
            assert!(requests(&input.parse().unwrap()).is_err(), "{input}");
        }
    }

    #[test]
    fn encoder_requires_visible_signature_and_typed_metadata() {
        let signature = function_signature(&"pub fn encode(top: i16, partition: i16, leader: &QuorumLeader, version: i16) -> Bytes {}".parse().unwrap()).unwrap();
        let output = compact(
            &response_body(
                &"response = Response, wire = wire".parse().unwrap(),
                &signature,
            )
            .unwrap(),
        );
        assert!(output.contains("iftop==0"));
        assert!(output.contains("encode_body(&response,version)"));
        assert!(
            response_body(
                &"response = Response<T>, wire = wire".parse().unwrap(),
                &signature
            )
            .is_err()
        );
    }
}
