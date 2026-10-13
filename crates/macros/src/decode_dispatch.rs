//! Decode one request before an ordinary raw dispatch function's native body.

use {
    crate::meta::{Tokens, edit_native_body, function_signature, parameter_ident},
    moxy::{
        ast::{FromMeta, Meta, MetaContent, Parse, ParseError, Parser, PatIdent, Path},
        token::{Spanner, TokenStream},
    },
    proc_macro::{Delimiter, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

#[derive(moxy::FromMeta)]
struct DecodeArgs {
    request: Tokens,
    binding: RequestBinding,
}

struct RequestBinding(PatIdent);

impl FromMeta for RequestBinding {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let MetaContent::List(group) = &meta.content else {
            return Err(ParseError::new(
                meta.span(),
                "expected `binding(req)` or `binding(mut req)`",
            ));
        };
        if !group.delim.is_paren() {
            return Err(ParseError::new(
                group.span(),
                "request binding needs parentheses",
            ));
        }
        let parser = Parser::from_tokens(&group.tokens);
        let binding = PatIdent::parse(&parser)?;
        if !parser.is_empty()
            || binding.by_ref.is_some()
            || binding.subpat.is_some()
            || !binding.attrs.is_empty()
        {
            return Err(ParseError::new(
                binding.span(),
                "binding must be a name or `mut name`",
            ));
        }
        Ok(Self(binding))
    }
}

fn prefix(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    let options: Meta = moxy::parse!({
        moxy::template! { decode_dispatch({{ meta }}) }
    })?;
    let args = DecodeArgs::from_meta(&options)?;
    let request_parser = Parser::from_tokens(&args.request.0);
    let request = Path::parse(&request_parser)?;
    if request.is_empty() || !request_parser.is_empty() {
        return Err(request_parser.error("expected a request type path"));
    }
    let binding = args.binding.0;

    let function = function_signature(item)?;
    let parameters = &function.params.inner.inputs;
    let supported_entry = match function.ident.text() {
        "dispatch" => function.asyncness.is_some() && parameters.len() == 4,
        "handle" => matches!(parameters.len(), 4 | 5),
        _ => false,
    };
    if !supported_entry
        || function.safety.is_some()
        || function.abi.is_some()
        || !function.generics.params.is_empty()
        || !function
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.path.is_ident("DispatchResult"))
    {
        return Err(ParseError::new(
            function.span(),
            "expected async dispatch(broker, version, bytes, context), or handle(broker, version, [correlation,] bytes, context), returning DispatchResult",
        ));
    }
    let names = parameters
        .iter()
        .map(|parameter| parameter_ident(parameter, "dispatch parameters need identifier bindings"))
        .collect::<Result<Vec<_>, _>>()?;
    if names.iter().any(|name| name.text() == binding.ident.text()) {
        return Err(ParseError::new(
            binding.span(),
            "request binding must differ from dispatch parameters",
        ));
    }
    let version = names[1];
    let bytes = names[names.len() - 2];
    Ok(moxy::template! {
        let {{ binding }} = crate::handlers::decode_request::<{{ request }}>({{ bytes }}, {{ version }})?;
    })
}

pub(crate) fn expand(
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let decode: NativeTokenStream = prefix(meta, &item.clone().into())?.into();
    Ok(edit_native_body(item, |body| {
        decode.into_iter().chain(body).collect()
    }))
}

pub(crate) fn expand_function(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let mut tokens: Vec<_> = input.into_iter().collect();
    let Some(NativeTokenTree::Group(body)) = tokens.pop() else {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected a decoded dispatch body",
        ));
    };
    let Some(metadata_index) = tokens.iter().rposition(|token| {
        matches!(token, NativeTokenTree::Group(group) if group.delimiter() == Delimiter::Bracket)
    }) else {
        return Err(ParseError::new(
            body.span().into(),
            "expected [request = Type, binding(req)] after the parameters",
        ));
    };
    let NativeTokenTree::Group(metadata) = tokens.remove(metadata_index) else {
        unreachable!()
    };
    if metadata.delimiter() != Delimiter::Bracket || body.delimiter() != Delimiter::Brace {
        return Err(ParseError::new(
            metadata.span().into(),
            "expected bracketed decoded_dispatch metadata followed by a braced body",
        ));
    }
    tokens.push(NativeTokenTree::Group(body));
    expand(&metadata.stream().into(), tokens.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use {super::prefix, crate::meta::compact, assert2::assert};

    #[test]
    fn parses_request_paths_and_mutable_bindings_without_parsing_operations() {
        let item = "pub(crate) async fn dispatch(broker: &Broker, version: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult { unrelated::complex::<Type>(&value).await?; }".parse().unwrap();
        for binding in ["req", "mut req"] {
            let meta = format!("request = module::Request, binding({binding})")
                .parse()
                .unwrap();
            let output = prefix(&meta, &item).unwrap();
            let output = compact(&output);
            assert!(output.contains("decode_request::<module::Request>(bytes,version)?"));
            assert!(output.starts_with(&format!("let{binding}=").replace(' ', "")));
        }
        for meta in [
            "request = Request",
            "binding(req)",
            "request = 2, binding(req)",
            "request = Request, binding(ref req)",
            "request = Request, binding(version)",
            "request = Request, binding = req",
            "request = Request, binding[req]",
            "request = Request, binding{mut req}",
            "request = Request, binding(req, extra)",
            "request = Request + Other, binding(req)",
        ] {
            assert!(prefix(&meta.parse().unwrap(), &item).is_err(), "{meta}");
        }
    }

    #[test]
    fn refuses_incompatible_entry_points() {
        let meta = "request = Request, binding(req)".parse().unwrap();
        for item in [
            "fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {}",
            "async fn other(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {}",
            "async fn dispatch(b: &Broker) -> DispatchResult {}",
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) {}",
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult;",
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {} fn extra() {}",
            "struct Request {}",
        ] {
            assert!(prefix(&meta, &item.parse().unwrap()).is_err(), "{item}");
        }
    }

    #[test]
    fn synchronous_and_async_handles_keep_the_correlation_slot() {
        let meta = "request = Request, binding(req)".parse().unwrap();
        for mode in ["", "async "] {
            for correlation in ["", "correlation: CorrelationId, "] {
                let item = format!("{mode}fn handle(b: &Broker, v: ApiVersion, {correlation}wire: &[u8], ctx: &Context) -> DispatchResult {{}}" ).parse().unwrap();
                let output = compact(&prefix(&meta, &item).unwrap());
                assert!(output.contains("decode_request::<Request>(wire,v)?"));
                assert!(!output.contains("correlation"));
            }
        }
    }
}
