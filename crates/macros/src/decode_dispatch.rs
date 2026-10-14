//! Decode one request before an ordinary raw dispatch function's native body.

use {
    crate::meta::{
        Tokens, edit_native_body, function_signature, parameter_ident, parenthesized_list,
    },
    moxy::{
        ast::{FromMeta, Meta, Parse, ParseError, Parser, PatIdent, Path},
        token::{Spanner, TokenStream},
    },
    proc_macro::{Delimiter, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

#[derive(moxy::FromMeta)]
struct DecodeArgs {
    request: Tokens,
    binding: RequestBinding,
}

struct RequestBinding {
    pattern: PatIdent,
}

impl FromMeta for RequestBinding {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let (tokens, _) = parenthesized_list(
            meta,
            "expected `binding(req)` or `binding(mut req)`",
            "request binding needs parentheses",
        )?;
        let parser = Parser::from_tokens(&tokens);
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
        Ok(Self { pattern: binding })
    }
}

fn prefix(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    let options: Meta = moxy::parse!({
        moxy::template! { decode_dispatch({{ meta }}) }
    })?;
    let args = DecodeArgs::from_meta(&options)?;
    let request_parser = Parser::from_tokens(&args.request.tokens);
    let request = Path::parse(&request_parser)?;
    if request.is_empty() || !request_parser.is_empty() {
        return Err(request_parser.error("expected a request type path"));
    }
    let binding = args.binding.pattern;

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
    use {super::prefix, crate::meta::compact};

    #[yare::parameterized(
        immutable = { "req" },
        mutable = { "mut req" },
    )]
    fn parses_request_paths_and_mutable_bindings_without_parsing_operations(binding: &str) {
        let item = "pub(crate) async fn dispatch(broker: &Broker, version: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult { unrelated::complex::<Type>(&value).await?; }".parse().unwrap();
        let meta = format!("request = module::Request, binding({binding})")
            .parse()
            .unwrap();
        let output = prefix(&meta, &item).unwrap();
        let output = compact(&output);
        assert2::assert!(output.contains("decode_request::<module::Request>(bytes,version)?"));
        assert2::assert!(output.starts_with(&format!("let{binding}=").replace(' ', "")));
    }

    #[yare::parameterized(
        missing_request = { "request = Request" },
        missing_binding = { "binding(req)" },
        invalid_request = { "request = 2, binding(req)" },
        reference_binding = { "request = Request, binding(ref req)" },
        reserved_binding = { "request = Request, binding(version)" },
        wrong_binding_syntax = { "request = Request, binding = req" },
        bracketed_binding = { "request = Request, binding[req]" },
        braced_binding = { "request = Request, binding{mut req}" },
        extra_binding = { "request = Request, binding(req, extra)" },
        expression_request = { "request = Request + Other, binding(req)" },
    )]
    fn rejects_invalid_request_metadata(meta: &str) {
        let item = "pub(crate) async fn dispatch(broker: &Broker, version: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult { unrelated::complex::<Type>(&value).await?; }".parse().unwrap();
        assert2::assert!(prefix(&meta.parse().unwrap(), &item).is_err(), "{meta}");
    }

    #[yare::parameterized(
        sync = {
            "fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {}",
        },
        wrong_name = {
            "async fn other(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {}",
        },
        wrong_arguments = { "async fn dispatch(b: &Broker) -> DispatchResult {}" },
        missing_result = {
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) {}",
        },
        missing_body = {
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult;",
        },
        trailing_item = {
            "async fn dispatch(b: &Broker, v: ApiVersion, bytes: &[u8], ctx: &Context) -> DispatchResult {} fn extra() {}",
        },
        non_function = { "struct Request {}" },
    )]
    fn refuses_incompatible_entry_points(item: &str) {
        let meta = "request = Request, binding(req)".parse().unwrap();
        assert2::assert!(prefix(&meta, &item.parse().unwrap()).is_err(), "{item}");
    }

    #[yare::parameterized(
        sync_without_correlation = { "", "" },
        sync_with_correlation = { "", "correlation: CorrelationId, " },
        async_without_correlation = { "async ", "" },
        async_with_correlation = { "async ", "correlation: CorrelationId, " },
    )]
    fn synchronous_and_async_handles_keep_the_correlation_slot(mode: &str, correlation: &str) {
        let meta = "request = Request, binding(req)".parse().unwrap();
        let item = format!("{mode}fn handle(b: &Broker, v: ApiVersion, {correlation}wire: &[u8], ctx: &Context) -> DispatchResult {{}}" ).parse().unwrap();
        let output = compact(&prefix(&meta, &item).unwrap());
        assert2::assert!(output.contains("decode_request::<Request>(wire,v)?"));
        assert2::assert!(!output.contains("correlation"));
    }
}
