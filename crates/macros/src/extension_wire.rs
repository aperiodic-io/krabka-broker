//! Adapt a typed extension operation to its raw wire entry point.

use {
    crate::meta::{compact, edit_native_body, function_signature},
    moxy::{
        ast::{FnParam, FromMeta, Meta, ParseError, PatIdent, PatType, Pattern},
        token::{LitStr, Spanner, TokenStream},
    },
    proc_macro::{Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree},
};

#[derive(moxy::FromMeta)]
struct Options {
    name: String,
    api: String,
    level: String,
}

struct Adapter {
    parameters: TokenStream,
    decode: TokenStream,
    tracing: TokenStream,
}

struct ParameterBinding<'a> {
    parameter: &'a PatType,
    binding: &'a PatIdent,
}

fn binding(parameter: &FnParam) -> Result<ParameterBinding<'_>, ParseError> {
    let FnParam::Typed(parameter) = parameter else {
        return Err(ParseError::new(
            parameter.span(),
            "extension parameters need typed identifier bindings",
        ));
    };
    let Pattern::Ident(binding) = parameter.pat.as_ref() else {
        return Err(ParseError::new(
            parameter.span(),
            "extension parameters need identifier bindings",
        ));
    };
    if !parameter.attrs.is_empty()
        || !binding.attrs.is_empty()
        || binding.by_ref.is_some()
        || binding.subpat.is_some()
        || matches!(binding.ident.text(), "req_bytes" | "_correlation_id")
    {
        return Err(ParseError::new(
            parameter.span(),
            "extension bindings must be plain names distinct from raw wire parameters",
        ));
    }
    Ok(ParameterBinding { parameter, binding })
}

fn adapter(meta: &TokenStream, item: &TokenStream) -> Result<Adapter, ParseError> {
    let metadata: Meta = moxy::parse!({
        moxy::template! { extension_wire({{ meta }}) }
    })?;
    let Options { name, api, level } = Options::from_meta(&metadata)?;
    if name.is_empty() || api.is_empty() {
        return Err(ParseError::new(
            metadata.span(),
            "extension span and API names must be nonempty",
        ));
    }
    if !matches!(level.as_str(), "info" | "debug") {
        return Err(ParseError::new(
            metadata.span(),
            "extension tracing level must be info or debug",
        ));
    }
    let name = LitStr::new(&name, metadata.span());
    let api = LitStr::new(&api, metadata.span());
    let level = LitStr::new(&level, metadata.span());
    let function = function_signature(item)?;
    if function.ident != "handle"
        || function.asyncness.is_none()
        || function.constness.is_some()
        || function.safety.is_some()
        || function.abi.is_some()
        || !function.generics.params.is_empty()
        || function.generics.where_clause.is_some()
        || function.params.inner.variadic.is_some()
        || !function
            .output
            .as_type()
            .and_then(|ty| ty.as_path())
            .is_some_and(|ty| ty.qself.is_none() && ty.path.is_ident("DispatchResult"))
    {
        return Err(ParseError::new(
            function.span(),
            "expected an ordinary async handle returning DispatchResult",
        ));
    }

    let parameters = function
        .params
        .inner
        .inputs
        .iter()
        .map(binding)
        .collect::<Result<Vec<_>, _>>()?;
    let [
        ParameterBinding {
            parameter: broker,
            binding: broker_name,
        },
        ParameterBinding {
            parameter: request,
            binding: request_name,
        },
        ParameterBinding {
            parameter: version,
            binding: version_name,
        },
        ParameterBinding {
            parameter: context,
            binding: context_name,
        },
    ] = parameters.as_slice()
    else {
        return Err(ParseError::new(
            function.span(),
            "expected broker, owned request, version and context parameters",
        ));
    };
    if compact(&moxy::template! { {{ broker.ty }} }) != "&Broker"
        || compact(&moxy::template! { {{ version.ty }} }) != "ApiVersion"
        || compact(&moxy::template! { {{ context.ty }} }) != "&RequestContext<'_>"
        || [broker_name, version_name, context_name]
            .iter()
            .any(|binding| binding.mutability.is_some())
        || !request
            .ty
            .as_path()
            .is_some_and(|ty| ty.qself.is_none() && !ty.path.is_empty())
    {
        return Err(ParseError::new(
            function.span(),
            "expected &Broker, an owned request type, ApiVersion and &RequestContext<'_>",
        ));
    }
    let names = [
        broker_name.ident.text(),
        request_name.ident.text(),
        version_name.ident.text(),
        context_name.ident.text(),
    ];
    if names
        .iter()
        .enumerate()
        .any(|(index, name)| names[index + 1..].contains(name))
    {
        return Err(ParseError::new(
            function.span(),
            "extension parameter names must be distinct",
        ));
    }
    Ok(Adapter {
        parameters: moxy::template! {
            {{ broker }}, {{ version }}, _correlation_id: i32, req_bytes: &[u8], {{ context }}
        },
        decode: moxy::template! {
            let {{ request.pat }} = crate::handlers::decode_request::<{{ request.ty }}>(req_bytes, {{ version_name.ident }})?;
        },
        tracing: moxy::template! {
            #[tracing::instrument(name = {{ name }}, level = {{ level }}, skip_all, fields(api = {{ api }}), err)]
        },
    })
}

pub(crate) fn expand(
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let Adapter {
        parameters,
        decode,
        tracing,
    } = adapter(meta, &item.clone().into())?;
    let decode: NativeTokenStream = decode.into();
    let item = edit_native_body(item, |body| decode.into_iter().chain(body).collect());
    let mut original: Vec<_> = item.into_iter().collect();
    let function = original
        .iter()
        .position(|token| matches!(token, NativeTokenTree::Ident(name) if name.to_string() == "fn"))
        .expect("validated extension function");
    let NativeTokenTree::Group(arguments) = &original[function + 2] else {
        unreachable!("validated parameters")
    };
    let mut replacement = Group::new(arguments.delimiter(), parameters.into());
    replacement.set_span(arguments.span());
    original[function + 2] = NativeTokenTree::Group(replacement);
    let mut output: NativeTokenStream = tracing.into();
    output.extend(original);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {super::adapter, crate::meta::compact};

    const HANDLER: &str = "pub(crate) async fn handle(source: &Broker, mut owned: Request, negotiated: ApiVersion, caller: &RequestContext<'_>) -> DispatchResult { if unrelated::<Type>(&value) { operation().await?; } }";
    const OPTIONS: &str = "name = \"handle_control\", api = \"Control\", level = \"info\"";

    #[yare::parameterized(
        info = { "info" },
        debug = { "debug" },
    )]
    fn infers_owned_request_and_preserves_bindings_for_both_span_levels(level: &str) {
        let meta = OPTIONS.replace("info", level).parse().unwrap();
        let output = adapter(&meta, &HANDLER.parse().unwrap()).unwrap();
        assert2::assert!(
            compact(&output.parameters)
                == "source:&Broker,negotiated:ApiVersion,_correlation_id:i32,req_bytes:&[u8],caller:&RequestContext<'_>"
        );
        assert2::assert!(
            compact(&output.decode)
                == "letmutowned=crate::handlers::decode_request::<Request>(req_bytes,negotiated)?;"
        );
        let trace = compact(&output.tracing);
        assert2::assert!(trace.contains("name=\"handle_control\""));
        assert2::assert!(trace.contains("api=\"Control\""));
        assert2::assert!(trace.contains(&format!("level=\"{level}\"")));
        assert2::assert!(trace.ends_with(",err)]"));
    }

    #[yare::parameterized(
        empty = { "" },
        missing_level = { "name = \"n\", api = \"a\"" },
        empty_name = { "name = \"\", api = \"a\", level = \"info\"" },
        empty_api = { "name = \"n\", api = \"\", level = \"info\"" },
        nonstring_name = { "name = 1, api = \"a\", level = \"info\"" },
        unsupported_level = { "name = \"n\", api = \"a\", level = \"trace\"" },
        extra_option = { "name = \"n\", api = \"a\", level = \"info\", extra = true" },
        duplicate_name = { "name = \"n\", name = \"other\", api = \"a\", level = \"info\"" },
    )]
    fn rejects_invalid_metadata(meta: &str) {
        let valid = HANDLER.parse().unwrap();
        assert2::assert!(adapter(&meta.parse().unwrap(), &valid).is_err(), "{meta}");
    }

    #[yare::parameterized(
        sync = { HANDLER.replace("async ", "") },
        wrong_name = { HANDLER.replace("handle", "other") },
        generic = { HANDLER.replace("handle(", "handle<T>(") },
        unsafe_fn = { HANDLER.replace("async fn", "async unsafe fn") },
        extern_fn = { HANDLER.replace("async fn", "async extern \"C\" fn") },
        const_fn = { HANDLER.replace("async fn", "const async fn") },
        wrong_result = { HANDLER.replace("DispatchResult", "Result<(), Error>") },
        borrowed_request = { HANDLER.replace("mut owned: Request", "owned: &Request") },
        mutable_broker = { HANDLER.replace("source: &Broker", "source: &mut Broker") },
        wrong_context = { HANDLER.replace("caller: &RequestContext<'_>", "caller: Context") },
        duplicate_wire_name = {
            HANDLER.replace("negotiated: ApiVersion", "req_bytes: ApiVersion"),
        },
        wrong_version = { HANDLER.replace("negotiated: ApiVersion", "negotiated: u16") },
        ref_pattern = { HANDLER.replace("mut owned", "ref owned") },
        duplicate_pattern = { HANDLER.replace("mut owned", "source") },
        missing_broker = { HANDLER.replace("source: &Broker, ", "") },
        receiver = { HANDLER.replace("source: &Broker", "&self") },
        mutable_source = { HANDLER.replace("source: &Broker", "mut source: &Broker") },
        destructured_request = { HANDLER.replace("mut owned", "(left, right)") },
        attributed_pattern = { HANDLER.replace("mut owned", "#[cfg(any())] owned") },
        where_clause = {
            HANDLER.replace("DispatchResult {", "DispatchResult where Request: Clone {"),
        },
        variadic = {
            HANDLER.replace("caller: &RequestContext<'_>", "caller: &RequestContext<'_>, ..."),
        },
        missing_body = { HANDLER.split('{').next().unwrap().to_owned() },
        trailing_item = { format!("{HANDLER} fn extra() {{}}") },
        non_function = { "struct Request {}".to_owned() },
    )]
    fn rejects_incompatible_headers(item: String) {
        let meta = OPTIONS.parse().unwrap();
        assert2::assert!(adapter(&meta, &item.parse().unwrap()).is_err(), "{item}");
    }
}
