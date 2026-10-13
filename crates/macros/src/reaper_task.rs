//! Cadence around an ordinary transaction sweep body, with caller-target shutdown logs.

use {
    crate::{
        meta::{compact, edit_native_body, function_signature, parameter_ident},
        sweep_loop::splice,
    },
    moxy::{
        ast::{FnParam, FromMeta, Meta, ParseError},
        token::{LitStr, Spanner, TokenStream},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

#[derive(moxy::FromMeta)]
struct Options {
    shutdown_message: String,
}

fn shutdown_message(meta: &TokenStream, item: &TokenStream) -> Result<LitStr, ParseError> {
    let metadata: Meta = moxy::parse!({
        moxy::template! { reaper_task({{ meta }}) }
    })?;
    let options = Options::from_meta(&metadata)?;
    if options.shutdown_message.is_empty() {
        return Err(ParseError::new(
            metadata.span(),
            "shutdown_message must be nonempty",
        ));
    }
    let signature = function_signature(item)?;
    if signature.asyncness.is_none()
        || signature.constness.is_some()
        || signature.safety.is_some()
        || signature.abi.is_some()
        || !signature.generics.params.is_empty()
        || signature.generics.where_clause.is_some()
        || signature.params.inner.variadic.is_some()
        || !signature.output.is_default()
    {
        return Err(ParseError::new(
            signature.span(),
            "expected an ordinary async reaper with no return type",
        ));
    }
    let parameters = &signature.params.inner.inputs;
    for (name, ty) in [("interval", "Time"), ("shutdown", "CancellationToken")] {
        if !parameters.iter().any(|parameter| parameter_ident(parameter, "expected a named reaper parameter").is_ok_and(|ident| ident.text() == name)
            && matches!(parameter, FnParam::Typed(parameter) if compact(&moxy::template! { {{ parameter.ty }} }) == ty)) {
            return Err(ParseError::new(signature.span(), format!("reaper requires {name}: {ty}")));
        }
    }
    Ok(LitStr::new(&options.shutdown_message, metadata.span()))
}

pub(crate) fn expand(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let message = shutdown_message(&meta.into(), &item.clone().into())?;
    let scaffold: NativeTokenStream = moxy::template! {
        use ::krabka_units::convert::TimeExt as _;
        let tick = ::tokio::time::interval(interval.to_std());
        crate::task_util::run_every(tick, &shutdown, || async { __reaper_body }).await;
        ::tracing::info!({{ message }});
    }
    .into();
    // The future still borrows the original immutable captures; no boxed future or inner function.
    Ok(edit_native_body(item, |body| {
        splice(
            scaffold,
            &[(
                "__reaper_body".to_owned(),
                Group::new(Delimiter::Brace, body),
            )],
            &["tick"],
        )
    }))
}

#[cfg(test)]
mod tests {
    use {super::shutdown_message, assert2::assert};

    #[test]
    fn requires_typed_cadence_shutdown_and_nonempty_message() {
        let item = "pub(crate) async fn run(coord: Arc<Coordinator>, interval: Time, shutdown: CancellationToken) { sweep(&coord).await; }".parse().unwrap();
        let metadata = "shutdown_message = \"reaper stopped\"".parse().unwrap();
        assert!(shutdown_message(&metadata, &item).unwrap().value() == "reaper stopped");
        for meta in [
            "",
            "shutdown_message = 1",
            "shutdown_message = \"\"",
            "unknown = \"message\"",
        ] {
            assert!(shutdown_message(&meta.parse().unwrap(), &item).is_err());
        }
        for invalid in [
            "fn run(interval: Time, shutdown: CancellationToken) {}",
            "async fn run(interval: Time) {}",
            "async fn run(interval: Other, shutdown: CancellationToken) {}",
            "async fn run(interval: Time, shutdown: CancellationToken) -> bool {}",
            "async fn run<T>(interval: Time, shutdown: CancellationToken) {}",
            "async fn run(interval: Time, shutdown: CancellationToken) {} fn extra() {}",
        ] {
            assert!(
                shutdown_message(&metadata, &invalid.parse().unwrap()).is_err(),
                "{invalid}"
            );
        }
    }
}
