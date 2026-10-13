//! Exporter transport selection behind complete, ordinary method declarations.

use {
    crate::meta::is_plain_sync,
    moxy::{
        ast::{
            Attributes, FnParam, GenericArgument, Parse, ParseError, Parser, Path, PathArguments,
            Signature, Token, Visibility,
        },
        token::{Spanner, TokenStream},
    },
    proc_macro::{
        Delimiter, Group, TokenStream as NativeTokenStream, TokenTree as NativeTokenTree,
    },
};

fn exporter(input: &TokenStream) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(input);
    let _: Attributes = Parse::parse(&parser)?;
    let _: Visibility = Parse::parse(&parser)?;
    let method = Signature::parse(&parser)?;
    <Token![;]>::parse(&parser)?;
    let parameters = &method.params.inner.inputs;
    if !parser.is_empty()
        || !is_plain_sync(&method)
        || parameters.len() != 1
        || !matches!(parameters.first(), Some(FnParam::Receiver(receiver))
            if receiver.reference.is_some() && receiver.mutability.is_none() && receiver.lifetime.is_none())
    {
        return Err(ParseError::new(
            input.span(),
            "expected a synchronous exporter method taking only &self and ending with `;`",
        ));
    }
    let result = method
        .output
        .as_type()
        .and_then(|ty| ty.as_path())
        .ok_or_else(|| {
            ParseError::new(
                method.output.span(),
                "expected Result<SpanExporter or LogExporter, TelemetryError>",
            )
        })?;
    let Some(segment) = result
        .path
        .last()
        .filter(|_| result.path.is_ident("Result"))
    else {
        return Err(ParseError::new(
            result.span(),
            "expected Result<SpanExporter or LogExporter, TelemetryError>",
        ));
    };
    let PathArguments::AngleBracketed(arguments) = &segment.args else {
        return Err(ParseError::new(
            result.span(),
            "expected an exporter Result with two type arguments",
        ));
    };
    if arguments.args.len() != 2 {
        return Err(ParseError::new(
            result.span(),
            "expected an exporter Result with two type arguments",
        ));
    }
    let (Some(GenericArgument::Type(value)), Some(GenericArgument::Type(error))) =
        (arguments.args.first(), arguments.args.last())
    else {
        return Err(ParseError::new(
            result.span(),
            "expected exporter and error type paths",
        ));
    };
    let path = value
        .as_path()
        .filter(|path| path.qself.is_none())
        .ok_or_else(|| ParseError::new(value.span(), "expected SpanExporter or LogExporter"))?;
    if !path.path.last().is_some_and(|segment| {
        matches!(segment.ident.text(), "SpanExporter" | "LogExporter")
            && matches!(segment.args, PathArguments::None)
    }) || !error
        .as_path()
        .is_some_and(|path| path.path.is_ident("TelemetryError"))
    {
        return Err(ParseError::new(
            result.span(),
            "expected Result<SpanExporter or LogExporter, TelemetryError>",
        ));
    }
    Ok(path.path.clone())
}

pub(crate) fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let exporter = exporter(&input.clone().into())?;
    let body: NativeTokenStream = moxy::template! {
        let builder = {{ exporter }}::builder();
        let exporter = match self.protocol {
            OtlpProtocol::Grpc => self.configure_exporter(builder.with_tonic()).build()?,
            OtlpProtocol::HttpProtobuf => self
                .configure_exporter(builder.with_http().with_protocol(Protocol::HttpBinary))
                .build()?,
        };
        Ok(exporter)
    }
    .into();
    let mut declaration: Vec<_> = input.into_iter().collect();
    declaration.pop(); // The validated declaration ends with a semicolon.
    declaration.push(NativeTokenTree::Group(Group::new(Delimiter::Brace, body)));
    Ok(declaration.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use {super::exporter, assert2::assert};

    #[test]
    fn typed_declarations_select_both_exporter_paths_and_preserve_method_choices() {
        for target in ["SpanExporter", "LogExporter"] {
            let input = format!("#[doc = \"export\"] pub(crate) fn build(&self) -> Result<{target}, TelemetryError>;").parse().unwrap();
            assert!(exporter(&input).unwrap().is_ident(target));
        }
    }

    #[test]
    fn rejects_incompatible_receivers_outputs_and_extra_items() {
        for input in [
            "fn build(self) -> Result<SpanExporter, TelemetryError>;",
            "fn build(&mut self) -> Result<SpanExporter, TelemetryError>;",
            "async fn build(&self) -> Result<SpanExporter, TelemetryError>;",
            "fn build(&self, extra: bool) -> Result<SpanExporter, TelemetryError>;",
            "fn build(&self) -> SpanExporter;",
            "fn build(&self) -> Result<Other, TelemetryError>;",
            "fn build(&self) -> Result<SpanExporter, OtherError>;",
            "fn build(&self) -> Result<SpanExporter>;",
            "fn build(&self) -> Result<SpanExporter, TelemetryError> {}",
            "fn build(&self) -> Result<SpanExporter, TelemetryError>; fn extra();",
        ] {
            assert!(exporter(&input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
