//! Shared tracing on ordinary remote-storage methods without recursive signature wrappers.

use {
    crate::meta::{compact, function_header, no_arguments},
    moxy::{
        ast::{FnParam, ParseError},
        token::TokenStream,
    },
    proc_macro::TokenStream as NativeTokenStream,
};

struct Instrumentation {
    debug: bool,
    parameters: &'static [&'static str],
    fields: TokenStream,
}

fn instrumentation(meta: &TokenStream, item: &TokenStream) -> Result<TokenStream, ParseError> {
    no_arguments(meta.clone(), "remote_operation")?;
    let method = function_header(
        item.clone(),
        "remote_operation",
        "expected one remote-storage method",
    )?;
    let Instrumentation {
        debug,
        parameters,
        fields,
    } = match method.sig.ident.text() {
        "copy_log_segment_data" => Instrumentation {
            debug: false,
            parameters: &["metadata", "data"],
            fields: moxy::template! { start_offset = metadata.start_offset(), end_offset = metadata.end_offset(), },
        },
        "fetch_log_segment" => Instrumentation {
            debug: true,
            parameters: &["metadata", "start_position", "end_position"],
            fields: moxy::template! { start_position = start_position, end_position = ?end_position, },
        },
        "fetch_index" => Instrumentation {
            debug: true,
            parameters: &["metadata", "index_type"],
            fields: moxy::template! { index_type = ?index_type, },
        },
        "delete_log_segment_data" => Instrumentation {
            debug: false,
            parameters: &["metadata"],
            fields: TokenStream::new(),
        },
        _ => {
            return Err(ParseError::new(
                method.sig.ident.span(),
                "expected a remote-storage operation method",
            ));
        }
    };
    let inputs = &method.sig.params.inner.inputs;
    if inputs.len() != parameters.len() + 1
        || !matches!(inputs.first(), Some(FnParam::Receiver(_)))
        || !inputs.iter().skip(1).zip(parameters).all(|(input, name)| {
            matches!(input, FnParam::Typed(parameter)
                if compact(&moxy::template! { {{ parameter.pat }} }) == *name)
        })
    {
        return Err(ParseError::new(
            method.sig.ident.span(),
            "remote-storage operation parameter names must match the SPI",
        ));
    }
    Ok(moxy::template! {
        #[tracing::instrument(
            @if debug { level = "debug", }
            skip_all,
            fields(
                topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
                partition = metadata.remote_log_segment_id().topic_id_partition.partition,
                segment = %metadata.remote_log_segment_id().id,
                {{ fields }}
            ),
            err
        )]
    })
}

pub(crate) fn expand(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let attribute: NativeTokenStream = instrumentation(&meta.into(), &item.clone().into())?.into();
    // Keep the original method, including asyncness, attributes, scopes and delimiter spans.
    Ok(attribute.into_iter().chain(item).collect())
}

#[cfg(test)]
mod tests {
    use {
        super::{compact, instrumentation},
        moxy::token::TokenStream,
    };

    #[yare::parameterized(
        copy = {
            "fn copy_log_segment_data(&self, metadata: &Metadata, data: &Data) -> Result<Option<CustomMetadata>, Error> {}",
            false,
            &["start_offset=metadata.start_offset()", "end_offset=metadata.end_offset()"][..]
        },
        fetch_segment = {
            "fn fetch_log_segment(&self, metadata: &Metadata, start_position: u32, end_position: Option<u32>) -> Result<Vec<u8>, Error> {}",
            true,
            &["start_position=start_position", "end_position=?end_position"][..]
        },
        fetch_index = {
            "fn fetch_index(&self, metadata: &Metadata, index_type: IndexType) -> Result<Vec<u8>, Error> {}",
            true,
            &["index_type=?index_type"][..]
        },
        delete = {
            "fn delete_log_segment_data(&self, metadata: &Metadata) -> Result<(), Error> {}",
            false,
            &[][..]
        },
    )]
    fn every_operation_emits_its_existing_tracing_level_and_fields(
        method: &str,
        debug: bool,
        operation_fields: &[&str],
    ) {
        let attribute = instrumentation(&TokenStream::new(), &method.parse().unwrap()).unwrap();
        let text = compact(&attribute);
        assert2::assert!(text.starts_with("#[tracing::instrument("));
        assert2::assert!(text.contains("level=\"debug\"") == debug);
        for field in [
            "topic_id=%metadata.remote_log_segment_id().topic_id_partition.topic_id",
            "partition=metadata.remote_log_segment_id().topic_id_partition.partition",
            "segment=%metadata.remote_log_segment_id().id",
        ]
        .into_iter()
        .chain(operation_fields.iter().copied())
        {
            assert2::assert!(text.contains(field));
        }
        assert2::assert!(text.contains("skip_all"));
        assert2::assert!(text.ends_with(",err)]"));
    }

    #[yare::parameterized(
        wrong_method = { "fn other(&self, metadata: &Metadata) {}" },
        missing_receiver = { "fn fetch_index(metadata: &Metadata, index_type: IndexType) {}" },
        wrong_argument = { "fn fetch_index(&self, segment: &Metadata, index_type: IndexType) {}" },
        missing_argument = { "fn fetch_index(&self, metadata: &Metadata) {}" },
        item = { "struct Storage {}" },
        extra_method = {
            "fn fetch_index(&self, metadata: &Metadata, index_type: IndexType) {} fn extra() {}",
        },
    )]
    fn refuses_arguments_and_methods_outside_the_fixed_spi(invalid: &str) {
        assert2::assert!(instrumentation(&TokenStream::new(), &invalid.parse().unwrap()).is_err());
    }
}
