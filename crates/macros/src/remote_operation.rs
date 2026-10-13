//! Shared tracing on ordinary remote-storage methods without recursive signature wrappers.

use {
    crate::meta::{ItemBody, compact, item_body, no_arguments},
    moxy::{
        ast::{FnParam, ItemFn, ParseError},
        token::{TokenStream, TokenTree},
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
    let ItemBody {
        tokens: mut header,
        body_index: body,
    } = item_body(item.clone(), "remote_operation", TokenTree::is_keyword_fn)?;
    if body + 1 != header.len() {
        return Err(ParseError::new(
            item.span(),
            "expected one remote-storage method",
        ));
    }
    let TokenTree::Group(group) = &mut header[body] else {
        unreachable!()
    };
    group.tokens = TokenStream::new();
    let method: ItemFn = moxy::parse!({ TokenStream::from(header) })?;
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
        assert2::assert,
        moxy::token::TokenStream,
    };

    #[test]
    fn every_operation_emits_its_existing_tracing_level_and_fields() {
        struct Case<'a> {
            method: &'a str,
            debug: bool,
            operation_fields: &'a [&'a str],
        }
        for case in [
            Case {
                method: "fn copy_log_segment_data(&self, metadata: &Metadata, data: &Data) -> Result<Option<CustomMetadata>, Error> {}",
                debug: false,
                operation_fields: &[
                    "start_offset=metadata.start_offset()",
                    "end_offset=metadata.end_offset()",
                ][..],
            },
            Case {
                method: "fn fetch_log_segment(&self, metadata: &Metadata, start_position: u32, end_position: Option<u32>) -> Result<Vec<u8>, Error> {}",
                debug: true,
                operation_fields: &[
                    "start_position=start_position",
                    "end_position=?end_position",
                ][..],
            },
            Case {
                method: "fn fetch_index(&self, metadata: &Metadata, index_type: IndexType) -> Result<Vec<u8>, Error> {}",
                debug: true,
                operation_fields: &["index_type=?index_type"][..],
            },
            Case {
                method: "fn delete_log_segment_data(&self, metadata: &Metadata) -> Result<(), Error> {}",
                debug: false,
                operation_fields: &[][..],
            },
        ] {
            let attribute =
                instrumentation(&TokenStream::new(), &case.method.parse().unwrap()).unwrap();
            let text = compact(&attribute);
            assert!(text.starts_with("#[tracing::instrument("));
            assert!(text.contains("level=\"debug\"") == case.debug);
            for field in [
                "topic_id=%metadata.remote_log_segment_id().topic_id_partition.topic_id",
                "partition=metadata.remote_log_segment_id().topic_id_partition.partition",
                "segment=%metadata.remote_log_segment_id().id",
            ]
            .into_iter()
            .chain(case.operation_fields.iter().copied())
            {
                assert!(text.contains(field));
            }
            assert!(text.contains("skip_all"));
            assert!(text.ends_with(",err)]"));
        }
    }

    #[test]
    fn refuses_arguments_and_methods_outside_the_fixed_spi() {
        let valid = "fn fetch_index(&self, metadata: &Metadata, index_type: IndexType) -> Result<Vec<u8>, Error> {}".parse().unwrap();
        assert!(instrumentation(&TokenStream::new(), &valid).is_ok());
        assert!(instrumentation(&"debug".parse().unwrap(), &valid).is_err());
        for invalid in [
            "fn other(&self, metadata: &Metadata) {}",
            "fn fetch_index(metadata: &Metadata, index_type: IndexType) {}",
            "fn fetch_index(&self, segment: &Metadata, index_type: IndexType) {}",
            "fn fetch_index(&self, metadata: &Metadata) {}",
            "struct Storage {}",
            "fn fetch_index(&self, metadata: &Metadata, index_type: IndexType) {} fn extra() {}",
        ] {
            assert!(instrumentation(&TokenStream::new(), &invalid.parse().unwrap()).is_err());
        }
    }
}
