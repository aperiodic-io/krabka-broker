//! Independent wire and file-region fixtures shared by network tests and benchmarks.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn request_frame(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
            #[derive(Clone, Copy)]
            pub(crate) struct FrameCorrelationId(pub i32);
            #[derive(Clone, Copy)]
            pub(crate) struct RequestFrameSetup<'a> {
                pub api_key: ::krabka_ids::ApiKey,
                pub api_version: ::krabka_ids::ApiVersion,
                pub correlation_id: FrameCorrelationId,
                pub client_id: Option<&'a [u8]>,
                pub tagged: Option<&'a [u8]>,
                pub body: &'a [u8],
            }
            impl Default for RequestFrameSetup<'_> {
                fn default() -> Self {
                    Self { api_key: ::krabka_ids::ApiKey(18), api_version: ::krabka_ids::ApiVersion(0), correlation_id: FrameCorrelationId(1), client_id: None, tagged: None, body: &[] }
                }
            }
            pub(crate) fn {{ name }}(setup: RequestFrameSetup<'_>) -> ::bytes::BytesMut {
                let RequestFrameSetup { api_key, api_version, correlation_id, client_id, tagged, body } = setup;
                use ::bytes::BufMut as _;
                let mut buf = ::bytes::BytesMut::new();
                buf.put_i16(api_key.0); buf.put_i16(api_version.0); buf.put_i32(correlation_id.0);
                match client_id {
                    Some(id) => { buf.put_i16(i16::try_from(id.len()).expect("client id length")); buf.put_slice(id); }
                    None => buf.put_i16(-1),
                }
                if let Some(tagged) = tagged { buf.put_slice(tagged); }
                buf.put_slice(body);
                buf
            }
        }
    })
}

pub(crate) fn patterned_bytes(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
        #[derive(Clone, Copy)]
        pub(crate) struct PatternedPayloadLength(pub usize);
        fn {{ name }}(len: PatternedPayloadLength) -> ::bytes::Bytes {
            let len = len.0;
            ::bytes::Bytes::from((0..len).map(|b| u8::try_from(b % 251).expect("b % 251 fits in a byte")).collect::<Vec<u8>>())
        }
        }
    })
}

pub(crate) fn frame_prefix(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
        #[derive(Clone, Copy)]
        pub(crate) struct ResponseByteCount(pub usize);
        #[derive(Clone, Copy)]
        pub(crate) struct ResponseCorrelationId(pub i32);
        #[derive(Clone, Copy)]
        pub(crate) enum ResponseHeaderEncoding { Legacy, Flexible }
        impl ResponseHeaderEncoding {
            /// Adapt the observed response header version at the wire boundary.
            pub(crate) fn from_wire(header_v1: bool) -> Self {
                if header_v1 { Self::Flexible } else { Self::Legacy }
            }
        }
        #[derive(Clone, Copy)]
        pub(crate) struct ResponsePrefixSetup<'a> {
            pub header_len: ResponseByteCount,
            pub correlation_id: ResponseCorrelationId,
            pub header: ResponseHeaderEncoding,
            pub body_len: ResponseByteCount,
            pub capacity: ResponseByteCount,
            pub context: &'a str,
        }
        impl Default for ResponsePrefixSetup<'_> {
            fn default() -> Self {
                Self { header_len: ResponseByteCount(4), correlation_id: ResponseCorrelationId(1), header: ResponseHeaderEncoding::Legacy,
                    body_len: ResponseByteCount(0), capacity: ResponseByteCount(8), context: "response fits frame" }
            }
        }
        fn {{ name }}(setup: ResponsePrefixSetup<'_>) -> ::bytes::BytesMut {
            use ::bytes::BufMut as _;
            let mut prefix = ::bytes::BytesMut::with_capacity(setup.capacity.0);
            prefix.put_u32(u32::try_from(setup.header_len.0 + setup.body_len.0).expect(setup.context));
            prefix.put_i32(setup.correlation_id.0);
            if matches!(setup.header, ResponseHeaderEncoding::Flexible) { prefix.put_u8(0); }
            prefix
        }
        }
    })
}

pub(crate) fn file_region(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
        fn {{ name }}(region: &::krabka_protocol::records::FileRegion) -> Vec<u8> {
            use ::std::os::unix::fs::FileExt as _;
            let mut buf = vec![0u8; region.len];
            let mut filled = 0;
            let mut off = region.offset;
            while filled < buf.len() {
                let n = region.file.read_at(&mut buf[filled..], off).unwrap();
                ::assert2::assert!(n > 0);
                filled += n; off += n as u64;
            }
            buf
        }
        }
    })
}

/// The keyed fixture's ordinary non-transactional headers and exact offset arithmetic.
pub(crate) fn keyed_record_batch(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
        #[derive(Clone, Copy)]
        pub(crate) struct KeyedRecordCount(pub i32);
        #[derive(Clone, Copy)]
        pub(crate) struct KeyedValueSize(pub usize);
        #[derive(Clone, Copy)]
        pub(crate) struct KeyedBatchSetup {
            pub records: KeyedRecordCount,
            pub value_size: KeyedValueSize,
        }
        impl Default for KeyedBatchSetup {
            fn default() -> Self { Self { records: KeyedRecordCount(1), value_size: KeyedValueSize(64) } }
        }
        pub(crate) fn {{ name }}(setup: KeyedBatchSetup) -> ::krabka_protocol::records::RecordBatch {
            let n = setup.records.0;
            let value_size = setup.value_size.0;
            let mut batch = ::krabka_protocol::records::RecordBatch { last_offset_delta: n - 1, ..Default::default() };
            for offset_delta in 0..n {
                batch.records.push(::krabka_protocol::records::Record {
                    offset_delta,
                    key: Some(::bytes::Bytes::from(format!("k{offset_delta}"))),
                    value: Some(::bytes::Bytes::from(vec![b'x'; value_size])),
                    ..Default::default()
                });
            }
            batch
        }
        }
    })
}

/// Export the original paths and snapshot policy before constructing the caller's epoch bytes.
pub(crate) fn export_segment_data(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
        #[derive(Clone, Copy)]
        pub(crate) enum ProducerSnapshotExport { Include, Omit }
        pub(crate) fn {{ name }}(
            export: &::krabka_log::SegmentExport,
            snapshot: ProducerSnapshotExport,
            epoch_bytes: impl FnOnce() -> ::bytes::Bytes,
        ) -> ::krabka_remote_storage::LogSegmentData {
            ::krabka_remote_storage::LogSegmentData {
                log_segment: export.log_path.clone(),
                offset_index: export.offset_index_path.clone(),
                time_index: export.time_index_path.clone(),
                transaction_index: export.transaction_index_path.clone(),
                producer_snapshot_index: matches!(snapshot, ProducerSnapshotExport::Include).then(|| export.producer_snapshot_path.clone()),
                leader_epoch_index: epoch_bytes(),
            }
        }
        }
    })
}

/// Apply the shared file-region platform condition without rebuilding the caller's tokens.
pub(crate) fn sendfile_platform(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut output: proc_macro::TokenStream = crate::sendfile_match::platform_cfg().into();
    output.extend(input);
    output
}

/// The test authorizer's common method header, followed by its original compiler body group.
pub(crate) fn test_authorizer(
    input: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, ParseError> {
    use proc_macro::{Delimiter, Group, TokenTree};

    let tokens: Vec<_> = input.into_iter().collect();
    let [
        TokenTree::Ident(name),
        TokenTree::Punct(comma),
        TokenTree::Ident(request),
        TokenTree::Punct(semi),
        TokenTree::Group(body),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected authorizer type, request; { authorization body }",
        ));
    };
    if comma.as_char() != ',' || semi.as_char() != ';' || body.delimiter() != Delimiter::Brace {
        return Err(ParseError::new(
            body.span().into(),
            "expected authorizer bindings and a brace-delimited authorization body",
        ));
    }
    let name: moxy::token::Ident = name.clone().into();
    let request: moxy::token::Ident = request.clone().into();
    let mut method: proc_macro::TokenStream = moxy::template! {
        fn authorize(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            {{ request }}: &crate::authorizer::AuthorizationRequest<'_>,
        ) -> crate::authorizer::AuthorizationResult
    }
    .into();
    method.extend([TokenTree::Group(body.clone())]);
    let mut implementation: proc_macro::TokenStream = moxy::template! {
        impl crate::authorizer::Authorizer for {{ name }}
    }
    .into();
    implementation.extend([TokenTree::Group(Group::new(Delimiter::Brace, method))]);
    Ok(implementation)
}

/// A checkpoint-only fault with the caller's original unit type, visibility and error kind.
pub(crate) fn epoch_checkpoint_failure(input: TokenStream) -> Result<TokenStream, ParseError> {
    use moxy::{ast::Visibility, token::TokenTree};

    let tokens = input.into_inner();
    let [
        visibility @ ..,
        TokenTree::Ident(name),
        comma,
        TokenTree::Ident(kind),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected unit type and error-kind variant",
        ));
    };
    if !comma.is_punct_comma() {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected type, error-kind variant",
        ));
    }
    let visibility: TokenStream = visibility.into();
    let visibility = moxy::parse!({ visibility } as Visibility)?;
    Ok(moxy::template! {
        #[derive(Debug)]
        {{ visibility }} struct {{ name }};
        impl ::krabka_log::LogIo for {{ name }} {
            fn write_at(
                &self,
                target: ::krabka_log::IoTarget,
                file: &::std::fs::File,
                buf: &[u8],
            ) -> ::std::io::Result<usize> {
                use ::std::io::Write as _;
                if target == ::krabka_log::IoTarget::LeaderEpochCheckpoint {
                    return Err(::std::io::ErrorKind::{{ kind }}.into());
                }
                (&*file).write(buf)
            }
        }
    })
}

/// Supply the two partition-spawn wrappers' common parameters while retaining their native bodies.
pub(crate) fn partition_spawn_parameters(
    meta: TokenStream,
    item: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, ParseError> {
    use proc_macro::{Delimiter, Group, TokenTree};

    let target = match meta.into_inner().as_slice() {
        [] => TokenStream::new(),
        [moxy::token::TokenTree::Ident(name)] if name == "target" => {
            moxy::template! { replication_target: crate::partition::ReplicationTarget, }
        }
        _ => {
            return Err(ParseError::new(
                moxy::token::Span::call_site(),
                "expected no arguments or target",
            ));
        }
    };
    let mut tokens: Vec<_> = item.into_iter().collect();
    let Some(function) = tokens
        .iter()
        .position(|token| matches!(token, TokenTree::Ident(name) if name.to_string() == "fn"))
    else {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected a partition-spawn function",
        ));
    };
    let Some(TokenTree::Group(arguments)) = tokens.get_mut(function + 2) else {
        return Err(ParseError::new(
            moxy::token::Span::call_site(),
            "expected function parameters",
        ));
    };
    if arguments.delimiter() != Delimiter::Parenthesis || !arguments.stream().is_empty() {
        return Err(ParseError::new(
            arguments.span().into(),
            "partition_spawn_parameters needs an empty parameter list",
        ));
    }
    let parameters: proc_macro::TokenStream = moxy::template! {
        topic: String,
        {{ target }}
        partition_id: PartitionIndex,
        log_dir: std::path::PathBuf,
        log: krabka_log::Log,
        log_dir_status: crate::log_dir_status::LogDirRegistry,
        producer_state: Arc<crate::producer_state::ProducerState>,
        diskless: bool,
    }
    .into();
    let mut replacement = Group::new(Delimiter::Parenthesis, parameters);
    replacement.set_span(arguments.span());
    *arguments = replacement;
    Ok(tokens.into_iter().collect())
}

/// Stage checkpoint bytes under the JVM decoder's caller-chosen file name.
pub(crate) fn jvm_checkpoint_dump(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(dir: &::std::path::Path, filename: &str, bytes: &[u8], image: &str) -> ::std::process::Output {
            use ::std::io::Write as _;
            ::std::fs::File::create(dir.join(filename)).unwrap().write_all(bytes).unwrap();
            ::std::process::Command::new("docker")
                .args([
                    "run", "--rm", "-v", &format!("{}:/work", dir.display()),
                    image, "/opt/kafka/bin/kafka-dump-log.sh", "--cluster-metadata-decoder",
                    "--files", &format!("/work/{filename}"),
                ])
                .output().expect("docker run kafka-dump-log")
        }
    })
}

/// Compose a literal Java probe with the standard string-producer imports and properties.
pub(crate) fn java_string_producer(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [before, bootstrap, after]: [TokenStream; 3] = crate::meta::arguments(input, 3)?
        .try_into()
        .expect("three arguments");
    Ok(moxy::template! {
        concat!(
            "import java.util.Properties;\nimport org.apache.kafka.clients.producer.KafkaProducer;\nimport org.apache.kafka.clients.producer.ProducerConfig;\nimport org.apache.kafka.clients.producer.ProducerRecord;\n",
            {{ before }},
            "    Properties config = new Properties();\n    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ",
            {{ bootstrap }},
            r#");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
"#,
            {{ after }}
        )
    })
}

/// Define an epoch-millisecond reader with the caller's attributes and overflow fallback.
pub(crate) fn epoch_millis(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [signature, overflow]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    Ok(moxy::template! {
        {{ signature }}() -> i64 {
            use ::std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now().duration_since(UNIX_EPOCH)
                .map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or({{ overflow }}))
        }
    })
}

/// Literal bytes captured from Kafka 4.3.1's principal serializer.
/// Keep this oracle independent of the broker's principal encoder.
pub(crate) fn jvm_principal_golden(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (user, token) = crate::fixtures::named_root(input)?;
    let name = match user.to_string().as_str() {
        "alice" => moxy::template! { 0x06, b'a', b'l', b'i', b'c', b'e' },
        "bob" => moxy::template! { 0x04, b'b', b'o', b'b' },
        _ => {
            return Err(ParseError::new(
                moxy::token::Spanner::span(&user),
                "expected alice or bob JVM fixture",
            ));
        }
    };
    Ok(moxy::template! {
        &[0x00, 0x00, 0x05, b'U', b's', b'e', b'r', {{ name }}, if {{ token }} { 0x01 } else { 0x00 }, 0x00]
    })
}

/// The deliberately distinctive proposal UUID used by both signing golden fixtures.
pub(crate) fn break_glass_golden_id(input: TokenStream) -> Result<TokenStream, ParseError> {
    crate::fixtures::named_items(input, |name| {
        moxy::template! {
            const {{ name }}: [u8; 16] = [
                0x0B, 0xAD, 0xC0, 0xFF, 0xEE, 0x00, 0x40, 0x00, 0x80, 0x00, 0x01, 0x02,
                0x03, 0x04, 0x05, 0x06,
            ];
        }
    })
}
