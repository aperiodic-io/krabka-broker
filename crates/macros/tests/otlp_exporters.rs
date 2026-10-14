//! Both exporter types retain transport selection, configuration and build errors.

use std::marker::PhantomData;

#[derive(Clone, Copy)]
enum OtlpProtocol {
    Grpc,
    HttpProtobuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Grpc,
    HttpJson,
    HttpBinary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildError {
    Endpoint,
    Timeout,
}

#[derive(Debug, PartialEq, Eq)]
enum TelemetryError {
    Exporter(BuildError),
}

impl From<BuildError> for TelemetryError {
    fn from(error: BuildError) -> Self { Self::Exporter(error) }
}

#[derive(Debug, PartialEq, Eq)]
struct Built {
    endpoint: String,
    timeout: u64,
    protocol: Protocol,
}

struct Builder<T> {
    endpoint: String,
    timeout: u64,
    protocol: Option<Protocol>,
    output: PhantomData<T>,
}

impl<T> Builder<T> {
    fn new() -> Self {
        Self {
            endpoint: String::new(),
            timeout: 0,
            protocol: None,
            output: PhantomData,
        }
    }

    fn with_tonic(mut self) -> Self {
        self.protocol = Some(Protocol::Grpc);
        self
    }

    fn with_http(mut self) -> Self {
        self.protocol = Some(Protocol::HttpJson);
        self
    }

    fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = Some(protocol);
        self
    }

    fn build(self) -> Result<T, BuildError>
    where
        T: From<Built>,
    {
        if self.endpoint.is_empty() {
            return Err(BuildError::Endpoint);
        }
        if self.timeout == 0 {
            return Err(BuildError::Timeout);
        }
        Ok(T::from(Built {
            endpoint: self.endpoint,
            timeout: self.timeout,
            protocol: self
                .protocol
                .expect("exporter transport must be selected before build"),
        }))
    }
}

#[derive(Debug)]
struct SpanExporter {
    built: Built,
}

impl SpanExporter {
    fn builder() -> Builder<Self> { Builder::new() }
}

impl From<Built> for SpanExporter {
    fn from(built: Built) -> Self { Self { built } }
}

#[derive(Debug)]
struct LogExporter {
    built: Built,
}

impl LogExporter {
    fn builder() -> Builder<Self> { Builder::new() }
}

impl From<Built> for LogExporter {
    fn from(built: Built) -> Self { Self { built } }
}

struct OtlpConfig {
    protocol: OtlpProtocol,
    endpoint: String,
    timeout: u64,
}

impl OtlpConfig {
    krabka_macros::otlp_exporter! {
        pub(crate) fn build_exporter(&self) -> Result<SpanExporter, TelemetryError>;
    }

    krabka_macros::otlp_exporter! {
        /// Native method documentation and visibility belong to the declaration.
        #[must_use = "check the log exporter result"]
        pub(crate) fn build_log_exporter(&self) -> Result<LogExporter, TelemetryError>;
    }

    fn configure_exporter<T>(&self, mut builder: Builder<T>) -> Builder<T> {
        builder.endpoint.clone_from(&self.endpoint);
        builder.timeout = self.timeout;
        builder
    }
}

#[yare::parameterized(
    grpc_span = { OtlpProtocol::Grpc, Protocol::Grpc, false },
    grpc_log = { OtlpProtocol::Grpc, Protocol::Grpc, true },
    http_span = { OtlpProtocol::HttpProtobuf, Protocol::HttpBinary, false },
    http_log = { OtlpProtocol::HttpProtobuf, Protocol::HttpBinary, true },
)]
fn grpc_and_http_protobuf_configure_both_exporter_types(
    protocol: OtlpProtocol,
    expected: Protocol,
    log_exporter: bool,
) {
    let cfg = OtlpConfig {
        protocol,
        endpoint: "http://collector:4317".into(),
        timeout: 9,
    };
    let built = if log_exporter {
        cfg.build_log_exporter().unwrap().built
    } else {
        cfg.build_exporter().unwrap().built
    };
    assert2::assert!(
        built
            == Built {
                endpoint: cfg.endpoint.clone(),
                timeout: 9,
                protocol: expected
            }
    );
    assert2::assert!(cfg.endpoint == "http://collector:4317");
}

#[yare::parameterized(
    grpc_empty_endpoint = { OtlpProtocol::Grpc, "", 9, BuildError::Endpoint },
    grpc_timeout = { OtlpProtocol::Grpc, "http://collector:4317", 0, BuildError::Timeout },
    grpc_both_invalid = { OtlpProtocol::Grpc, "", 0, BuildError::Endpoint },
    http_empty_endpoint = { OtlpProtocol::HttpProtobuf, "", 9, BuildError::Endpoint },
    http_timeout = { OtlpProtocol::HttpProtobuf, "http://collector:4317", 0, BuildError::Timeout },
    http_both_invalid = { OtlpProtocol::HttpProtobuf, "", 0, BuildError::Endpoint },
)]
fn either_transport_propagates_the_sdk_error_through_both_methods(
    protocol: OtlpProtocol,
    endpoint: &str,
    timeout: u64,
    expected: BuildError,
) {
    let cfg = OtlpConfig {
        protocol,
        endpoint: endpoint.into(),
        timeout,
    };
    assert2::assert!(cfg.build_exporter().unwrap_err() == TelemetryError::Exporter(expected));
    assert2::assert!(cfg.build_log_exporter().unwrap_err() == TelemetryError::Exporter(expected));
}
