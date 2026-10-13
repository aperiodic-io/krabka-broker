//! Both exporter types retain transport selection, configuration and build errors.

use {assert2::assert, std::marker::PhantomData};

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
struct SpanExporter(Built);

impl SpanExporter {
    fn builder() -> Builder<Self> { Builder::new() }
}

impl From<Built> for SpanExporter {
    fn from(built: Built) -> Self { Self(built) }
}

#[derive(Debug)]
struct LogExporter(Built);

impl LogExporter {
    fn builder() -> Builder<Self> { Builder::new() }
}

impl From<Built> for LogExporter {
    fn from(built: Built) -> Self { Self(built) }
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

#[test]
fn grpc_and_http_protobuf_configure_both_exporter_types() {
    for (protocol, expected) in [
        (OtlpProtocol::Grpc, Protocol::Grpc),
        (OtlpProtocol::HttpProtobuf, Protocol::HttpBinary),
    ] {
        let cfg = OtlpConfig {
            protocol,
            endpoint: "http://collector:4317".into(),
            timeout: 9,
        };
        for built in [
            cfg.build_exporter().unwrap().0,
            cfg.build_log_exporter().unwrap().0,
        ] {
            assert!(
                built
                    == Built {
                        endpoint: cfg.endpoint.clone(),
                        timeout: 9,
                        protocol: expected
                    }
            );
        }
        assert!(cfg.endpoint == "http://collector:4317");
    }
}

#[test]
fn either_transport_propagates_the_sdk_error_through_both_methods() {
    for protocol in [OtlpProtocol::Grpc, OtlpProtocol::HttpProtobuf] {
        for (endpoint, timeout, expected) in [
            ("", 9, BuildError::Endpoint),
            ("http://collector:4317", 0, BuildError::Timeout),
            ("", 0, BuildError::Endpoint),
        ] {
            let cfg = OtlpConfig {
                protocol,
                endpoint: endpoint.into(),
                timeout,
            };
            let errors = [
                cfg.build_exporter().unwrap_err(),
                cfg.build_log_exporter().unwrap_err(),
            ];
            for error in errors {
                assert!(error == TelemetryError::Exporter(expected));
            }
        }
    }
}
