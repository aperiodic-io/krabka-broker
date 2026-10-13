//! Construction of the OTLP exporters from a resolved `OtlpConfig`.
//!
//! The span exporter, the log exporter, the OpenTelemetry `Resource`, and the
//! head sampler are all built here, so the transport details stay out of the
//! environment parsing and out of the subscriber install.

use {
    crate::{
        config::{OtlpConfig, OtlpProtocol},
        error::TelemetryError,
    },
    krabka_units::prelude::TimeExt as _,
    opentelemetry::KeyValue,
    opentelemetry_otlp::{LogExporter, Protocol, SpanExporter, WithExportConfig},
    opentelemetry_sdk::{Resource, trace::Sampler},
};

impl OtlpConfig {
    krabka_macros::otlp_exporter! {
        pub(crate) fn build_exporter(&self) -> Result<SpanExporter, TelemetryError>;
    }

    krabka_macros::otlp_exporter! {
        /// Build the OTLP **log** exporter.
        ///
        /// This function mirrors [`Self::build_exporter`], which builds the span
        /// exporter. Services can thus send their `tracing` logs over OTLP to the
        /// logs pipeline, and they do not depend on container-stdout tailing.
        pub(crate) fn build_log_exporter(&self) -> Result<LogExporter, TelemetryError>;
    }

    fn configure_exporter<B: WithExportConfig>(&self, builder: B) -> B {
        builder
            .with_endpoint(self.endpoint.clone())
            .with_timeout(self.timeout.to_std())
    }

    pub(crate) fn resource(&self) -> Resource {
        Resource::builder()
            .with_service_name(self.service_name.clone())
            .with_attributes([
                KeyValue::new("service.version", self.service_version.clone()),
                KeyValue::new("service.instance.id", self.service_instance_id.clone()),
            ])
            .build()
    }

    pub(crate) fn sampler(&self) -> Sampler {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.sample_ratio)))
    }
}
