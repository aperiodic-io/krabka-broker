//! The periodic heartbeat span that keeps the OTLP trace pipeline observable.
//!
//! The task emits one span at a fixed interval, under a synthetic remote parent
//! so that a ratio sampler cannot drop it. An operator can thus see that a
//! service still exports even when it serves no traffic.

use {
    krabka_units::prelude::{Time, TimeExt as _},
    opentelemetry::{
        Context, KeyValue,
        trace::{
            Span as _, SpanBuilder, SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId,
            TraceState, TracerProvider as _,
        },
    },
    opentelemetry_sdk::trace::SdkTracerProvider,
};

pub fn spawn_heartbeat_task(
    provider: SdkTracerProvider,
    service_name: String,
    service_instance_id: String,
    interval: Time,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let tracer = provider.tracer("krabka-telemetry-heartbeat");
        let mut sequence = 0_i64;
        loop {
            emit_heartbeat_span(&tracer, &service_name, &service_instance_id, sequence);
            sequence = sequence.saturating_add(1);
            tokio::time::sleep(interval.to_std()).await;
        }
    })
}

fn heartbeat_parent_context(sequence: i64) -> Context {
    let trace_sequence = u128::try_from(sequence).unwrap_or(0);
    let span_sequence = u64::try_from(sequence).unwrap_or(0);
    let trace_id =
        TraceId::from(0x6372_6162_6b61_5f68_6561_7274_0000_0001_u128.wrapping_add(trace_sequence));
    let span_id = SpanId::from(0x6865_6172_7400_0001_u64.wrapping_add(span_sequence));
    Context::new().with_remote_span_context(SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    ))
}

fn emit_heartbeat_span<T: opentelemetry::trace::Tracer>(
    tracer: &T,
    service_name: &str,
    service_instance_id: &str,
    sequence: i64,
) {
    let mut span = tracer.build_with_context(
        SpanBuilder::from_name("krabka.telemetry.heartbeat").with_attributes([
            KeyValue::new("krabka.telemetry.signal", "heartbeat"),
            KeyValue::new("krabka.telemetry.service_name", service_name.to_owned()),
            KeyValue::new(
                "krabka.telemetry.service_instance_id",
                service_instance_id.to_owned(),
            ),
            KeyValue::new("krabka.telemetry.sequence", sequence),
        ]),
        &heartbeat_parent_context(sequence),
    );
    span.end();
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        opentelemetry_sdk::{error::OTelSdkResult, trace::Sampler},
        std::sync::{Arc, Mutex},
    };

    #[derive(Clone, Debug, Default)]
    struct RecordingSpanExporter {
        spans: Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>,
    }

    impl RecordingSpanExporter {
        fn spans(&self) -> Vec<opentelemetry_sdk::trace::SpanData> {
            self.spans.lock().expect("recorded spans lock").clone()
        }
    }

    impl opentelemetry_sdk::trace::SpanExporter for RecordingSpanExporter {
        fn export(
            &self,
            batch: Vec<opentelemetry_sdk::trace::SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            self.spans
                .lock()
                .expect("recorded spans lock")
                .extend(batch);
            std::future::ready(Ok(()))
        }
    }

    #[test]
    fn heartbeat_span_is_sampled_when_ratio_sampler_would_drop_roots() {
        use {opentelemetry::trace::TraceFlags, opentelemetry_sdk::trace::SimpleSpanProcessor};

        let exporter = RecordingSpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                0.0,
            ))))
            .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
            .build();
        let tracer = provider.tracer("krabka-telemetry-test");

        emit_heartbeat_span(&tracer, "krabka-broker", "broker-1", 7);
        provider.force_flush().expect("flush spans");

        let spans = exporter.spans();
        assert2::assert!(spans.len() == 1);
        let span = &spans[0];
        assert2::assert!(span.name == "krabka.telemetry.heartbeat");
        check!(span.parent_span_is_remote);
        assert2::assert!(span.span_context.trace_flags() == TraceFlags::SAMPLED);
        check!(
            span.attributes
                .iter()
                .any(|attr| attr.key.as_str() == "krabka.telemetry.sequence"
                    && attr.value == opentelemetry::Value::I64(7))
        );
    }
}
