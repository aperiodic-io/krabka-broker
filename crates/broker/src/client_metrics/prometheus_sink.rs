//! Prometheus sink for KIP-714 client metrics.
//!
//! Client metric *names* are dynamic, so this module registers a custom
//! `Collector` instead of static `Family` values. That collector renders a
//! live snapshot at scrape time, with stale points removed, as
//! `krabka_client_*` series.
//!
//! The point shapes and the ingest input type live in `point`, the live
//! snapshot and its ingest path in `collector`, and the scrape-time
//! rendering in `render`.

mod collector;
mod point;
mod render;

pub(crate) use self::{
    collector::ClientMetricsCollector,
    point::{DataPoint, PointValue},
    render::SharedClientMetricsCollector,
};

#[cfg(test)]
pub(super) mod test_support {
    use super::{DataPoint, PointValue};

    #[bon::builder]
    pub(super) fn data_point(
        metric: String,
        value: PointValue,
        #[builder(default = String::from("i"))] client_instance_id: String,
        #[builder(default = String::from("c"))] client_id: String,
        #[builder(default)] attributes: Vec<(String, String)>,
        delta_start: Option<u64>,
    ) -> DataPoint {
        DataPoint {
            metric,
            client_instance_id,
            client_id,
            attributes,
            value,
            delta_start,
        }
    }
}
