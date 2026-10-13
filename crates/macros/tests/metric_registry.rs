//! Direct registry expansion keeps caller bindings and the lock's original scope.

use {
    assert2::assert,
    prometheus_client::{metrics::counter::Counter, registry::Registry},
    std::sync::{Mutex, MutexGuard},
};

struct RegistryLock(Mutex<Registry>);

impl RegistryLock {
    fn lock(&self) -> std::future::Ready<MutexGuard<'_, Registry>> {
        std::future::ready(self.0.lock().unwrap())
    }
}

struct Metrics {
    registry: RegistryLock,
}

fn metrics() -> Metrics {
    let mut registry = Registry::default();
    let count = Counter::<u64>::default();
    count.inc_by(7);
    registry.register("fixture", "fixture counter", count);
    Metrics {
        registry: RegistryLock(Mutex::new(registry)),
    }
}

fn selected<const A: usize, const B: usize>(metrics: &Metrics) -> &Metrics {
    assert!(A + B == 3);
    metrics
}

#[tokio::test]
async fn unwrap_keeps_the_guard_until_the_caller_drops_it() {
    let metrics = metrics();
    krabka_macros::metric_registry_fixture!(metrics, rendered, guard; unwrap());
    assert!(rendered.contains("fixture_total 7"));
    assert!(metrics.registry.0.try_lock().is_err());
    drop(guard);
    assert!(metrics.registry.0.try_lock().is_ok());
}

#[tokio::test]
async fn expect_accepts_a_rust_expression_with_generic_arguments() {
    let metrics = metrics();
    krabka_macros::metric_registry_fixture!(
        selected::<1, 2>(&metrics), rendered, guard; expect("registry encodes")
    );
    assert!(rendered.contains("fixture_total 7"));
    drop(guard);
}
