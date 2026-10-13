//! The periodic-injection task of the barrier coordinator.
//!
//! `Broker::start` spawns this task on every broker. On each tick it refreshes
//! the coordinator's leader-partition view, and then injects into every group
//! whose interval elapsed. A group runs only on the broker that coordinates it
//! now, so two brokers never inject the same epoch.
//!
//! A group with no interval injects only on demand, and the scheduler passes
//! over it.

use {
    crate::{
        barrier::coordinator::BarrierCoordinator, metadata_source::MetadataSource,
        task_util::run_every, time_util::now_ms,
    },
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info},
};

/// Entry point of the spawned task. It returns when `shutdown` is cancelled.
pub(crate) async fn run(
    coordinator: Arc<BarrierCoordinator>,
    controller: Arc<dyn MetadataSource>,
    shutdown: CancellationToken,
) {
    let tick = tokio::time::interval(coordinator.scheduler_tick().to_std());
    run_every(tick, &shutdown, || {
        inject_due(&coordinator, controller.as_ref())
    })
    .await;
    info!("barrier scheduler shutting down");
}

/// Run one tick. It refreshes the leader-partition view, and then injects into
/// every group that is due.
async fn inject_due(coordinator: &BarrierCoordinator, controller: &dyn MetadataSource) {
    let image = controller.current_image();
    coordinator.refresh_leader_partitions(&image).await;
    let injected = coordinator.run_due_injections(now_ms()).await;
    if injected.is_empty() {
        debug!("barrier scheduler: no group is due");
    } else {
        info!(count = injected.len(), "barrier scheduler: injected");
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::coordinator::test_support::{Fixture, GROUP, spec},
        krabka_units::millis,
    };

    /// A coordinator that holds one periodic group, and the metadata source it
    /// reads.
    struct PeriodicGroup {
        coordinator: BarrierCoordinator,
        controller: Arc<dyn MetadataSource>,
    }

    async fn periodic_group(fixture: &Fixture) -> PeriodicGroup {
        let coordinator = fixture.coordinator().await;
        let controller = Arc::clone(&fixture.source) as Arc<dyn MetadataSource>;

        coordinator
            .create_group(
                GROUP,
                spec()
                    .maybe_interval(Some(millis(1)))
                    .retained_cuts(5)
                    .call(),
            )
            .await
            .expect("create group");
        PeriodicGroup {
            coordinator,
            controller,
        }
    }

    #[tokio::test]
    async fn inject_due_injects_due_group() {
        let fixture = Fixture::new();
        let PeriodicGroup {
            coordinator,
            controller,
        } = periodic_group(&fixture).await;

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        let before = coordinator.list_cuts(GROUP).await.expect("list cuts");
        assert2::check!(before.is_empty());

        inject_due(&coordinator, controller.as_ref()).await;

        let after = coordinator.list_cuts(GROUP).await.expect("list cuts");
        assert2::check!(after.len() == 1);
    }

    #[tokio::test]
    async fn run_ticks_and_shuts_down_on_cancellation() {
        let fixture = Fixture::new();
        let PeriodicGroup {
            coordinator,
            controller,
        } = periodic_group(&fixture).await;
        let coordinator = Arc::new(coordinator);

        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            Arc::clone(&coordinator),
            Arc::clone(&controller),
            shutdown.clone(),
        ));

        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        assert2::check!(!task.is_finished());
        shutdown.cancel();
        task.await.expect("task completes on shutdown");
    }
}
