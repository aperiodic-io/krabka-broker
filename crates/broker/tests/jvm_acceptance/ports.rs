//! Per-process port allocation for the JVM acceptance suites.
//!
//! Each `jvm_acceptance_*` binary is its own process, so it allocates its own
//! listeners here instead of sharing the fixed 9092-9097 range. Two container
//! suites can then run at the same time without one losing the bind.

use crate::support::{JvmListeners, free_port, jvm_client_ports};

/// Ports for this test process, allocated once rather than fixed at 9092-9097.
///
/// This suite runs up to three brokers, each with a client and a controller
/// listener, plus `MinIO` for the tiered-storage cases. Fixed ports meant no two
/// container suites could run at once -- the second to start lost the bind and
/// reported `Address already in use` as a test failure.
///
/// Accessors return `&'static str` so ordinary use sites read as the constants
/// they replaced, and are named apart from the locals a format string binds.
pub(crate) struct Ports {
    client: [String; 3],
    advertised: [String; 3],
    controller: [String; 3],
    loopback: String,
    minio: u16,
}

pub(crate) fn ports() -> &'static Ports {
    static PORTS: std::sync::OnceLock<Ports> = std::sync::OnceLock::new();
    PORTS.get_or_init(|| {
        let (client, listen, advertised) = jvm_client_ports::<3>();
        let controller: [u16; 3] = std::array::from_fn(|_| free_port());
        Ports {
            client: listen,
            advertised,
            controller: controller.map(|p| format!("0.0.0.0:{p}")),
            loopback: format!("127.0.0.1:{}", client[0]),
            minio: free_port(),
        }
    })
}

pub(crate) fn broker0_advertised() -> &'static str { &ports().advertised[0] }

pub(crate) fn broker0_listen() -> &'static str { &ports().client[0] }

pub(crate) fn controller_addr_0() -> &'static str { &ports().controller[0] }

pub(crate) fn broker1_advertised() -> &'static str { &ports().advertised[1] }

pub(crate) fn broker1_listen() -> &'static str { &ports().client[1] }

pub(crate) fn controller_addr_1() -> &'static str { &ports().controller[1] }

pub(crate) fn broker2_advertised() -> &'static str { &ports().advertised[2] }

pub(crate) fn broker2_listen() -> &'static str { &ports().client[2] }

pub(crate) fn controller_addr_2() -> &'static str { &ports().controller[2] }

/// Broker 0 over loopback. The tests' own clients use this; only the containers
/// use the advertised `host.docker.internal` name.
pub(crate) fn rlmm_broker0_advertised() -> &'static str { &ports().loopback }

pub(crate) fn host_port() -> u16 {
    ports().client[0]
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .expect("client addr has a numeric port")
}

pub(crate) fn minio_port() -> u16 { ports().minio }

/// The first `N` broker endpoints allocated for this test process.
pub(crate) fn cluster_listeners<const N: usize>() -> [JvmListeners; N] {
    std::array::from_fn(|index| JvmListeners {
        listen: ports().client[index].clone(),
        advertised: ports().advertised[index].clone(),
        controller: ports().controller[index].clone(),
    })
}
