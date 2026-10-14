//! Coverage for the `support::relay` fault injector.
//!
//! `tests/support/` is helper code compiled into *every* integration-test
//! binary in this crate, so it cannot carry `#[test]` functions of its own —
//! they would run once per binary. The relay's own tests therefore live here, in
//! a dedicated suite whose upstream is a plain echo server rather than a broker:
//! what is under test is the forwarding and the cut, not anything Kafka.
//!
//! Every wait is bounded with `tokio::time::timeout`. A relay bug is usually a
//! read that never completes, and an unbounded wait would turn that into CI's
//! 600s kill, reported as TIMEOUT with no cause; a bounded one fails in seconds
//! and says which step hung.

use {
    assert2::{assert, check},
    std::{future::Future, io, net::SocketAddr, sync::Arc, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    },
};

mod support;

use support::relay::{Relay, SiteLink};

// Generous enough that a loaded runner does not fail a healthy relay, short
// enough that a broken one reports in seconds.
const TIMEOUT: Duration = Duration::from_secs(5);

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    tokio::time::timeout(TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {TIMEOUT:?}"))
}

// Bind a loopback echo server and return its address. Its tasks end when the
// test's runtime is dropped, which is the end of the test.
async fn start_echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind echo");
    let addr = listener.local_addr().expect("echo local addr");
    tokio::spawn(async move {
        while let Ok((mut stream, _peer)) = listener.accept().await {
            tokio::spawn(async move {
                let (mut read, mut write) = stream.split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });
    addr
}

async fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> io::Result<Vec<u8>> {
    stream.write_all(payload).await?;
    let mut echoed = vec![0u8; payload.len()];
    // `read_exact` reports a torn-down link as `UnexpectedEof` rather than a
    // short read, so both halves of "the link is gone" land in the `Err` arm.
    stream.read_exact(&mut echoed).await?;
    Ok(echoed)
}

// Connect through `addr` and echo `payload` back, as one fallible step.
//
// A cut relay may refuse at either point — the connect can fail outright, or it
// can complete and the first read then end at once, which is what the
// accept-and-drop cut produces. The caller cares only that the link does not
// carry data, so both failures are collapsed into one `Err`.
async fn probe(addr: SocketAddr, payload: &[u8]) -> io::Result<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await?;
    round_trip(&mut stream, payload).await
}

#[tokio::test]
async fn relay_forwards_bytes_to_the_upstream() {
    let upstream = start_echo().await;
    let relay = Relay::start(upstream).await;

    check!(
        relay.addr() != upstream,
        "the relay binds a port of its own"
    );
    let echoed = within("round trip", probe(relay.addr(), b"ping")).await;
    check!(echoed.expect("round trip through a healthy relay") == b"ping".to_vec());

    relay.shutdown().await;
}

// A relay to a fresh echo server, and a connection through it that has made
// one round trip with `payload`.
async fn relay_with_open_connection(payload: &[u8]) -> (Relay, TcpStream) {
    let relay = Relay::start(start_echo().await).await;
    let mut open = within("connect", TcpStream::connect(relay.addr()))
        .await
        .expect("connect through a healthy relay");
    let echoed = within(
        "round trip through the relay",
        round_trip(&mut open, payload),
    )
    .await;
    check!(echoed.expect("round trip through a healthy relay") == payload.to_vec());
    (relay, open)
}

// Cut `relay` and require that `open`, already connected, dies with it.
async fn cut_and_expect_closed(relay: &Relay, open: &mut TcpStream) {
    relay.cut();
    // The connection that was already open must die too. A cut that only
    // blocked new connections would leave a broker replicating over the socket
    // it already had, and the test would prove nothing.
    let after_cut = within("open connection after the cut", round_trip(open, b"x")).await;
    assert!(let Err(_) = &after_cut, "cut must tear down connections already open");
}

#[tokio::test]
async fn cut_tears_down_an_open_connection_and_stops_new_ones() {
    let (relay, mut open) = relay_with_open_connection(b"before").await;
    cut_and_expect_closed(&relay, &mut open).await;

    // And a fresh connection must fail rather than hang.
    let while_cut = within("fresh connection while cut", probe(relay.addr(), b"x")).await;
    assert!(let Err(_) = &while_cut, "cut must not carry a new connection");

    relay.shutdown().await;
}

#[tokio::test]
async fn healing_a_relay_that_is_not_cut_leaves_a_later_cut_complete() {
    let (relay, mut open) = relay_with_open_connection(b"before").await;

    // A `heal_all` over every link heals links that were never cut. That must
    // not move the open connection out of reach of the next cut.
    relay.heal();
    let healed = within("round trip after heal", round_trip(&mut open, b"still")).await;
    check!(healed.expect("round trip after a no-op heal") == b"still".to_vec());
    cut_and_expect_closed(&relay, &mut open).await;

    relay.shutdown().await;
}

#[tokio::test]
async fn heal_restores_forwarding_without_a_restart() {
    let upstream = start_echo().await;
    let relay = Relay::start(upstream).await;
    let addr = relay.addr();

    relay.cut();
    let while_cut = within("fresh connection while cut", probe(addr, b"x")).await;
    assert!(let Err(_) = &while_cut);

    relay.heal();

    // Same address, no restart: healing installs a new link generation rather
    // than rebinding, so nothing the brokers were told changes.
    check!(relay.addr() == addr);
    let healed = within("round trip after the heal", probe(addr, b"after")).await;
    check!(healed.expect("round trip after the heal") == b"after".to_vec());

    relay.shutdown().await;
}

#[tokio::test]
async fn shutdown_ends_the_relay_with_a_connection_still_open() {
    let upstream = start_echo().await;
    let relay = Relay::start(upstream).await;
    let addr = relay.addr();

    // Hold a live forwarded connection across the shutdown: it is the case that
    // would hang if `shutdown` awaited a copy task nothing had cancelled.
    let mut open = within("connect", TcpStream::connect(addr))
        .await
        .expect("connect through a healthy relay");
    let before = within("round trip", round_trip(&mut open, b"open")).await;
    check!(before.expect("round trip before the shutdown") == b"open".to_vec());

    within("shutdown", relay.shutdown()).await;

    let after = within("read after the shutdown", round_trip(&mut open, b"x")).await;
    assert!(let Err(_) = &after, "shutdown must tear down open connections");
    let reconnect = within("connect after the shutdown", probe(addr, b"x")).await;
    assert!(let Err(_) = &reconnect, "the listener must be gone once shutdown returns");
}

#[tokio::test]
async fn cut_and_heal_work_through_a_shared_relay() {
    // The partition tests hold the relay behind an `Arc` and cut it from a task
    // that is not the one that built it, which is why `cut`/`heal` take `&self`.
    let upstream = start_echo().await;
    let relay = Arc::new(Relay::start(upstream).await);
    let addr = relay.addr();

    let other = Arc::clone(&relay);
    tokio::spawn(async move { other.cut() })
        .await
        .expect("cut from another task");
    let while_cut = within("fresh connection while cut", probe(addr, b"x")).await;
    assert!(let Err(_) = &while_cut);

    let other = Arc::clone(&relay);
    tokio::spawn(async move { other.heal() })
        .await
        .expect("heal from another task");
    let healed = within("round trip after the heal", probe(addr, b"after")).await;
    check!(healed.expect("round trip after the heal") == b"after".to_vec());

    Arc::into_inner(relay)
        .expect("sole owner of the relay")
        .shutdown()
        .await;
}

#[tokio::test]
async fn site_link_cuts_and_heals_both_listeners_together() {
    let controller_upstream = start_echo().await;
    let client_upstream = start_echo().await;
    let site = SiteLink::start(controller_upstream, client_upstream).await;

    check!(site.controller_addr() != site.client_addr());
    let controller = site.controller_addr();
    let client = site.client_addr();

    check!(
        within("controller round trip", probe(controller, b"ctrl"))
            .await
            .expect("controller round trip")
            == b"ctrl".to_vec()
    );
    check!(
        within("client round trip", probe(client, b"data"))
            .await
            .expect("client round trip")
            == b"data".to_vec()
    );

    // A site is partitioned whole: a node that kept its data plane while losing
    // its controller plane is not a failure any deployment produces.
    site.cut();
    assert!(let Err(_) = &within("controller while cut", probe(controller, b"x")).await);
    assert!(let Err(_) = &within("client while cut", probe(client, b"x")).await);

    site.heal();
    check!(
        within("controller after the heal", probe(controller, b"ctrl"))
            .await
            .expect("controller round trip after the heal")
            == b"ctrl".to_vec()
    );
    check!(
        within("client after the heal", probe(client, b"data"))
            .await
            .expect("client round trip after the heal")
            == b"data".to_vec()
    );

    site.shutdown().await;
}
