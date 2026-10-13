//! `fetch.min.bytes` is a floor, at the wire.
//!
//! A Fetch whose `min_bytes` is above everything the log holds must be held
//! until `max_wait_ms` expires and must then answer with what is there. A
//! broker that treats the field as a hint answers on the first append instead,
//! which is a partial response per round trip for every consumer that raised
//! the floor -- Streams and Connect do.
//!
//! The same exchange runs against a pinned Apache Kafka broker in
//! `fetch_min_bytes_jvm`, which is where the claim that this is Kafka's
//! behavior and not just krabka's is checked. That half needs Docker and is
//! `#[ignore]`d; this one is hermetic.
//!
//! Cargo compiles this file as its own test binary, so the shared exchange
//! carries an explicit `#[path]` onto the sibling `fetch_min_bytes/`
//! directory; `support` is a `tests/<name>/mod.rs` helper, which the crate-root
//! rule already resolves.

mod support;
#[path = "fetch_min_bytes/wire.rs"]
mod wire;

use {
    crate::wire::{FetchFacts, HELD_AT_LEAST, min_bytes_exchange},
    assert2::assert,
};

#[tokio::test]
async fn a_fetch_below_its_min_bytes_is_held_until_max_wait_and_then_answers() {
    let broker = support::start().await;
    let bootstrap = broker.broker.listen_addr().to_string();

    let (held, facts) = min_bytes_exchange(&bootstrap, "fetch-min-bytes").await;

    assert!(held >= HELD_AT_LEAST);
    assert!(
        facts
            == FetchFacts {
                error_code: 0,
                high_watermark: 2,
                base_offsets: vec![0, 1],
            }
    );
}
