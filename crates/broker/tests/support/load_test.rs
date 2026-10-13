//! Shared imports for broker load, soak, and durability integration tests.

#![allow(unused_imports)] // Each suite uses a different subset of this prelude.

pub(crate) use {
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    krabka_client_core::Client,
    krabka_client_producer::{Acks, Producer, ProducerRecord},
    krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest},
    std::time::{Duration, Instant},
};
