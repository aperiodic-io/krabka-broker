//! Shared imports for broker load, soak, and durability integration tests.

#![allow(unused_imports)] // Each suite uses a different subset of this prelude.

pub(crate) use std::time::{Duration, Instant};

pub(crate) use krabka_client_consumer::{AutoOffsetReset, Consumer};
pub(crate) use krabka_client_core::Client;
pub(crate) use krabka_client_producer::{Acks, Producer, ProducerRecord};
pub(crate) use krabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreateTopicsRequest,
};
