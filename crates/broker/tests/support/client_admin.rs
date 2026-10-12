//! Imports shared by admin API tests that seed topics and produce records.

#![allow(unused_imports)] // Each admin suite uses a different subset of this prelude.

pub(crate) use super::{
    admin::{standalone_admin, topic_spec},
    producer::{ProducerRecordSetup, default_producer, producer_record},
};
