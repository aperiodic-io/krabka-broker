//! The request helpers that the broker unit suites share.
//!
//! Creating a topic and resolving its id are two round trips that most of the
//! suites need before they can drive the behaviour they test, so both live
//! here instead of once per module.

use crate::support::{InProcess, client::create_topic as create_support_topic};

pub async fn create_topic(p: &InProcess, name: &str, num_partitions: i32) {
    create_support_topic(&p.client, name, num_partitions).await;
}

pub use crate::support::topic_id_for;
