//! PLAINTEXT wire helpers for the partition-reassignment suite: the client id
//! every request carries, and the `CreateTopics` call that provisions a topic
//! before a test drives a reassignment.
//!
//! The authorizer compatibility shim allows every request when there are no
//! `super_users` and no ACLs, so nothing here performs a SASL handshake.

/// The client id every request header in this suite carries.
pub const CLIENT_ID: &str = "krabka-reassign-test";
