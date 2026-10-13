//! SASL/PLAIN wire helpers for the authorization test.
//!
//! The deny test needs a principal that is not a super-user, so it runs against
//! a single-broker `SASL_PLAINTEXT` listener with `SimpleAclAuthorizer`
//! installed. This module holds that cluster boot and the
//! authenticated `CreateTopics` and `AlterPartitionReassignments` drivers.

/// Creates a topic over SASL/PLAIN as the given admin user. Copied from
/// `create_topic_sasl_plain` in `elect_leaders.rs`.
pub use crate::kafka_wire::create_topic_as_admin;
pub use crate::support::sasl::start_sasl_plaintext_with_acl_users as start_single_broker_sasl_plaintext_with_users;
use {
    crate::{
        kafka_wire::{self, PlainAuthenticationSetup},
        plaintext_wire::CLIENT_ID,
        reassign_rpc::alter_reassignments_on,
    },
    std::net::SocketAddr,
};

/// Drives `AlterPartitionReassignments` over a SASL/PLAIN authenticated
/// connection. It returns `(topic_name, [(partition_index, error_code)])`
/// rows.
pub async fn drive_alter_reassignments_sasl_plain(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (pass.as_bytes()).to_vec(),
    })
    .await
    .expect("SASL authenticate for AlterPartitionReassignments");
    alter_reassignments_on(&mut stream, rows).await
}
