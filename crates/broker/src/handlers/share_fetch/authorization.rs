//! The per-topic `Read` ACL gate that a `ShareFetch` row passes before it may
//! acquire anything.
//!
//! It is the only part of the handler that consults the authorizer per topic,
//! so it sits apart from the acquisition machinery.

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use crate::{
    broker::Broker,
    handlers::{RequestContext, acl_denied},
};

/// Reports whether the per-topic `Read` ACL denies this row.
pub(super) fn topic_read_denied(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    topic_name: &str,
) -> bool {
    acl_denied(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        ResourceType::Topic,
        topic_name,
        AclOperation::Read,
    )
}
