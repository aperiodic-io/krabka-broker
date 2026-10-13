//! The per-request cluster grants of a controller-listener connection.
//!
//! Kafka's `ControllerApis` authorizes every request against the connection
//! principal. [`ControllerPeerGrants`] holds that principal and asks the
//! broker authorizer for each request, against the controller's current
//! metadata image, so an ACL change applies to the next request of an open
//! connection.

use {
    super::ControllerHandleArc,
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        handlers::{
            RequestContext, acl_wire::CLUSTER_RESOURCE_NAME,
            authorized_operations::authorized_operations_bits,
        },
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_raft::{ClusterGrants, ClusterOperation},
    std::sync::Arc,
};

/// The cluster grants of one controller-listener connection.
pub(super) struct ControllerPeerGrants {
    pub(super) authorizer: Arc<dyn Authorizer>,
    pub(super) controller: ControllerHandleArc,
    /// The SASL principal, the mTLS principal, or `ANONYMOUS`.
    pub(super) principal: krabka_security::Principal,
    pub(super) peer: std::net::SocketAddr,
}

impl ClusterGrants for ControllerPeerGrants {
    fn allows(&self, operation: ClusterOperation) -> bool {
        // The controller handle is late-bound. Before it is set the
        // controller cannot serve, so deny.
        let Some(controller) = self.controller.get() else {
            return false;
        };
        let operation = match operation {
            ClusterOperation::ClusterAction => AclOperation::ClusterAction,
            ClusterOperation::Alter => AclOperation::Alter,
            ClusterOperation::Describe => AclOperation::Describe,
        };
        let image = controller.current_image();
        self.authorizer.authorize(&*image, &AuthorizationRequest {
            principal: &self.principal,
            host: &self.peer,
            resource_type: ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation,
        }) == AuthorizationResult::Allow
    }

    fn cluster_authorized_operations(&self) -> i32 {
        // Before the controller handle is set the connection holds nothing.
        let Some(controller) = self.controller.get() else {
            return 0;
        };
        // The bitfield reads only the principal and peer of the context.
        let ctx = RequestContext::new(&self.principal, &self.peer, None, "", false, "");
        authorized_operations_bits(
            self.authorizer.as_ref(),
            &controller.current_image(),
            &ctx,
            ResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        )
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{GrantsInPrincipalName, peer, principal},
        assert2::check,
        tokio::sync::OnceCell,
    };

    /// Each cluster operation maps to the ACL operation of the same name, the
    /// principal and peer of the connection reach the authorizer, and a
    /// connection that arrives before the controller handle is set is denied.
    #[tokio::test]
    async fn grants_ask_the_authorizer_for_each_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let controller = Arc::new(
            krabka_raft::Controller::start(krabka_raft::ControllerConfig::for_tests(
                krabka_raft::NodeId(1),
                dir.path().to_path_buf(),
            ))
            .await
            .expect("controller"),
        );
        let bound: ControllerHandleArc = Arc::new(OnceCell::new());
        check!(bound.set(Arc::clone(&controller)).is_ok());
        let grants = |controller: &ControllerHandleArc, name: &str| ControllerPeerGrants {
            authorizer: Arc::new(GrantsInPrincipalName),
            controller: Arc::clone(controller),
            principal: principal(name),
            peer: peer(),
        };

        let operations = [
            ClusterOperation::ClusterAction,
            ClusterOperation::Alter,
            ClusterOperation::Describe,
        ];
        // The KIP-430 bitfield sets bit `code` of each allowed cluster
        // operation: ALTER is 7, DESCRIBE 8 and CLUSTER_ACTION 9.
        let cases = [
            ("none", [false, false, false], 0),
            ("Cluster:ClusterAction", [true, false, false], 1 << 9),
            ("Cluster:Alter", [false, true, false], 1 << 7),
            ("Cluster:Describe", [false, false, true], 1 << 8),
            (
                "Cluster:Alter+Cluster:Describe",
                [false, true, true],
                1 << 7 | 1 << 8,
            ),
            ("Topic:ClusterAction+Group:Alter", [false, false, false], 0),
        ];
        for (name, expected, bits) in cases {
            let connection = grants(&bound, name);
            check!(
                (
                    operations.map(|operation| connection.allows(operation)),
                    connection.cluster_authorized_operations(),
                ) == (expected, bits),
                "{name}"
            );
        }

        let unbound: ControllerHandleArc = Arc::new(OnceCell::new());
        let early = grants(
            &unbound,
            "Cluster:ClusterAction+Cluster:Alter+Cluster:Describe",
        );
        check!(operations.map(|operation| early.allows(operation)) == [false, false, false]);
        check!(early.cluster_authorized_operations() == 0);

        drop(bound);
        let controller = Arc::try_unwrap(controller)
            .unwrap_or_else(|_| panic!("controller handle still shared after the test"));
        controller.shutdown().await;
    }
}
