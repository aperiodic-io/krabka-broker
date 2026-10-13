//! Public batch authorization keeps dispatch mode and input ownership.

use {
    assert2::assert,
    krabka_authz::{
        AclSource, AuthorizationRequest, AuthorizationResult, Authorizer, authorize_topics,
        authorize_topics_logged,
    },
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    krabka_security::{AuthMethod, Principal},
    std::sync::Mutex,
    uuid::Uuid,
};

#[derive(Debug, PartialEq, Eq)]
struct DispatchCall {
    logged: bool,
    resource: String,
}

#[derive(Debug, Default)]
struct DispatchProbe {
    calls: Mutex<Vec<DispatchCall>>,
}

impl DispatchProbe {
    fn record(&self, request: &AuthorizationRequest<'_>, logged: bool) -> AuthorizationResult {
        assert!(request.resource_type == ResourceType::Topic);
        assert!(request.operation == AclOperation::Write);
        assert!(request.principal.name == "batch" && request.host.ip().is_loopback());
        self.calls.lock().unwrap().push(DispatchCall {
            logged,
            resource: request.resource_name.to_owned(),
        });
        AuthorizationResult::Deny
    }
}

impl Authorizer for DispatchProbe {
    fn authorize(&self, _: &dyn AclSource, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        self.record(req, true)
    }

    fn authorize_quiet(
        &self,
        _: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        self.record(req, false)
    }
}

#[test]
fn batch_dispatch_keeps_quiet_checks_borrowed_keys_and_repeated_names() {
    let principal = Principal {
        name: "batch".into(),
        auth_method: AuthMethod::SaslPlain,
        groups: Vec::new(),
    };
    let source = MetadataImage::new(Uuid::nil());
    let host = "127.0.0.1:9092".parse().unwrap();
    let topics = ["first".to_owned(), "second".to_owned()];
    let names = [topics[0].as_str(), topics[0].as_str(), topics[1].as_str()];
    for log_denied in [None, Some(true), Some(false)] {
        let probe = DispatchProbe::default();
        let decisions = match log_denied {
            Some(flag) => authorize_topics_logged(
                &probe,
                &source,
                &principal,
                &host,
                AclOperation::Write,
                names,
                flag,
            ),
            None => authorize_topics(
                &probe,
                &source,
                &principal,
                &host,
                AclOperation::Write,
                names,
            ),
        };
        let logged = log_denied.unwrap_or(true);
        assert!(
            decisions.len() == 2
                && decisions
                    .values()
                    .all(|decision| *decision == AuthorizationResult::Deny)
        );
        let expected = ["first", "first", "second"].map(|resource| DispatchCall {
            logged,
            resource: resource.to_owned(),
        });
        assert!(probe.calls.lock().unwrap().as_slice() == expected.as_slice());
        for original in &topics {
            let (borrowed, _) = decisions.get_key_value(original.as_str()).unwrap();
            assert!(std::ptr::eq(borrowed.as_ptr(), original.as_ptr()));
        }
    }
}
