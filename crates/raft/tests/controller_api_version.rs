//! Request-derived controller ranges retain release gates and header framing.

use {
    assert2::assert,
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest, sasl_handshake_request::SaslHandshakeRequest,
        unregister_controller_request::UnregisterControllerRequest,
    },
    krabka_raft::{
        ControllerApiVersion,
        UnstableApiVersions::{Disabled, Enabled},
    },
};

#[test]
fn request_constructor_keeps_release_limits_trunk_absence_and_header_boundaries() {
    const RELEASED: ControllerApiVersion =
        ControllerApiVersion::for_request::<ApiVersionsRequest>();
    const TRUNK: ControllerApiVersion =
        ControllerApiVersion::for_request::<UnregisterControllerRequest>();
    const NONFLEXIBLE: ControllerApiVersion =
        ControllerApiVersion::for_request::<SaslHandshakeRequest>();

    assert!(RELEASED.api_key == 18 && RELEASED.min_version == 0 && RELEASED.flexible_min == 3);
    assert!(RELEASED.enabled_max(Disabled) == Some(4));
    assert!(RELEASED.enabled_max(Enabled) == Some(5));
    assert!(RELEASED.is_disabled_version(5, Disabled));
    assert!(!RELEASED.is_disabled_version(5, Enabled));

    assert!(TRUNK.api_key == 94 && TRUNK.flexible_min == 0);
    assert!(TRUNK.enabled_max(Disabled).is_none());
    assert!(TRUNK.enabled_max(Enabled) == Some(0));
    assert!(TRUNK.is_disabled_version(0, Disabled));
    assert!(!TRUNK.is_disabled_version(0, Enabled));

    assert!(NONFLEXIBLE.api_key == 17 && NONFLEXIBLE.min_version == 0);
    assert!(NONFLEXIBLE.enabled_max(Disabled) == Some(1));
    assert!(NONFLEXIBLE.flexible_min == i16::MAX);
}
