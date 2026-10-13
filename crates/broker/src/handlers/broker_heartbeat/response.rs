//! The `BrokerHeartbeatResponse` bodies the handler returns.
//!
//! Every response the handler sends comes from one of these builders, so the
//! default-valued fields stay in one place.

use krabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse;

pub(super) fn success_response(
    is_caught_up: bool,
    is_fenced: bool,
    should_shut_down: bool,
) -> BrokerHeartbeatResponse {
    BrokerHeartbeatResponse {
        is_caught_up,
        is_fenced,
        should_shut_down,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{codes, handlers::ErrorCodeResponse as _},
        assert2::assert,
    };

    #[test]
    fn heartbeat_response_builders_preserve_non_default_fields() {
        let expected_not_controller = krabka_macros::unthrottled_wire!(BrokerHeartbeatResponse {
            error_code: codes::NOT_CONTROLLER,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
        });
        assert!(BrokerHeartbeatResponse::error(codes::NOT_CONTROLLER) == expected_not_controller);

        let expected_success = krabka_macros::unthrottled_wire!(BrokerHeartbeatResponse {
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: true,
        });
        assert!(success_response(true, false, true) == expected_success);

        let expected_denied = krabka_macros::unthrottled_wire!(BrokerHeartbeatResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            is_caught_up: false,
            is_fenced: true,
            should_shut_down: false,
        });
        assert!(
            BrokerHeartbeatResponse::error(codes::CLUSTER_AUTHORIZATION_FAILED) == expected_denied
        );
    }
}
