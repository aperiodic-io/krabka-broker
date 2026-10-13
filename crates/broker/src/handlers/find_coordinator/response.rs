//! Shaping the `FindCoordinatorResponse` from the resolved coordinator rows.
//!
//! The response has two forms on the wire. Versions 0 through 3 carry a single
//! coordinator in the top-level `node_id`, `host`, `port`, `error_code` and
//! `error_message` fields; version 4 and later carry the per-key `coordinators`
//! array. This module fills both from one row list, so a v0-v3 client reads the
//! first row out of the top-level fields while a v4+ client reads the array.
//!
//! The messages follow `KafkaApis`. A v4+ row that `getCoordinator` answers has
//! no message, whatever its code. A v0-v3 answer carries `Errors.message()` of
//! its code, which for `NONE` is the enum name `"NONE"`. A request that fails
//! as a whole goes through `FindCoordinatorRequest.getErrorResponse`, which
//! puts `Errors.message()` on the top-level answer or on every v4+ row.

use {
    crate::codes,
    krabka_protocol::owned::find_coordinator_response::{Coordinator, FindCoordinatorResponse},
};

/// Kafka's `Errors.message()` for the codes `FindCoordinator` answers.
pub(super) fn error_message(error_code: i16) -> Option<String> {
    let message = match error_code {
        codes::NONE => "NONE",
        codes::COORDINATOR_NOT_AVAILABLE => "The coordinator is not available.",
        codes::GROUP_AUTHORIZATION_FAILED => "Group authorization failed.",
        codes::CLUSTER_AUTHORIZATION_FAILED => "Cluster authorization failed.",
        codes::INVALID_REQUEST => {
            "This most likely occurs because of a request being malformed by the client library \
             or the message was sent to an incompatible broker. See the broker logs for more \
             details."
        }
        codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED => "Transactional Id authorization failed.",
        _ => return None,
    };
    Some(message.to_string())
}

/// A row with `Node.noNode()` in place of the coordinator.
pub(super) fn no_node_row(key: String, error_code: i16) -> Coordinator {
    Coordinator {
        key,
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code,
        error_message: None,
        ..Default::default()
    }
}

fn legacy_response(row: &Coordinator) -> FindCoordinatorResponse {
    // `handleFindCoordinatorRequestLessThanV4` answers an error through
    // `getErrorResponse`, which sends `Node.noNode()`.
    let (node_id, host, port) = if row.error_code == codes::NONE {
        (row.node_id, row.host.clone(), row.port)
    } else {
        (-1, String::new(), -1)
    };
    FindCoordinatorResponse {
        error_code: row.error_code,
        error_message: error_message(row.error_code),
        node_id,
        host,
        port,
        ..Default::default()
    }
}

/// The answer to a request whose keys each got a row.
pub(super) fn coordinators_response(
    version: i16,
    coordinators: Vec<Coordinator>,
) -> FindCoordinatorResponse {
    if version < 4 {
        let row = coordinators
            .first()
            .cloned()
            .unwrap_or_else(|| no_node_row(String::new(), codes::COORDINATOR_NOT_AVAILABLE));
        legacy_response(&row)
    } else {
        FindCoordinatorResponse {
            coordinators,
            ..Default::default()
        }
    }
}

/// The answer to a request that failed as a whole with `error_code`.
pub(super) fn request_error_response(
    version: i16,
    error_code: i16,
    keys: Vec<String>,
) -> FindCoordinatorResponse {
    if version < 4 {
        legacy_response(&no_node_row(String::new(), error_code))
    } else {
        FindCoordinatorResponse {
            coordinators: keys
                .into_iter()
                .map(|key| Coordinator {
                    error_message: error_message(error_code),
                    ..no_node_row(key, error_code)
                })
                .collect(),
            ..Default::default()
        }
    }
}
