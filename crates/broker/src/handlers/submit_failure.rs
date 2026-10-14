//! The error code that a handler answers when a controller write fails.

use {crate::codes, krabka_raft::RaftError};

/// The error code for a `submit_change` that failed with `error`.
///
/// The controller leader refuses a break-glass consume or a topic-freeze
/// replacement with [`RaftError::UncommittedTail`] while its log holds records
/// that are not committed. A new leader is in that state until it commits a
/// record from its own epoch. Kafka's controller is not active in the same
/// window, and it answers `NOT_CONTROLLER`. A Kafka admin client treats that
/// code as retriable: it finds the active controller again and retries the
/// call. So this function answers `NOT_CONTROLLER` for that refusal.
///
/// Every other failure answers `otherwise`, the code that the handler uses for
/// a failed submit.
pub(crate) fn submit_failure_code(error: &RaftError, otherwise: i16) -> i16 {
    match error {
        RaftError::UncommittedTail => codes::NOT_CONTROLLER,
        _ => otherwise,
    }
}

#[cfg(test)]
mod tests {
    use {super::submit_failure_code, crate::codes, assert2::check, krabka_raft::RaftError};

    #[yare::parameterized(
        uncommitted_unknown_server_default = {
            RaftError::UncommittedTail,
            codes::UNKNOWN_SERVER_ERROR,
            codes::NOT_CONTROLLER,
        },
        uncommitted_coordinator_default = {
            RaftError::UncommittedTail,
            codes::COORDINATOR_NOT_AVAILABLE,
            codes::NOT_CONTROLLER,
        },
        refusal_keeps_default = {
            RaftError::ChangeRejected("stale".to_owned()),
            codes::UNKNOWN_SERVER_ERROR,
            codes::UNKNOWN_SERVER_ERROR,
        },
        shutdown_keeps_default = {
            RaftError::Shutdown,
            codes::COORDINATOR_NOT_AVAILABLE,
            codes::COORDINATOR_NOT_AVAILABLE,
        },
    )]
    fn an_uncommitted_controller_tail_answers_not_controller(
        error: RaftError,
        otherwise: i16,
        expected: i16,
    ) {
        check!(submit_failure_code(&error, otherwise) == expected);
    }
}
