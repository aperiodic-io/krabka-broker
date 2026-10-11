//! Construction of the per-user rows of the response.
//!
//! One place builds every row shape the handler can return: an accepted row,
//! a row that carries an error code and a message, and the rewrite that turns
//! the rows still marked accepted into a server error when the metadata
//! submit fails.

use krabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;

use crate::{codes, handlers::stamp_unset};

pub(super) fn ok_result(name: String) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: 0,
        error_message: None,
        ..Default::default()
    }
}

pub(super) fn err_result(name: String, code: i16, msg: &str) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: code,
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

pub(super) fn apply_submit_error(results: &mut [AlterUserScramCredentialsResult], msg: &str) {
    stamp_unset(results, codes::UNKNOWN_SERVER_ERROR, msg);
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers::alter_user_scram_credentials::test_support::expected_result;

    #[test]
    fn err_result_carries_code_and_message() {
        let r = err_result("alice".into(), codes::UNACCEPTABLE_CREDENTIAL, "bad");
        assert!(r == expected_result("alice", codes::UNACCEPTABLE_CREDENTIAL, Some("bad")));
    }

    #[test]
    fn ok_result_has_zero_error_code() {
        let r = ok_result("alice".into());
        assert!(r == expected_result("alice", 0, None));
    }

    #[test]
    fn submit_error_rewrites_only_success_rows() {
        let mut results = vec![
            ok_result("alice".into()),
            err_result(
                "bob".into(),
                codes::DUPLICATE_RESOURCE,
                "duplicate resource",
            ),
        ];

        apply_submit_error(&mut results, "submit failed: not controller");

        let expected = vec![
            expected_result(
                "alice",
                codes::UNKNOWN_SERVER_ERROR,
                Some("submit failed: not controller"),
            ),
            expected_result("bob", codes::DUPLICATE_RESOURCE, Some("duplicate resource")),
        ];
        assert!(results == expected);
    }
}
