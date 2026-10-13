//! The group-level `OffsetDelete` response.
//!
//! A group-level refusal carries only its top-level code. For the group ACL
//! denial, Kafka sends `OffsetDeleteRequest.getErrorResponse`; for a
//! coordinator error (routing, a missing group, `NON_EMPTY_GROUP`, a failed
//! tombstone write), the coordinator's `OffsetDeleteResponseData` holds only
//! the code, and `OffsetDeleteResponse.Builder.merge` lets it replace every
//! row the broker had built.

#[cfg(test)]
mod tests {
    use {
        crate::{codes, handlers::ErrorCodeResponse as _},
        assert2::check,
        krabka_protocol::owned::offset_delete_response::OffsetDeleteResponse,
    };

    #[test]
    fn whole_error_carries_only_the_top_level_code() {
        for code in [
            codes::GROUP_AUTHORIZATION_FAILED,
            codes::NOT_COORDINATOR,
            codes::GROUP_ID_NOT_FOUND,
        ] {
            let expected = unthrottled_wire!(OffsetDeleteResponse {
                error_code: code,
                topics: Vec::new(),
            });
            check!(OffsetDeleteResponse::error(code) == expected);
        }
    }
}
