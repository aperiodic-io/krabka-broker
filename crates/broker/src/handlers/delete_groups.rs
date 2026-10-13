//! `DeleteGroups` (`api_key=42`).
//!
//! This handler deletes empty groups of every kind, tombstoning their offsets
//! and group records, and rejects a group with members with
//! `NON_EMPTY_GROUP`. As in Kafka's `KafkaApis.handleDeleteGroupsRequest`, a
//! duplicate id is answered once, and the rows of the groups the principal may
//! not delete follow the coordinator's rows.

use {
    crate::{
        broker::Broker,
        codes,
        coordinator::DeleteGroupError,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, acl_denied, group_coordinator_error},
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        delete_groups_request::DeleteGroupsRequest,
        delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse},
    },
    std::collections::HashSet,
};

// cargo-mutants: coordinator-backed request orchestration; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DeleteGroupsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DeleteGroupsResponse, BrokerError> {
    // Kafka's `handleDeleteGroupsRequest` drops duplicate ids first
    // (`groupsNames.distinct`), keeping the first-seen order, and then
    // partitions the ids by the `Delete` grant on each group.
    let mut seen = HashSet::with_capacity(req.groups_names.len());
    let groups: Vec<String> = req
        .groups_names
        .into_iter()
        .filter(|gid| seen.insert(gid.clone()))
        .collect();
    let (authorized, denied): (Vec<String>, Vec<String>) = {
        let image = broker.controller.current_image();
        groups.into_iter().partition(|gid| {
            !acl_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                ResourceType::Group,
                gid.as_str(),
                AclOperation::Delete,
            )
        })
    };

    // The coordinator's results come first, and a `GROUP_AUTHORIZATION_FAILED`
    // row for each denied group follows them.
    let mut results: Vec<DeletableGroupResult> =
        Vec::with_capacity(authorized.len() + denied.len());
    for gid in authorized {
        let error_code = delete_one(broker, &gid).await;
        results.push(DeletableGroupResult {
            group_id: gid,
            error_code,
            ..Default::default()
        });
    }
    results.extend(denied.into_iter().map(|gid| DeletableGroupResult {
        group_id: gid,
        error_code: codes::GROUP_AUTHORIZATION_FAILED,
        ..Default::default()
    }));

    let resp = DeleteGroupsResponse {
        results,
        ..Default::default()
    };
    Ok(resp)
}

/// Deletes one authorized group and returns its result's error code.
async fn delete_one(broker: &Broker, group_id: &str) -> i16 {
    if let Some(error_code) = group_coordinator_error(broker, group_id) {
        return error_code;
    }
    match broker.group_coordinator.delete_group(group_id).await {
        Ok(()) => codes::NONE,
        Err(DeleteGroupError::NotFound) => codes::GROUP_ID_NOT_FOUND,
        Err(DeleteGroupError::NonEmpty) => codes::NON_EMPTY_GROUP,
        Err(DeleteGroupError::ShareState(error_code)) => error_code,
        Err(DeleteGroupError::Internal) => codes::UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::AuthorizationResult,
            handlers::RequestContext,
            test_support::{
                DenyAll, dispatch_context, peer, principal, start_group_broker_no_audit, test_ctx,
            },
        },
        assert2::assert,
        std::sync::Arc,
    };

    const VERSION: i16 = 2;

    fn request(groups: &[&str]) -> DeleteGroupsRequest {
        DeleteGroupsRequest {
            groups_names: groups.iter().map(|g| (*g).into()).collect(),
            ..Default::default()
        }
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    async fn drive(
        broker: &Broker,
        req: &DeleteGroupsRequest,
        ctx: &RequestContext<'_>,
    ) -> DeleteGroupsResponse {
        handle(broker, req.clone(), VERSION, ctx)
            .await
            .expect("handle")
    }

    fn result(group_id: &str, error_code: i16) -> DeletableGroupResult {
        krabka_macros::tagged_wire!(DeletableGroupResult {
            group_id: group_id.to_owned(),
            error_code,
            error_message: None,
        })
    }

    async fn assert_results(
        broker: &Broker,
        ctx: &RequestContext<'_>,
        groups: &[&str],
        results: Vec<DeletableGroupResult>,
    ) {
        let response = drive(broker, &request(groups), ctx).await;
        let expected = krabka_macros::unthrottled_wire!(DeleteGroupsResponse { results });
        assert!(response == expected);
    }

    #[tokio::test]
    async fn handle_denies_delete_for_each_group() {
        let (broker_handle, _dir) = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((p, peer), principal("alice"));
        let ctx = test_context(&p, &peer);
        assert_results(&broker, &ctx, &["group-a", "group-b"], vec![
            result("group-a", codes::GROUP_AUTHORIZATION_FAILED),
            result("group-b", codes::GROUP_AUTHORIZATION_FAILED),
        ])
        .await;
        broker_handle.shutdown().await;
    }

    /// Denies every operation on the group named `denied` and allows the
    /// rest.
    #[derive(Debug)]
    struct DenyGroupNamedDenied;

    test_authorizer!(DenyGroupNamedDenied, (self, _source, req), {
        if req.resource_name == "denied" {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    });

    /// Kafka answers a duplicate group id once, and appends the
    /// `GROUP_AUTHORIZATION_FAILED` rows after the coordinator's rows.
    #[tokio::test]
    async fn handle_deduplicates_ids_and_appends_denied_rows() {
        /// A group id and the error code its result row carries.
        struct GroupRow {
            group_id: &'static str,
            error_code: i16,
        }

        let (broker_handle, _dir) =
            start_group_broker_no_audit(Arc::new(DenyGroupNamedDenied)).await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((p, peer), principal("alice"));
        let response = |rows: &[GroupRow]| DeleteGroupsResponse {
            results: rows
                .iter()
                .map(|row| DeletableGroupResult {
                    group_id: row.group_id.to_string(),
                    error_code: row.error_code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let cases = [
            (
                &["denied", "missing"][..],
                response(&[
                    GroupRow {
                        group_id: "missing",
                        error_code: codes::GROUP_ID_NOT_FOUND,
                    },
                    GroupRow {
                        group_id: "denied",
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    },
                ]),
            ),
            (
                &["missing", "missing"][..],
                response(&[GroupRow {
                    group_id: "missing",
                    error_code: codes::GROUP_ID_NOT_FOUND,
                }]),
            ),
            (
                &["denied", "denied"][..],
                response(&[GroupRow {
                    group_id: "denied",
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                }]),
            ),
            (
                &["denied", "a", "denied", "b", "a"][..],
                response(&[
                    GroupRow {
                        group_id: "a",
                        error_code: codes::GROUP_ID_NOT_FOUND,
                    },
                    GroupRow {
                        group_id: "b",
                        error_code: codes::GROUP_ID_NOT_FOUND,
                    },
                    GroupRow {
                        group_id: "denied",
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    },
                ]),
            ),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (groups, response) in cases {
            actual.push((
                groups,
                drive(&broker, &request(groups), &test_context(&p, &peer)).await,
            ));
            expected.push((groups, response));
        }
        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_allowed_missing_group_returns_not_found() {
        broker_fixture!((broker_handle, _dir, broker), group_allow_all);
        request_identity!((p, peer), principal("admin"));
        let ctx = test_context(&p, &peer);
        assert_results(&broker, &ctx, &["missing"], vec![result(
            "missing",
            codes::GROUP_ID_NOT_FOUND,
        )])
        .await;
        broker_handle.shutdown().await;
    }

    /// Trunk's v3 (KIP-1331) gives each group result a nullable
    /// `ErrorMessage`. The only topology description plugin krabka runs is
    /// Kafka's in-memory one, which cannot fail a delete, so krabka never
    /// answers `GROUP_DELETION_FAILED` and the message is always null: the
    /// v3 answer is the v2 answer with one null string per result.
    #[tokio::test]
    async fn handle_answers_v3_with_a_null_error_message_per_group() {
        use krabka_protocol::{Decode as _, Encode as _};

        let (broker_handle, _dir) = start_group_broker_no_audit(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "alice");
        let req = request(&["group-a", "group-b"]);

        let mut answers = Vec::new();
        for version in [2_i16, 3] {
            let mut body = bytes::BytesMut::new();
            req.encode(&mut body, version).expect("encode request");
            let bytes = dispatch_context(
                &broker,
                krabka_protocol::api_key::ApiKey::DeleteGroups as i16,
                version,
                &body,
                &ctx,
            )
            .await;
            let decoded =
                DeleteGroupsResponse::decode(&mut &bytes[..], version).expect("decode response");
            answers.push((version, bytes.len(), decoded));
        }

        let denied = |group_id: &str| {
            krabka_macros::tagged_wire!(DeletableGroupResult {
                group_id: group_id.to_string(),
                error_code: codes::GROUP_AUTHORIZATION_FAILED,
                error_message: None,
            })
        };
        let expected = DeleteGroupsResponse {
            results: vec![denied("group-a"), denied("group-b")],
            ..Default::default()
        };
        assert!(answers[0].2 == expected);
        assert!(answers[1].2 == expected);
        // One compact null string, a single 0 byte, for each of the two results.
        assert!(answers[1].1 == answers[0].1 + 2);
        broker_handle.shutdown().await;
    }
}
