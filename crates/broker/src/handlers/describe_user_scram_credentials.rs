//! `DescribeUserScramCredentials` (`api_key` 50, KIP-554 read half).

use {
    crate::{
        broker::Broker,
        codes::{CLUSTER_AUTHORIZATION_FAILED, DUPLICATE_RESOURCE, RESOURCE_NOT_FOUND},
        handlers::{RequestContext, cluster_describe_denied},
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::{
            CredentialInfo, DescribeUserScramCredentialsResponse,
            DescribeUserScramCredentialsResult,
        },
    },
    krabka_security::SaslMechanism,
};

/// Kafka's `ScramImage.DESCRIBE_DUPLICATE_USER`; the row message appends the
/// user name after `": "`.
const DESCRIBE_DUPLICATE_USER: &str =
    "Cannot describe SCRAM credentials for the same user twice in a single request";

/// Kafka's `ScramImage.DESCRIBE_USER_THAT_DOES_NOT_EXIST`; the row message
/// appends the user name after `": "`.
const DESCRIBE_USER_THAT_DOES_NOT_EXIST: &str =
    "Attempt to describe a user credential that does not exist";

pub(crate) fn handle(
    broker: &Broker,
    req: &DescribeUserScramCredentialsRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> DescribeUserScramCredentialsResponse {
    let image = broker.controller.current_image();

    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return denied_response(req);
    }

    let known_users: std::collections::HashSet<String> =
        image.scram_credentials_users().into_iter().collect();
    let targets = requested_targets(&known_users, req.users.as_deref());

    let results = build_results(&image, &known_users, targets);

    DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: 0,
        error_message: None,
        results,
        ..Default::default()
    }
}

/// The refusal Kafka's `DescribeUserScramCredentialsRequest.getErrorResponse`
/// builds for a `CLUSTER_AUTHORIZATION_FAILED` exception.
///
/// `ApiError.fromThrowable` drops a message equal to the error's default text,
/// so the top level carries no message. Each requested user gets one row with
/// the same code and an empty user name, because Kafka never sets `user` on
/// those rows. A null user list gives no rows.
fn denied_response(
    req: &DescribeUserScramCredentialsRequest,
) -> DescribeUserScramCredentialsResponse {
    let results = req
        .users
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|_| DescribeUserScramCredentialsResult {
            user: String::new(),
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            ..Default::default()
        })
        .collect();
    DescribeUserScramCredentialsResponse {
        throttle_time_ms: 0,
        error_code: CLUSTER_AUTHORIZATION_FAILED,
        error_message: None,
        results,
        ..Default::default()
    }
}

fn build_results(
    image: &MetadataImage,
    known_users: &std::collections::HashSet<String>,
    targets: Vec<DescribeTarget>,
) -> Vec<DescribeUserScramCredentialsResult> {
    targets
        .into_iter()
        .map(|target| {
            let user = target.user;
            if target.is_duplicate {
                return DescribeUserScramCredentialsResult {
                    error_message: Some(format!("{DESCRIBE_DUPLICATE_USER}: {user}")),
                    user,
                    error_code: DUPLICATE_RESOURCE,
                    credential_infos: vec![],
                    ..Default::default()
                };
            }

            let mut pairs = image.scram_credentials_for_user(&user);
            if pairs.is_empty() && !known_users.contains(&user) {
                DescribeUserScramCredentialsResult {
                    error_code: RESOURCE_NOT_FOUND,
                    error_message: Some(format!("{DESCRIBE_USER_THAT_DOES_NOT_EXIST}: {user}")),
                    user,
                    credential_infos: vec![],
                    ..Default::default()
                }
            } else {
                pairs.sort_by_key(|(mech, _)| sasl_mechanism_to_byte(*mech));
                let credential_infos: Vec<CredentialInfo> = pairs
                    .into_iter()
                    .map(|(mech, iters)| CredentialInfo {
                        mechanism: sasl_mechanism_to_byte(mech),
                        iterations: iters.cast_signed(),
                        ..Default::default()
                    })
                    .collect();
                DescribeUserScramCredentialsResult {
                    user,
                    error_code: 0,
                    error_message: None,
                    credential_infos,
                    ..Default::default()
                }
            }
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DescribeTarget {
    user: String,
    is_duplicate: bool,
}

fn requested_targets(
    known_users: &std::collections::HashSet<String>,
    users_filter: Option<&[UserName]>,
) -> Vec<DescribeTarget> {
    let Some(filter) = users_filter else {
        return all_known_user_targets(known_users);
    };
    if filter.is_empty() {
        return all_known_user_targets(known_users);
    }

    let mut requested_users = Vec::new();
    let mut duplicate_flags = std::collections::HashMap::new();
    for requested_user in filter {
        let user = requested_user.name.clone();
        match duplicate_flags.entry(user.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(false);
                requested_users.push(user);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.insert(true);
            }
        }
    }

    requested_users
        .into_iter()
        .map(|user| DescribeTarget {
            is_duplicate: duplicate_flags.get(&user).copied().unwrap_or(false),
            user,
        })
        .collect()
}

fn all_known_user_targets(known_users: &std::collections::HashSet<String>) -> Vec<DescribeTarget> {
    let mut users: Vec<String> = known_users.iter().cloned().collect();
    users.sort();
    users
        .into_iter()
        .map(|user| DescribeTarget {
            user,
            is_duplicate: false,
        })
        .collect()
}

#[must_use]
fn sasl_mechanism_to_byte(m: SaslMechanism) -> i8 {
    match m {
        SaslMechanism::ScramSha256 => 1,
        SaslMechanism::ScramSha512 => 2,
        // Non-SCRAM mechanisms never own SCRAM credential records; map to the
        // KIP-554 UNKNOWN sentinel (0).
        SaslMechanism::Plain | SaslMechanism::OAuthBearer | SaslMechanism::Gssapi => 0,
    }
}

#[cfg(test)]
mod tests {
    use {
        assert2::assert,
        krabka_metadata::{AclOperation, MetadataRecord, ResourceType, ScramCredentialRecord},
        std::sync::Arc,
    };

    #[derive(Debug)]
    struct ClusterDescribeOnly;

    krabka_macros::test_authorizer!(ClusterDescribeOnly, req; {
        if req.resource_type == ResourceType::Cluster
            && req.resource_name == crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME
            && req.operation == AclOperation::Describe
        {
            return AuthorizationResult::Allow;
        }

        AuthorizationResult::Deny
    });

    use {
        super::*,
        crate::{
            authorizer::AuthorizationResult,
            test_support::{DenyAll, start_broker_with_authorizer},
        },
    };

    /// A SCRAM credential to seed: its user, mechanism and iteration count.
    #[derive(Clone, Copy)]
    struct SeededUser<'a> {
        user: &'a str,
        mechanism: SaslMechanism,
        iterations: u32,
    }

    fn img_with_scram(users: &[SeededUser<'_>]) -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        for SeededUser {
            user,
            mechanism,
            iterations,
        } in users
        {
            img.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
                user: (*user).into(),
                mechanism: *mechanism,
                iterations: *iterations,
                salt: vec![1, 2, 3],
                server_key: vec![4, 5, 6],
                stored_key: vec![7, 8, 9],
            }));
        }
        img
    }

    fn process_targets_for_test(
        image: &MetadataImage,
        users_filter: Option<
            &[krabka_protocol::owned::describe_user_scram_credentials_request::UserName],
        >,
    ) -> DescribeUserScramCredentialsResponse {
        let known_users: std::collections::HashSet<String> =
            image.scram_credentials_users().into_iter().collect();
        let targets = requested_targets(&known_users, users_filter);
        let results = build_results(image, &known_users, targets);
        DescribeUserScramCredentialsResponse {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            results,
            ..Default::default()
        }
    }

    krabka_macros::scram_users_fixture!(scram_users_request);

    const ALICE: SeededUser<'static> = SeededUser {
        user: "alice",
        mechanism: SaslMechanism::ScramSha512,
        iterations: 4096,
    };

    const ALICE_ONLY: [SeededUser<'static>; 1] = [ALICE];

    const ALICE_AND_BOB: [SeededUser<'static>; 2] = [ALICE, SeededUser {
        user: "bob",
        mechanism: SaslMechanism::ScramSha512,
        iterations: 8192,
    }];

    fn run_handle_filter(
        users_filter: Option<Vec<String>>,
        seeded: &[SeededUser<'_>],
    ) -> DescribeUserScramCredentialsResponse {
        let req = scram_users_request(users_filter);
        let image = img_with_scram(seeded);
        process_targets_for_test(&image, req.users.as_deref())
    }

    #[test]
    fn describe_all_users_when_filter_none() {
        let resp = run_handle_filter(None, &ALICE_AND_BOB);
        assert!(resp.results.len() == 2);
        let users: Vec<&str> = resp.results.iter().map(|r| r.user.as_str()).collect();
        assert!(users.contains(&"alice") && users.contains(&"bob"));
    }

    #[test]
    fn describe_filter_returns_only_listed_users() {
        let resp = run_handle_filter(Some(vec!["alice".into()]), &ALICE_AND_BOB);
        let expected = vec![krabka_macros::tagged_wire!(
            DescribeUserScramCredentialsResult {
                user: "alice".to_string(),
                error_code: 0,
                error_message: None,
                credential_infos: vec![krabka_macros::tagged_wire!(CredentialInfo {
                    mechanism: 2,
                    iterations: 4096,
                })],
            }
        )];
        assert!(resp.results == expected);
    }

    #[test]
    fn unknown_user_returns_resource_not_found() {
        let resp = run_handle_filter(Some(vec!["ghost".into()]), &ALICE_ONLY);
        let expected = vec![krabka_macros::tagged_wire!(
            DescribeUserScramCredentialsResult {
                user: "ghost".to_string(),
                error_code: 91,
                error_message: Some(
                    "Attempt to describe a user credential that does not exist: ghost".to_string(),
                ),
                credential_infos: Vec::new(),
            }
        )];
        assert!(resp.results == expected);
    }

    #[test]
    fn duplicate_requested_user_returns_single_duplicate_resource_row() {
        const KAFKA_DUPLICATE_RESOURCE: i16 = 92;

        let resp = run_handle_filter(
            Some(vec!["alice".into(), "bob".into(), "alice".into()]),
            &ALICE_AND_BOB,
        );

        assert!(
            resp.results.len() == 2,
            "duplicate users collapse to one row"
        );
        let alice_rows: Vec<_> = resp.results.iter().filter(|r| r.user == "alice").collect();
        assert!(
            alice_rows.len() == 1,
            "alice should appear once: {:?}",
            resp.results
        );
        assert!(alice_rows[0].error_code == KAFKA_DUPLICATE_RESOURCE);
        assert!(alice_rows[0].credential_infos.is_empty());

        let bob = resp
            .results
            .iter()
            .find(|r| r.user == "bob")
            .expect("distinct users remain in the response");
        assert!(bob.error_code == 0);
        assert!(
            bob.credential_infos
                == vec![krabka_macros::tagged_wire!(CredentialInfo {
                    mechanism: 2,
                    iterations: 8192,
                })]
        );
    }

    #[test]
    fn credential_infos_are_ordered_by_kafka_scram_mechanism_type() {
        let resp = run_handle_filter(Some(vec!["alice".into()]), &[
            SeededUser {
                user: "alice",
                mechanism: SaslMechanism::ScramSha512,
                iterations: 8192,
            },
            SeededUser {
                user: "alice",
                mechanism: SaslMechanism::ScramSha256,
                iterations: 4096,
            },
        ]);

        let alice = resp
            .results
            .iter()
            .find(|row| row.user == "alice")
            .expect("alice result exists");
        let mechanisms: Vec<i8> = alice
            .credential_infos
            .iter()
            .map(|info| info.mechanism)
            .collect();

        assert!(mechanisms == vec![1, 2]);
    }

    #[yare::parameterized(
        scram_sha256 = { SaslMechanism::ScramSha256, 1 },
        scram_sha512 = { SaslMechanism::ScramSha512, 2 },
        plain = { SaslMechanism::Plain, 0 },
    )]
    fn sasl_mechanism_byte_mapping(mechanism: SaslMechanism, want: i8) {
        assert2::assert!(sasl_mechanism_to_byte(mechanism) == want, "{mechanism:?}");
    }

    /// An empty users filter asks for all credentials at the initial protocol version.
    fn empty_describe_response(
        broker: &Broker,
        context: &RequestContext<'_>,
    ) -> DescribeUserScramCredentialsResponse {
        handle(
            broker,
            &DescribeUserScramCredentialsRequest::default(),
            0,
            context,
        )
    }
    #[tokio::test]
    async fn handle_allows_cluster_describe_authorization() {
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [start_broker_with_authorizer(Arc::new(ClusterDescribeOnly))];
            user ["alice"];
            client ["scram-describe-test"];
        );
        let resp = empty_describe_response(&broker, &ctx);

        assert!(
            resp == krabka_macros::unthrottled_wire!(DescribeUserScramCredentialsResponse {
                error_code: 0,
                error_message: None,
                results: Vec::new(),
            }),
            "Cluster Describe should authorize"
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_without_cluster_describe_authorization() {
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, principal, peer, ctx);
            boot [start_broker_with_authorizer(Arc::new(DenyAll,))];
            user ["alice"];
            client ["scram-describe-test"];
        );
        let resp = empty_describe_response(&broker, &ctx);

        assert!(
            resp == krabka_macros::unthrottled_wire!(DescribeUserScramCredentialsResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: None,
                results: Vec::new(),
            })
        );
        broker_handle.shutdown().await;
    }

    #[yare::parameterized(
        absent_users = { None, 0 },
        empty_users = { Some(vec![]), 0 },
        repeated_users = { Some(vec!["alice", "bob", "alice"]), 3 },
    )]
    fn denied_response_matches_kafka_get_error_response(
        users: Option<Vec<&str>>,
        expected_rows: usize,
    ) {
        use krabka_protocol::owned::describe_user_scram_credentials_request::UserName;

        let denied_row = krabka_macros::tagged_wire!(DescribeUserScramCredentialsResult {
            user: String::new(),
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            credential_infos: Vec::new(),
        });
        let req = DescribeUserScramCredentialsRequest {
            users: users.as_ref().map(|names| {
                names
                    .iter()
                    .map(|name| UserName {
                        name: name.to_string(),
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        };
        let want = krabka_macros::unthrottled_wire!(DescribeUserScramCredentialsResponse {
            error_code: CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            results: vec![denied_row; expected_rows],
        });
        assert2::assert!(denied_response(&req) == want, "users = {users:?}");
    }
}
