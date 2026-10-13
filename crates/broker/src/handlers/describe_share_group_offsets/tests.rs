//! End-to-end tests of the `DescribeShareGroupOffsets` handler against a
//! running broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group and
//! per-partition rows KIP-932 asks for -- feature disabled, group
//! denied, topic unknown -- stay exactly what the JVM admin client reads.

use {
    super::*,
    crate::{
        authorizer::Authorizer,
        test_support::{DenyAll, ShareApiSupport},
    },
    krabka_protocol::{
        owned::{
            describe_share_group_offsets_request::{
                DescribeShareGroupOffsetsRequestGroup, DescribeShareGroupOffsetsRequestTopic,
            },
            describe_share_group_offsets_response::{
                self, DescribeShareGroupOffsetsResponsePartition,
                DescribeShareGroupOffsetsResponseTopic,
            },
        },
        primitives::uuid::Uuid,
    },
    std::sync::Arc,
};

/// A requested topic and the partitions of it that the request names.
struct RequestTopic<'a> {
    topic_name: &'a str,
    partitions: Vec<i32>,
}

/// A requested group and the topics of it that the request names.
struct RequestGroup<'a> {
    group_id: &'a str,
    topics: Vec<RequestTopic<'a>>,
}

fn request(groups: &[RequestGroup<'_>]) -> DescribeShareGroupOffsetsRequest {
    DescribeShareGroupOffsetsRequest {
        groups: groups
            .iter()
            .map(|group| DescribeShareGroupOffsetsRequestGroup {
                group_id: group.group_id.into(),
                topics: Some(
                    group
                        .topics
                        .iter()
                        .map(|topic| DescribeShareGroupOffsetsRequestTopic {
                            topic_name: topic.topic_name.into(),
                            partitions: topic.partitions.clone(),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

krabka_macros::wire_codec_helpers! {
    test_context ["admin-client"];
}

#[tokio::test]
async fn handle_error_scenarios_preserve_expected_rows() {
    type Case<'a> = (
        &'a str,
        Arc<dyn Authorizer>,
        ShareApiSupport,
        Vec<RequestGroup<'a>>,
        DescribeShareGroupOffsetsResponse,
    );
    let version = describe_share_group_offsets_response::MAX_VERSION;
    let cases: Vec<Case<'_>> = vec![
        (
            "disabled feature preserves group error rows",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            crate::test_support::ShareApiSupport::Disabled,
            vec![
                RequestGroup {
                    group_id: "g1",
                    topics: vec![RequestTopic {
                        topic_name: "t1",
                        partitions: vec![0],
                    }],
                },
                RequestGroup {
                    group_id: "g2",
                    topics: vec![RequestTopic {
                        topic_name: "t2",
                        partitions: vec![1],
                    }],
                },
            ],
            krabka_macros::unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![
                    krabka_macros::tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: Vec::new(),
                        error_code: codes::UNSUPPORTED_VERSION,
                        error_message: None,
                    }),
                    krabka_macros::tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g2".into(),
                        topics: Vec::new(),
                        error_code: codes::UNSUPPORTED_VERSION,
                        error_message: None,
                    }),
                ],
            }),
        ),
        (
            "denied group preserves group id and error code",
            Arc::new(crate::test_support::ControllerPeerAllowed(DenyAll)),
            crate::test_support::ShareApiSupport::Enabled,
            vec![RequestGroup {
                group_id: "g1",
                topics: vec![RequestTopic {
                    topic_name: "missing",
                    partitions: vec![0],
                }],
            }],
            krabka_macros::unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![krabka_macros::tagged_wire!(
                    DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: Vec::new(),
                        error_code: codes::GROUP_AUTHORIZATION_FAILED,
                        error_message: None,
                    }
                )],
            }),
        ),
        (
            "an unknown topic has no data and no error",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            crate::test_support::ShareApiSupport::Enabled,
            vec![RequestGroup {
                group_id: "g1",
                topics: vec![RequestTopic {
                    topic_name: "missing-topic",
                    partitions: vec![3, 5],
                }],
            }],
            krabka_macros::unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![krabka_macros::tagged_wire!(
                    DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: vec![krabka_macros::tagged_wire!(
                            DescribeShareGroupOffsetsResponseTopic {
                                topic_name: "missing-topic".into(),
                                topic_id: Uuid::default(),
                                partitions: vec![
                                    krabka_macros::tagged_wire!(
                                        DescribeShareGroupOffsetsResponsePartition {
                                            partition_index: 3,
                                            start_offset: -1,
                                            leader_epoch: 0,
                                            lag: -1,
                                            error_code: codes::NONE,
                                            error_message: None,
                                        }
                                    ),
                                    krabka_macros::tagged_wire!(
                                        DescribeShareGroupOffsetsResponsePartition {
                                            partition_index: 5,
                                            start_offset: -1,
                                            leader_epoch: 0,
                                            lag: -1,
                                            error_code: codes::NONE,
                                            error_message: None,
                                        }
                                    ),
                                ],
                            }
                        )],
                        error_code: codes::NONE,
                        error_message: None,
                    }
                )],
            }),
        ),
    ];
    krabka_macros::share_refusal_matrix! {
        case(case, authorizer, share_enabled, groups, expected) [cases];
        bind(broker_handle, _dir, broker, ctx, resp);
        invoke [handle(&broker, request(&groups), version, &ctx)];
    };
}
