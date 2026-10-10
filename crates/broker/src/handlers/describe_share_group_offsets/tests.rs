//! End-to-end tests of the `DescribeShareGroupOffsets` handler against a
//! running broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group and
//! per-partition rows KIP-932 asks for -- feature disabled, group
//! denied, topic unknown -- stay exactly what the JVM admin client reads.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::{
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
};

use super::*;
use crate::{
    authorizer::Authorizer,
    test_support::{DenyAll, test_ctx},
};

type RequestTopic<'a> = (&'a str, Vec<i32>);
type RequestGroup<'a> = (&'a str, Vec<RequestTopic<'a>>);

fn request(groups: &[RequestGroup<'_>]) -> DescribeShareGroupOffsetsRequest {
    DescribeShareGroupOffsetsRequest {
        groups: groups
            .iter()
            .map(|(group_id, topics)| DescribeShareGroupOffsetsRequestGroup {
                group_id: (*group_id).into(),
                topics: Some(
                    topics
                        .iter()
                        .map(
                            |(topic_name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                                topic_name: (*topic_name).into(),
                                partitions: partitions.clone(),
                                ..Default::default()
                            },
                        )
                        .collect(),
                ),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

crate::test_support::context_helper!(client_id = "admin-client");

#[tokio::test]
async fn handle_error_scenarios_preserve_expected_rows() {
    type Case<'a> = (
        &'a str,
        Arc<dyn Authorizer>,
        crate::test_support::ShareApiSupport,
        Vec<RequestGroup<'a>>,
        DescribeShareGroupOffsetsResponse,
    );
    let version = describe_share_group_offsets_response::MAX_VERSION;
    let cases: Vec<Case<'_>> = vec![
        (
            "disabled feature preserves group error rows",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            crate::test_support::ShareApiSupport::Disabled,
            vec![("g1", vec![("t1", vec![0])]), ("g2", vec![("t2", vec![1])])],
            unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![
                    tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g1".into(),
                        topics: Vec::new(),
                        error_code: codes::UNSUPPORTED_VERSION,
                        error_message: None,
                    }),
                    tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
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
            vec![("g1", vec![("missing", vec![0])])],
            unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
                    group_id: "g1".into(),
                    topics: Vec::new(),
                    error_code: codes::GROUP_AUTHORIZATION_FAILED,
                    error_message: None,
                })],
            }),
        ),
        (
            "an unknown topic has no data and no error",
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            crate::test_support::ShareApiSupport::Enabled,
            vec![("g1", vec![("missing-topic", vec![3, 5])])],
            unthrottled_wire!(DescribeShareGroupOffsetsResponse {
                groups: vec![tagged_wire!(DescribeShareGroupOffsetsResponseGroup {
                    group_id: "g1".into(),
                    topics: vec![tagged_wire!(DescribeShareGroupOffsetsResponseTopic {
                        topic_name: "missing-topic".into(),
                        topic_id: Uuid::default(),
                        partitions: vec![
                            tagged_wire!(DescribeShareGroupOffsetsResponsePartition {
                                partition_index: 3,
                                start_offset: -1,
                                leader_epoch: 0,
                                lag: -1,
                                error_code: codes::NONE,
                                error_message: None,
                            }),
                            tagged_wire!(DescribeShareGroupOffsetsResponsePartition {
                                partition_index: 5,
                                start_offset: -1,
                                leader_epoch: 0,
                                lag: -1,
                                error_code: codes::NONE,
                                error_message: None,
                            }),
                        ],
                    })],
                    error_code: codes::NONE,
                    error_message: None,
                })],
            }),
        ),
    ];
    share_refusal_cases!(
        (case, authorizer, share_enabled, [groups], expected) in cases;
        (broker_handle, _dir, broker, ctx, resp);
        handle(request(&groups), version)
    );
}
