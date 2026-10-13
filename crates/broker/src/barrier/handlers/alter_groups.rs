//! `AlterBarrierGroups`, api key 1010.
//!
//! One request carries a list of entries, and each entry names one group. An
//! entry with `delete` set removes the group. Every other entry creates the
//! group when no group of that name is live, and replaces the definition when
//! one is.
//!
//! The handler reads the live set before it picks create or update, and the
//! coordinator re-checks it under the group lock. A group that appears or goes
//! away between the two reads comes back as
//! [`BarrierError::GroupExists`][crate::barrier::error::BarrierError::GroupExists]
//! or
//! [`BarrierError::UnknownGroup`][crate::barrier::error::BarrierError::UnknownGroup],
//! so a race reports itself rather than silently taking the other branch.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On a deny every entry
//! of the response carries `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! A group name or a topic name of more than 32767 bytes cannot go into a
//! `__barrier_state` record, whose strings carry an `i16` length, and a compact
//! string on the wire can carry one. The entry that names it answers
//! `INVALID_REQUEST` (42), and nothing reaches the coordinator.

use {
    crate::{
        barrier::{
            coordinator::BarrierCoordinator,
            handlers::{error_code, error_text, interval_from_wire},
            state::GroupSpec,
        },
        codes,
        coordinator::unified::persistence::MAX_STRING_BYTES,
        handlers::{cluster_alter_denied, encode_response},
    },
    krabka_protocol::krabka::barrier::{
        AlterBarrierGroupResult, AlterBarrierGroupsRequest, AlterBarrierGroupsResponse,
        AlterableBarrierGroup,
    },
    std::slice,
};

wire_handler!(
    handle,
    "handle_alter_barrier_groups",
    "AlterBarrierGroups",
    "info",
    AlterBarrierGroupsRequest,
    |broker, version, req, ctx| {
        let image = broker.controller.current_image();
        let denied = cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx);

        let mut results = Vec::with_capacity(req.groups.len());
        for entry in &req.groups {
            if denied {
                results.push(row(
                    &entry.group,
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("alter-barrier-groups denied".to_owned()),
                ));
            } else {
                results.push(apply(&broker.barrier_coordinator, entry).await);
            }
        }

        encode_response(
            &AlterBarrierGroupsResponse {
                throttle_time_ms: 0,
                results,
                ..AlterBarrierGroupsResponse::default()
            },
            version,
        )
    }
);

/// Apply one entry and build its result row.
async fn apply(
    coordinator: &BarrierCoordinator,
    entry: &AlterableBarrierGroup,
) -> AlterBarrierGroupResult {
    if entry.group.len() > MAX_STRING_BYTES {
        return row(
            &entry.group,
            codes::INVALID_REQUEST,
            Some(format!(
                "a barrier group name is {} bytes, and the limit is {MAX_STRING_BYTES}",
                entry.group.len()
            )),
        );
    }
    if entry.delete {
        return match coordinator.delete_group(&entry.group).await {
            Ok(()) => row(&entry.group, codes::NONE, None),
            Err(error) => row(&entry.group, error_code(&error), Some(error_text(&error))),
        };
    }

    if let Some(reason) = topic_list_fault(&entry.topics) {
        return row(&entry.group, codes::INVALID_REQUEST, Some(reason));
    }

    let spec = spec_of(entry);
    let live = !coordinator
        .describe_groups(slice::from_ref(&entry.group))
        .await
        .is_empty();
    let outcome = if live {
        coordinator.update_group(&entry.group, spec).await
    } else {
        coordinator.create_group(&entry.group, spec).await
    };
    match outcome {
        Ok(_) => row(&entry.group, codes::NONE, None),
        Err(error) => row(&entry.group, error_code(&error), Some(error_text(&error))),
    }
}

/// The group definition that one entry names.
fn spec_of(entry: &AlterableBarrierGroup) -> GroupSpec {
    GroupSpec {
        topics: entry.topics.clone(),
        interval: interval_from_wire(entry.interval_ms),
        retained_cuts: entry.retained_cuts,
    }
}

/// The reason a topic list is not usable, or `None` when it is.
///
/// A fault here is a fault in the shape of the request, so the handler answers
/// `INVALID_REQUEST` (42). A retention or an interval that is out of range is
/// a group setting instead, and `validate_spec` rejects it with
/// `INVALID_CONFIG` (40).
fn topic_list_fault(topics: &[String]) -> Option<String> {
    if topics.is_empty() {
        return Some("a barrier group needs at least one topic".to_owned());
    }
    if topics.iter().any(String::is_empty) {
        return Some("a barrier group topic name is empty".to_owned());
    }
    if let Some(topic) = topics.iter().find(|topic| topic.len() > MAX_STRING_BYTES) {
        return Some(format!(
            "a barrier group topic name is {} bytes, and the limit is {MAX_STRING_BYTES}",
            topic.len()
        ));
    }
    for (index, topic) in topics.iter().enumerate() {
        if topics[index + 1..].contains(topic) {
            return Some(format!("a barrier group names topic {topic} twice"));
        }
    }
    None
}

/// One result row of the response.
fn row(group: &str, error_code: i16, error_message: Option<String>) -> AlterBarrierGroupResult {
    AlterBarrierGroupResult {
        group: group.to_owned(),
        error_code,
        error_message,
        ..AlterBarrierGroupResult::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::barrier::{coordinator::test_support::Fixture, error::BarrierError},
        assert2::check,
        krabka_units::millis,
    };

    fn entry(topics: &[&str], interval_ms: i64, retained_cuts: i32) -> AlterableBarrierGroup {
        AlterableBarrierGroup {
            group: "orders-cut".to_owned(),
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
            interval_ms,
            retained_cuts,
            delete: false,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }
    }

    #[test]
    fn an_entry_becomes_the_group_definition_it_names() {
        let expected = GroupSpec {
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            interval: Some(millis(30_000)),
            retained_cuts: 5,
        };
        check!(spec_of(&entry(&["orders", "payments"], 30_000, 5)) == expected);
    }

    #[test]
    fn an_entry_with_no_interval_defines_a_group_that_only_a_trigger_cuts() {
        let expected = GroupSpec {
            topics: vec!["orders".to_owned()],
            interval: None,
            retained_cuts: 3,
        };
        check!(spec_of(&entry(&["orders"], -1, 3)) == expected);
    }

    #[test]
    fn a_topic_name_is_usable_up_to_32767_bytes() {
        for (length, fault) in [
            (MAX_STRING_BYTES, None),
            (
                MAX_STRING_BYTES + 1,
                Some("a barrier group topic name is 32768 bytes, and the limit is 32767"),
            ),
        ] {
            let list = vec!["orders".to_owned(), "t".repeat(length)];
            check!(
                topic_list_fault(&list) == fault.map(ToOwned::to_owned),
                "{length} bytes"
            );
        }
    }

    #[test]
    fn a_malformed_topic_list_names_its_own_fault() {
        struct Case<'a> {
            topics: &'a [&'a str],
            fault: Option<&'a str>,
        }
        let cases = &[
            Case {
                topics: &["orders", "payments"],
                fault: None,
            },
            Case {
                topics: &[],
                fault: Some("a barrier group needs at least one topic"),
            },
            Case {
                topics: &["orders", ""],
                fault: Some("a barrier group topic name is empty"),
            },
            Case {
                topics: &["orders", "payments", "orders"],
                fault: Some("a barrier group names topic orders twice"),
            },
        ];
        for Case { topics, fault } in cases {
            let list: Vec<String> = topics.iter().map(|t| (*t).to_owned()).collect();
            check!(
                topic_list_fault(&list) == fault.map(ToOwned::to_owned),
                "{topics:?}"
            );
        }
    }

    /// A compact string on the wire can carry a name that a state record
    /// cannot, so the entry that names one is refused as a bad request and
    /// nothing reaches the coordinator.
    #[tokio::test]
    async fn an_entry_with_a_name_of_32768_bytes_is_an_invalid_request_and_creates_nothing() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let long = "n".repeat(MAX_STRING_BYTES + 1);
        let cases = [
            ("a group name to create", AlterableBarrierGroup {
                group: long.clone(),
                ..entry(&["orders"], 30_000, 5)
            }),
            ("a group name to delete", AlterableBarrierGroup {
                group: long.clone(),
                delete: true,
                ..entry(&["orders"], 30_000, 5)
            }),
            ("a topic name", entry(&["orders", &long], 30_000, 5)),
        ];
        for (case, entry) in cases {
            let answered = apply(&coordinator, &entry).await;
            check!(answered.error_code == codes::INVALID_REQUEST, "{case}");
        }
        check!(coordinator.describe_groups(&[]).await.is_empty());
    }

    /// The longest name that a state record can carry is taken.
    #[tokio::test]
    async fn an_entry_with_a_group_and_a_topic_of_32767_bytes_creates_the_group() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let group = "g".repeat(MAX_STRING_BYTES);
        let topic = "t".repeat(MAX_STRING_BYTES);
        let created = AlterableBarrierGroup {
            group: group.clone(),
            ..entry(&["orders", &topic], 30_000, 5)
        };

        let answered = apply(&coordinator, &created).await;

        check!(answered.error_code == codes::NONE);
        let names: Vec<String> = coordinator
            .describe_groups(&[])
            .await
            .into_iter()
            .map(|description| description.group)
            .collect();
        check!(names == vec![group]);
    }

    #[test]
    fn a_result_row_carries_the_group_the_code_and_the_text() {
        let error = BarrierError::GroupExists {
            group: "orders-cut".to_owned(),
        };
        let expected = AlterBarrierGroupResult {
            group: "orders-cut".to_owned(),
            error_code: codes::TOPIC_ALREADY_EXISTS,
            error_message: Some(error.to_string()),
            ..AlterBarrierGroupResult::default()
        };
        check!(
            row("orders-cut", error_code(&error), Some(error_text(&error))) == expected,
            "{error}"
        );
    }

    #[test]
    fn a_row_that_applied_carries_no_code_and_no_text() {
        let expected = AlterBarrierGroupResult {
            group: "orders-cut".to_owned(),
            error_code: codes::NONE,
            error_message: None,
            ..AlterBarrierGroupResult::default()
        };
        check!(row("orders-cut", codes::NONE, None) == expected);
    }
}
