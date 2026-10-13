use {super::*, assert2::assert};

#[test]
fn quota_split_is_kafkas_floor_and_remainder() {
    // Kafka's own example: 11 partitions over 3 members.
    let rows = [
        (11, 3, UniformQuotaSplit {
            minimum_quota: 3,
            extra_quotas: 2,
        }),
        (5, 2, UniformQuotaSplit {
            minimum_quota: 2,
            extra_quotas: 1,
        }),
        (2, 3, UniformQuotaSplit {
            minimum_quota: 0,
            extra_quotas: 2,
        }),
        (6, 3, UniformQuotaSplit {
            minimum_quota: 2,
            extra_quotas: 0,
        }),
        (0, 4, UniformQuotaSplit {
            minimum_quota: 0,
            extra_quotas: 0,
        }),
    ];
    for (total, members, expected) in rows {
        assert!(
            uniform_quota_split(total, members) == expected,
            "{total} over {members}"
        );
    }
}

#[test]
fn homogeneous_quotas_follow_kafka_scenarios() {
    // Rows are `UniformHomogeneousAssignmentBuilderTest` scenarios, with
    // the members in ascending member-ID order.
    let rows = [
        // testFirstAssignmentTwoMembersTwoTopicsNoMemberRacks: 5
        // partitions, nothing owned. A gives the extra slot up for B.
        QuotaRow {
            name: "first assignment, 5 over 2",
            minimum: 2,
            extras: 1,
            owned: &[0, 0],
            expected: vec![quota(false, 0, 2), quota(true, 0, 3)],
        },
        // testFirstAssignmentNumMembersGreaterThanTotalNumPartitions.
        QuotaRow {
            name: "first assignment, 2 over 3",
            minimum: 0,
            extras: 2,
            owned: &[0, 0, 0],
            expected: vec![quota(false, 0, 0), quota(true, 0, 1), quota(true, 0, 1)],
        },
        // testReassignmentForTwoMembersTwoTopicsGivenUnbalancedPrevAssignment:
        // A owns 4 of 6 and gives one back.
        QuotaRow {
            name: "unbalanced previous assignment",
            minimum: 3,
            extras: 0,
            owned: &[4, 2],
            expected: vec![quota(false, 3, 0), quota(false, 2, 1)],
        },
        // testReassignmentWhenPartitionsAreAddedForTwoMembersTwoTopics:
        // 11 partitions, each member owns 3.
        QuotaRow {
            name: "partitions added",
            minimum: 5,
            extras: 1,
            owned: &[3, 3],
            expected: vec![quota(false, 3, 2), quota(true, 3, 3)],
        },
        // testReassignmentWhenOneMemberAddedAfterInitialAssignmentWithTwoMembersTwoTopics.
        QuotaRow {
            name: "member added",
            minimum: 2,
            extras: 0,
            owned: &[3, 3, 0],
            expected: vec![quota(false, 2, 0), quota(false, 2, 0), quota(false, 0, 2)],
        },
        // A member already above the minimum keeps the extra slot even
        // when later members could take it.
        QuotaRow {
            name: "owner above minimum keeps the extra slot",
            minimum: 2,
            extras: 1,
            owned: &[3, 0, 0],
            expected: vec![quota(true, 3, 0), quota(false, 0, 2), quota(false, 0, 2)],
        },
    ];
    for row in rows {
        assert!(
            homogeneous_member_quotas(row.minimum, row.extras, row.owned) == row.expected,
            "{}",
            row.name
        );
    }
}

#[test]
fn least_loaded_selection_orders_by_load_then_start_then_position() {
    let rows: [(&str, &[SubscriberLoad], Option<usize>); 6] = [
        ("no subscribers", &[], None),
        (
            "smallest current load",
            &[load(4, 4), load(1, 1), load(1, 1)],
            Some(1),
        ),
        ("first of equal loads", &[load(2, 2), load(2, 2)], Some(0)),
        // A member that started the topic lighter goes first at a level,
        // even behind a lower member index.
        (
            "starting load breaks a level tie",
            &[load(1, 1), load(1, 0)],
            Some(1),
        ),
        (
            "current load outranks starting load",
            &[load(2, 0), load(1, 1)],
            Some(1),
        ),
        (
            "maximum loads",
            &[load(usize::MAX, 0), load(usize::MAX, 0)],
            Some(0),
        ),
    ];
    for (name, candidates, expected) in rows {
        assert!(select_least_loaded(candidates) == expected, "{name}");
    }
}
