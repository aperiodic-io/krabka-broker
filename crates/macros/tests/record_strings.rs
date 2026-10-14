//! Record traversal keeps required, optional and native nested strings in order.

use {
    assert2::assert,
    std::cell::{Cell, RefCell},
};

trait RecordStrings {
    fn longest_record_string(&self) -> usize;
}

thread_local! {
    static VISITED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn longest<'a>(strings: impl IntoIterator<Item = &'a str>) -> usize {
    VISITED.with(|visited| {
        let mut visited = visited.borrow_mut();
        visited.clear();
        strings
            .into_iter()
            .inspect(|value| visited.push((*value).into()))
            .map(str::len)
            .max()
            .unwrap_or(0)
    })
}

#[derive(Default)]
struct NestedRequest {
    group: String,
    member: String,
    instance: Option<String>,
    names: Vec<String>,
    extra_evaluations: Cell<usize>,
}

#[derive(Default)]
struct OptionalRequest {
    name: Option<String>,
}

struct EmptyRequest;

struct StateRequest {
    group_id: String,
}

struct CommitRequest {
    owner: String,
    topics: Vec<Topic>,
}

struct Topic {
    name: String,
    partitions: Vec<Partition>,
}

struct Partition {
    committed_metadata: Option<String>,
}

krabka_macros::record_strings! {
    NestedRequest(self) {
        required(group, member);
        optional(instance);
        extra {
            self.extra_evaluations.set(self.extra_evaluations.get() + 1);
            let __record_strings_fields = self.names.iter();
            __record_strings_fields.map(|name| {
                // The ordinary local block and qualified macro remain native.
                let _ = std::matches!(name.as_str(), "first" | "second");
                name.as_str()
            })
        }
    }
    OptionalRequest(self) { required(); optional(name); }
    EmptyRequest(self) { required(); optional(); }
    StateRequest(self) { required(group_id); optional(); }
    CommitRequest(self) { required(owner); optional(); offset_metadata(topics); }
}

#[test]
fn traversal_consumes_strings_in_declaration_order_and_counts_utf8_bytes() {
    let request = NestedRequest {
        group: "group".into(),
        member: "member".into(),
        instance: Some("instance".into()),
        names: vec!["first".into(), "second".into(), "🦀🦀🦀".into()],
        extra_evaluations: Cell::new(0),
    };
    assert!(request.longest_record_string() == 12);
    assert!(request.extra_evaluations.get() == 1);
    VISITED.with(|visited| {
        assert!(*visited.borrow() == ["group", "member", "instance", "first", "second", "🦀🦀🦀"]);
    });
    let empty = NestedRequest::default();
    assert!(empty.longest_record_string() == 0);
    assert!(empty.extra_evaluations.get() == 1);
    VISITED.with(|visited| {
        assert!(*visited.borrow() == ["", ""]);
    });
    assert!(OptionalRequest::default().longest_record_string() == 0);
    assert!(EmptyRequest.longest_record_string() == 0);
}

#[yare::parameterized(
    maximum = { 32_767 },
    over_maximum = { 32_768 },
)]
fn longest_string_boundary_keeps_the_exact_utf8_byte_length(length: usize) {
    assert2::assert!(
        StateRequest {
            group_id: "x".repeat(length)
        }
        .longest_record_string()
            == length
    );
}

#[test]
fn offset_policy_visits_topic_names_then_non_null_partition_metadata() {
    let request = CommitRequest {
        owner: "owner".into(),
        topics: vec![
            Topic {
                name: "orders".into(),
                partitions: vec![
                    Partition {
                        committed_metadata: None,
                    },
                    Partition {
                        committed_metadata: Some("metadata".into()),
                    },
                ],
            },
            Topic {
                name: "receipts".into(),
                partitions: vec![],
            },
        ],
    };
    assert!(request.longest_record_string() == 8);
    VISITED.with(|visited| {
        assert!(*visited.borrow() == ["owner", "orders", "metadata", "receipts"]);
    });
}
