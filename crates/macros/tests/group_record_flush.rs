//! Record encoding and durable append precede an actor's visible cache update.

use {assert2::assert, std::cell::RefCell};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Encode,
    Append,
}

struct Delta {
    empty: bool,
    failure: Option<Failure>,
}

impl Delta {
    fn is_empty(&self) -> bool { self.empty }

    fn encode(&self, events: &RefCell<Vec<&'static str>>) -> Result<u8, Failure> {
        events.borrow_mut().push("encode");
        if self.failure == Some(Failure::Encode) {
            Err(Failure::Encode)
        } else {
            Ok(7)
        }
    }
}

struct Journal<'a> {
    events: &'a RefCell<Vec<&'static str>>,
    fails: bool,
}

impl Journal<'_> {
    fn append(&self, group: &str, batch: u8) -> std::future::Ready<Result<(), Failure>> {
        assert!(group == "group" && batch == 7);
        self.events.borrow_mut().push("append");
        std::future::ready(if self.fails {
            Err(Failure::Append)
        } else {
            Ok(())
        })
    }
}

#[krabka_macros::flush_group_records(group [group]; encode [pending.encode(events)];)]
async fn flush(
    group: &str,
    pending: Delta,
    log: &Journal<'_>,
    events: &RefCell<Vec<&'static str>>,
    _now: i64,
) -> Result<(), Failure> {
    events.borrow_mut().push("cache");
}

#[tokio::test]
async fn empty_and_failed_deltas_never_publish_a_cache_update() {
    struct Scenario {
        empty: bool,
        failure: Option<Failure>,
        expected: &'static [&'static str],
    }
    for case in [
        Scenario {
            empty: true,
            failure: None,
            expected: &[],
        },
        Scenario {
            empty: false,
            failure: Some(Failure::Encode),
            expected: &["encode"],
        },
        Scenario {
            empty: false,
            failure: Some(Failure::Append),
            expected: &["encode", "append"],
        },
        Scenario {
            empty: false,
            failure: None,
            expected: &["encode", "append", "cache"],
        },
    ] {
        let events = RefCell::new(Vec::new());
        let log = Journal {
            events: &events,
            fails: case.failure == Some(Failure::Append),
        };
        let result = flush(
            "group",
            Delta {
                empty: case.empty,
                failure: case.failure,
            },
            &log,
            &events,
            0,
        )
        .await;
        assert!(result.err() == case.failure);
        assert!(events.into_inner() == case.expected);
    }
}
