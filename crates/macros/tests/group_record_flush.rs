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

#[yare::parameterized(
    empty_delta = { true, None, &[][..] },
    encode_failure = { false, Some(Failure::Encode), &["encode"][..] },
    append_failure = { false, Some(Failure::Append), &["encode", "append"][..] },
    successful_flush = { false, None, &["encode", "append", "cache"][..] },
)]
#[test_macro(tokio::test)]
async fn empty_and_failed_deltas_never_publish_a_cache_update(
    empty: bool,
    failure: Option<Failure>,
    expected: &[&str],
) {
    let events = RefCell::new(Vec::new());
    let log = Journal {
        events: &events,
        fails: failure == Some(Failure::Append),
    };
    let result = flush("group", Delta { empty, failure }, &log, &events, 0).await;
    assert2::assert!(result.err() == failure);
    assert2::assert!(events.into_inner() == expected);
}
