//! Caller-level command exits and native reassignment evaluation are executable contracts.

use {
    assert2::assert,
    std::{
        cell::{Cell, RefCell},
        future::{Ready, ready},
        num::Wrapping,
    },
};

#[derive(Clone, Copy)]
enum ReplyMode {
    Success,
    Refusal,
    TransportFailure,
}
#[derive(Clone, Copy)]
struct Request {
    mode: ReplyMode,
}
struct Reply {
    error_code: i16,
    error_message: Option<String>,
    value: i32,
}
struct Client {
    sends: Cell<usize>,
}
#[derive(Debug, PartialEq, Eq)]
struct Failure;
struct TransportFailure;
impl From<TransportFailure> for Failure {
    fn from(_: TransportFailure) -> Self { Self }
}
impl Client {
    fn send(&self, request: Request) -> Ready<Result<Reply, TransportFailure>> {
        self.sends.set(self.sends.get() + 1);
        ready(match request.mode {
            ReplyMode::Success => Ok(Reply {
                error_code: 0,
                error_message: None,
                value: 42,
            }),
            ReplyMode::Refusal => Ok(Reply {
                error_code: 31,
                error_message: Some("denied".into()),
                value: 42,
            }),
            ReplyMode::TransportFailure => Err(TransportFailure),
        })
    }
}
fn report_error(code: i16, message: Option<&str>) -> i32 {
    assert!(code == 31 && message == Some("denied"));
    -i32::from(code)
}
async fn command(
    client: &Client,
    request: Request,
    continued: &Cell<bool>,
) -> Result<i32, Failure> {
    let reply = krabka_macros::guard_response!(client, request);
    continued.set(true);
    Ok(reply.value)
}

#[yare::parameterized(
    success = { ReplyMode::Success, Ok(42), true },
    refusal = { ReplyMode::Refusal, Ok(-31), false },
    transport_failure = { ReplyMode::TransportFailure, Err(Failure), false },
)]
#[test_macro(tokio::test)]
async fn command_errors_return_from_the_caller_before_success_interpretation(
    mode: ReplyMode,
    expected: Result<i32, Failure>,
    continues: bool,
) {
    let client = Client {
        sends: Cell::new(0),
    };
    let continued = Cell::new(false);
    let result = command(&client, Request { mode }, &continued).await;
    assert2::assert!(result == expected && continued.get() == continues && client.sends.get() == 1);
}

#[derive(Default)]
struct Assignment {
    replicas: Vec<i32>,
    isr: Vec<i32>,
    adding: Vec<i32>,
    removing: Vec<i32>,
    leader: Wrapping<i32>,
    directories: Vec<u8>,
}
struct Builder<'a> {
    order: &'a RefCell<Vec<&'static str>>,
    fields: Assignment,
}
impl Builder<'_> {
    fn replicas(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("replicas");
        self.fields.replicas = value;
        self
    }

    fn isr(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("isr");
        self.fields.isr = value;
        self
    }

    fn adding(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("adding");
        self.fields.adding = value;
        self
    }

    fn removing(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("removing");
        self.fields.removing = value;
        self
    }

    fn leader(mut self, value: i32) -> Self {
        self.order.borrow_mut().push("leader");
        self.fields.leader = Wrapping(value);
        self
    }

    fn directories(mut self, value: Vec<u8>) -> Self {
        self.order.borrow_mut().push("directories");
        self.fields.directories = value;
        self
    }
}

#[test]
fn reassignment_sources_run_once_before_setters_and_keep_all_owned_fields() {
    let order = RefCell::new(Vec::new());
    let assignment = Assignment {
        replicas: vec![1, 2],
        isr: vec![1],
        adding: vec![3],
        removing: vec![2],
        leader: Wrapping(1),
        directories: vec![7, 8],
    };
    let result = krabka_macros::reassignment_fields! {
        assignment { order.borrow_mut().push("assignment"); assignment }
        builder { order.borrow_mut().push("builder"); Builder { order: &order, fields: Assignment::default() } }
    };
    assert!(
        *order.borrow()
            == [
                "assignment",
                "builder",
                "replicas",
                "isr",
                "adding",
                "removing",
                "leader",
                "directories"
            ]
    );
    assert!(result.fields.replicas == [1, 2] && result.fields.isr == [1]);
    assert!(result.fields.adding == [3] && result.fields.removing == [2]);
    assert!(result.fields.leader == Wrapping(1) && result.fields.directories == [7, 8]);
}
