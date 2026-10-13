//! Admission answers and failures return before the unchanged voter operation runs.

use {assert2::assert, std::cell::RefCell};

type ApiVersion = i16;
type Bytes = Vec<u8>;
type BrokerError = &'static str;

#[derive(Clone, Copy)]
enum AdmissionMode {
    Continue,
    Answer,
    Error,
}

struct Broker {
    mode: AdmissionMode,
    events: RefCell<Vec<&'static str>>,
}

struct RequestContext<'a> {
    principal: &'a str,
}

mod handlers {
    pub mod raft_voter {
        use {
            super::super::{AdmissionMode, ApiVersion, Broker, BrokerError, Bytes, RequestContext},
            assert2::assert,
            std::ops::ControlFlow,
        };

        pub struct Admitted<R> {
            pub req: R,
            pub image: u8,
            pub quorum: u8,
        }

        pub fn prelude<R: From<u8>, Refusal: AsRef<[u8]>>(
            broker: &Broker,
            version: ApiVersion,
            req_bytes: &[u8],
            ctx: &RequestContext<'_>,
            api: i16,
            gate: fn() -> bool,
            refusals: Refusal,
        ) -> std::future::Ready<Result<ControlFlow<Bytes, Admitted<R>>, BrokerError>> {
            broker.events.borrow_mut().push("admission");
            assert!(version == 2);
            assert!(api == 80);
            assert!(ctx.principal == "admin");
            assert!(gate());
            std::future::ready(match broker.mode {
                AdmissionMode::Continue => Ok(ControlFlow::Continue(Admitted {
                    req: R::from(req_bytes[0]),
                    image: 2,
                    quorum: 3,
                })),
                AdmissionMode::Answer => Ok(ControlFlow::Break(refusals.as_ref().to_vec())),
                AdmissionMode::Error => Err("admission failed"),
            })
        }
    }
}

fn gate() -> bool { true }

#[krabka_macros::voter_admission(request = u8, api = 80, gate = gate, refusals = vec![31_u8])]
async fn handle(
    broker: &Broker,
    version: ApiVersion,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    broker.events.borrow_mut().push("operation");
    if req == 0 {
        Err("operation failed")
    } else {
        Ok([req, image, quorum]
            .iter()
            .map(|&value| value + 1)
            .collect())
    }
}

#[tokio::test]
async fn preserves_admission_early_answers_errors_and_operation_results() {
    for AdmissionCase {
        mode,
        value,
        expected,
        events,
    } in [
        AdmissionCase {
            mode: AdmissionMode::Continue,
            value: 7,
            expected: Ok(vec![8, 3, 4]),
            events: vec!["admission", "operation"],
        },
        AdmissionCase {
            mode: AdmissionMode::Answer,
            value: 7,
            expected: Ok(vec![31]),
            events: vec!["admission"],
        },
        AdmissionCase {
            mode: AdmissionMode::Error,
            value: 7,
            expected: Err("admission failed"),
            events: vec!["admission"],
        },
        AdmissionCase {
            mode: AdmissionMode::Continue,
            value: 0,
            expected: Err("operation failed"),
            events: vec!["admission", "operation"],
        },
    ] {
        let broker = Broker {
            mode,
            events: RefCell::new(Vec::new()),
        };
        let ctx = RequestContext { principal: "admin" };
        assert!(handle(&broker, 2, &[value], &ctx).await == expected);
        assert!(*broker.events.borrow() == events);
    }
}

struct AdmissionCase {
    mode: AdmissionMode,
    value: u8,
    expected: Result<Bytes, BrokerError>,
    events: Vec<&'static str>,
}
