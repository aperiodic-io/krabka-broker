//! Decoding keeps version, request ownership and the native operation's control flow.

use {assert2::assert, std::cell::Cell};

type ApiVersion = i16;
type DispatchResult = Result<Vec<u8>, &'static str>;

struct Broker {
    operations: Cell<usize>,
}

struct RequestContext<'a> {
    principal: &'a str,
    correlation_id: u8,
}

struct Request {
    value: u8,
}

trait Decode: Sized {
    fn decode(bytes: &[u8], version: ApiVersion) -> Result<Self, &'static str>;
}

impl Decode for Request {
    fn decode(bytes: &[u8], version: ApiVersion) -> Result<Self, &'static str> {
        if version != 2 {
            return Err("unsupported version");
        }
        let &value = bytes.first().ok_or("truncated request")?;
        if value == 0xff {
            return Err("invalid request");
        }
        Ok(Self { value })
    }
}

mod handlers {
    use super::{ApiVersion, Decode};

    pub(crate) fn decode_request<R: Decode>(
        bytes: &[u8],
        version: ApiVersion,
    ) -> Result<R, &'static str> {
        R::decode(bytes, version)
    }
}

mod immutable {
    use super::{ApiVersion, Broker, DispatchResult, Request, RequestContext};

    /// Native documentation and tracing remain attached to the entry point.
    #[krabka_macros::decode_dispatch(request = Request, binding(req))]
    #[tracing::instrument(skip_all)]
    pub(super) async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    ) -> DispatchResult {
        broker.operations.set(broker.operations.get() + 1);
        if ctx.principal != "admin" {
            return Err("operation denied");
        }
        Ok(vec![req.value, req_bytes[0], ctx.correlation_id])
    }
}

#[tokio::test]
async fn successful_decode_preserves_context_raw_bytes_and_early_answers() {
    let broker = Broker {
        operations: Cell::new(0),
    };
    for (principal, expected) in [
        ("admin", Ok(vec![7, 7, 42])),
        ("guest", Err("operation denied")),
    ] {
        let ctx = RequestContext {
            principal,
            correlation_id: 42,
        };
        assert!(immutable::dispatch(&broker, 2, &[7, 99], &ctx).await == expected);
    }
    assert!(broker.operations.get() == 2);
}

#[tokio::test]
async fn failed_decoding_never_enters_the_original_body() {
    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 1,
    };
    for (version, bytes, error) in [
        (1, &b"\x07"[..], "unsupported version"),
        (2, &b""[..], "truncated request"),
        (2, &b"\xff"[..], "invalid request"),
    ] {
        assert!(immutable::dispatch(&broker, version, bytes, &ctx).await == Err(error));
    }
    assert!(broker.operations.get() == 0);
}

#[tokio::test]
async fn mutable_request_binding_remains_an_owned_local() {
    krabka_macros::decoded_dispatch! {
        async fn dispatch(
            source: &Broker,
            negotiated: ApiVersion,
            body: &[u8],
            caller: &RequestContext<'_>,
        )
        [request = Request, binding(mut req)] -> DispatchResult
        {
            tokio::task::yield_now().await;
            req.value += u8::try_from(negotiated).unwrap();
            source.operations.set(usize::from(req.value));
            Ok(vec![req.value, caller.correlation_id])
        }
    }

    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 42,
    };
    assert!(dispatch(&broker, 2, &[7], &ctx).await == Ok(vec![9, 42]));
    assert!(broker.operations.get() == 9);
}

#[test]
fn synchronous_handle_preserves_its_correlation_parameter() {
    #[krabka_macros::decode_dispatch(request = Request, binding(req))]
    fn handle(
        broker: &Broker,
        version: ApiVersion,
        correlation: u8,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    ) -> DispatchResult {
        broker.operations.set(usize::from(req.value));
        Ok(vec![req.value, correlation, ctx.correlation_id])
    }

    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 42,
    };
    assert!(handle(&broker, 2, 99, &[7], &ctx) == Ok(vec![7, 99, 42]));
    assert!(handle(&broker, 2, 99, &[], &ctx) == Err("truncated request"));
    assert!(broker.operations.get() == 7);
}

mod extension_handlers {
    use super::{ApiVersion, Broker, Decode, DispatchResult, Request, RequestContext};

    struct MarkerRequest(Request);

    impl Decode for MarkerRequest {
        fn decode(bytes: &[u8], version: ApiVersion) -> Result<Self, &'static str> {
            Request::decode(bytes, version).map(Self)
        }
    }

    pub(super) mod control {
        use super::{ApiVersion, Broker, DispatchResult, Request, RequestContext};

        #[krabka_macros::extension_wire(
            name = "handle_extension_control",
            api = "ExtensionControl",
            level = "info"
        )]
        pub(crate) async fn handle(
            broker: &Broker,
            req: Request,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> DispatchResult {
            broker.operations.set(broker.operations.get() + 1);
            tokio::task::yield_now().await;
            if ctx.principal == "guest" {
                Err("control refused")
            } else {
                Ok(vec![req.value, ctx.correlation_id])
            }
        }
    }

    pub(super) mod marker {
        use super::{ApiVersion, Broker, DispatchResult, MarkerRequest, RequestContext};

        #[krabka_macros::extension_wire(
            name = "handle_extension_marker",
            api = "ExtensionMarker",
            level = "debug"
        )]
        pub(crate) async fn handle(
            broker: &Broker,
            req: MarkerRequest,
            version: ApiVersion,
            _ctx: &RequestContext<'_>,
        ) -> DispatchResult {
            if req.0.value == 0 {
                return Err("marker refused");
            }
            broker.operations.set(broker.operations.get() + 1);
            Ok(vec![req.0.value])
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn extension_spans_wrap_decoding_and_native_operations_at_their_original_levels() {
    use {
        extension_handlers::{control, marker},
        std::sync::Mutex,
        tracing_subscriber::fmt::{format::FmtSpan, writer::MakeWriter as _},
    };

    static OUTPUT: Mutex<Vec<u8>> = Mutex::new(Vec::new());
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::NEW)
        .with_writer(|| OUTPUT.make_writer())
        .finish();
    let _tracing = tracing::subscriber::set_default(subscriber);
    let broker = Broker {
        operations: Cell::new(0),
    };
    let mut ctx = RequestContext {
        principal: "admin",
        correlation_id: 42,
    };

    // Trailing bytes remain ignored and the legacy correlation slot remains unused.
    assert!(control::handle(&broker, 2, 99, &[7, 88], &ctx).await == Ok(vec![7, 42]));
    assert!(control::handle(&broker, 2, 99, &[], &ctx).await == Err("truncated request"));
    assert!(broker.operations.get() == 1);
    ctx.principal = "guest";
    assert!(control::handle(&broker, 2, 99, &[7], &ctx).await == Err("control refused"));

    assert!(marker::handle(&broker, 2, 99, &[8, 88], &ctx).await == Ok(vec![8]));
    assert!(marker::handle(&broker, 2, 99, &[0xff], &ctx).await == Err("invalid request"));
    assert!(marker::handle(&broker, 2, 99, &[0], &ctx).await == Err("marker refused"));
    assert!(broker.operations.get() == 3);

    let output = String::from_utf8(OUTPUT.lock().unwrap().clone()).unwrap();
    let lines: Vec<_> = output.lines().collect();
    assert!(lines.len() == 10);
    for (line, level, operation, api) in [
        (
            lines[0],
            "INFO",
            "handle_extension_control",
            "ExtensionControl",
        ),
        (
            lines[1],
            "INFO",
            "handle_extension_control",
            "ExtensionControl",
        ),
        (
            lines[2],
            "ERROR",
            "handle_extension_control",
            "ExtensionControl",
        ),
        (
            lines[3],
            "INFO",
            "handle_extension_control",
            "ExtensionControl",
        ),
        (
            lines[4],
            "ERROR",
            "handle_extension_control",
            "ExtensionControl",
        ),
        (
            lines[5],
            "DEBUG",
            "handle_extension_marker",
            "ExtensionMarker",
        ),
        (
            lines[6],
            "DEBUG",
            "handle_extension_marker",
            "ExtensionMarker",
        ),
        (
            lines[7],
            "ERROR",
            "handle_extension_marker",
            "ExtensionMarker",
        ),
        (
            lines[8],
            "DEBUG",
            "handle_extension_marker",
            "ExtensionMarker",
        ),
        (
            lines[9],
            "ERROR",
            "handle_extension_marker",
            "ExtensionMarker",
        ),
    ] {
        assert!(line.contains(level));
        assert!(line.contains(operation));
        assert!(line.contains(&format!("api=\"{api}\"")));
    }
    for (line, error) in [
        (lines[2], "truncated request"),
        (lines[4], "control refused"),
        (lines[7], "invalid request"),
        (lines[9], "marker refused"),
    ] {
        assert!(line.contains(error));
    }
    for skipped in [
        "principal=",
        "req_bytes=",
        "version=",
        "correlation_id=",
        "broker=",
    ] {
        assert!(!output.contains(skipped));
    }
}
