//! A typed plain driver forwards the owned request and preserves transport errors.

use {
    assert2::assert,
    std::{io, net::SocketAddr},
};

struct Request {
    value: u8,
}

#[derive(Debug, PartialEq, Eq)]
struct Reply {
    value: u8,
}

mod broker {
    pub struct Broker {
        pub value: u8,
    }
}

mod handlers {
    pub struct RequestContext<'a> {
        pub principal: &'a str,
    }
}

mod test_support {
    use {
        super::{Reply, Request, broker::Broker, handlers::RequestContext},
        assert2::assert,
        std::future::{Ready, ready},
    };

    pub fn dispatch_wire(
        broker: &Broker,
        api_key: i16,
        version: i16,
        request: &Request,
        context: &RequestContext<'_>,
    ) -> Ready<Reply> {
        assert!(api_key == 42 && version == 7 && context.principal == "alice");
        ready(Reply {
            value: broker.value + request.value,
        })
    }
}

krabka_macros::wire_test_driver! {
    broker answer_probe { request: Request, response: Reply, api: 42 }
}

#[tokio::test]
async fn in_process_driver_keeps_the_request_available_after_dispatch() {
    let request = Request { value: 9 };
    let broker = broker::Broker { value: 1 };
    let context = handlers::RequestContext { principal: "alice" };
    let response = answer_probe(&broker, 7, &request, &context).await;
    assert!(response.value == 10 && request.value == 9);
}

mod support {
    pub mod wire {
        #[derive(Clone, Copy)]
        pub struct ApiRequestSetup {
            pub key: i16,
            pub version: i16,
        }
    }
}

mod kafka_wire {
    use super::{Reply, Request, SocketAddr, io, support::wire::ApiRequestSetup};

    #[derive(Clone)]
    pub struct PlainAuthenticationSetup<'a> {
        pub username: &'a str,
    }

    pub struct PlainRequestSetup<'a> {
        pub authentication: PlainAuthenticationSetup<'a>,
        pub api: ApiRequestSetup,
        pub operation: &'static str,
    }

    pub fn request_as_plain(
        address: SocketAddr,
        request: &Request,
        setup: &PlainRequestSetup<'_>,
    ) -> std::future::Ready<io::Result<Reply>> {
        assert2::assert!(address.port() == 9092);
        assert2::assert!(setup.authentication.username == "alice");
        assert2::assert!(setup.api.key == 42 && setup.api.version == 7);
        assert2::assert!(setup.operation == "probe response");
        std::future::ready(if request.value == 0 {
            Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "transport failure",
            ))
        } else {
            Ok(Reply {
                value: request.value,
            })
        })
    }
}

use {kafka_wire::PlainAuthenticationSetup, support::wire::ApiRequestSetup};

krabka_macros::wire_test_driver!(plain send_probe {
    request: Request,
    response: Reply,
    api: ApiRequestSetup { key: 42, version: 7 },
    operation: "probe response",
});

#[tokio::test]
async fn typed_driver_keeps_schema_identity_and_propagates_socket_failure() {
    let address = "127.0.0.1:9092".parse().unwrap();
    let authentication = PlainAuthenticationSetup { username: "alice" };
    assert!(
        send_probe(address, Request { value: 9 }, &authentication)
            .await
            .unwrap()
            == Reply { value: 9 }
    );
    let error = send_probe(address, Request { value: 0 }, &authentication)
        .await
        .unwrap_err();
    assert!(error.kind() == io::ErrorKind::ConnectionReset);
    assert!(error.to_string() == "transport failure");
}
