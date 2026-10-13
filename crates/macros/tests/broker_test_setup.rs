//! Resource retention, visible identities and refusal matrices use flat declarations.

use {
    assert2::assert,
    std::{
        cell::RefCell,
        ptr,
        sync::{Arc, Mutex},
    },
};

type Events = Arc<Mutex<Vec<String>>>;
thread_local! { static CURRENT_EVENTS: RefCell<Option<Events>> = const { RefCell::new(None) }; }

fn record(events: &Events, event: impl Into<String>) { events.lock().unwrap().push(event.into()); }
fn active_events() -> Events {
    CURRENT_EVENTS.with_borrow(|events| events.as_ref().unwrap().clone())
}
fn install_events(events: &Events) {
    CURRENT_EVENTS.with_borrow_mut(|current| *current = Some(Arc::clone(events)));
}

fn assert_context_borrows(
    context: &handlers::RequestContext<'_>,
    identity: &Identity,
    address: &Address,
) {
    assert!(ptr::eq(
        ptr::from_ref(context.identity),
        ptr::from_ref(identity)
    ));
    assert!(ptr::eq(
        ptr::from_ref(context.address),
        ptr::from_ref(address)
    ));
}

struct Broker {
    events: Events,
    enabled: bool,
}
struct Handle(Arc<Broker>);
struct Directory(Events);

impl Handle {
    fn broker_arc_for_test(&self) -> Arc<Broker> {
        record(&self.0.events, "broker");
        Arc::clone(&self.0)
    }

    async fn shutdown(&self) {
        tokio::task::yield_now().await;
        record(&self.0.events, "shutdown");
    }
}

impl Drop for Directory {
    fn drop(&mut self) { record(&self.0, "directory dropped"); }
}

async fn start<const MAJOR: u8, const MINOR: u8>(events: Events) -> (Handle, Directory) {
    assert!((MAJOR, MINOR) == (1, 2));
    tokio::task::yield_now().await;
    record(&events, "start");
    (
        Handle(Arc::new(Broker {
            events: Arc::clone(&events),
            enabled: true,
        })),
        Directory(events),
    )
}

struct Identity {
    name: String,
    events: Events,
}
struct Address(Events);

impl Drop for Identity {
    fn drop(&mut self) { record(&self.events, format!("identity dropped:{}", self.name)); }
}

impl Drop for Address {
    fn drop(&mut self) { record(&self.0, "address dropped"); }
}

mod handlers {
    use super::{Address, Identity, record};

    pub(crate) struct RequestContext<'a> {
        pub identity: &'a Identity,
        pub address: &'a Address,
        pub client: &'a str,
        pub connection: &'a str,
        pub sendfile: bool,
        pub listener: &'a str,
    }

    impl<'a> RequestContext<'a> {
        pub(crate) fn new(
            identity: &'a Identity,
            address: &'a Address,
            client: &'a str,
            connection: &'a str,
            sendfile: bool,
            listener: &'a str,
        ) -> Self {
            record(&identity.events, format!("context:{client}:{connection}"));
            Self {
                identity,
                address,
                client,
                connection,
                sendfile,
                listener,
            }
        }
    }

    pub(crate) mod test_support {
        use super::super::{active_events, record};

        pub(crate) fn allow_all_authorizer() -> bool {
            record(&active_events(), "allow policy");
            true
        }

        pub(crate) fn deny_all_authorizer() -> bool {
            record(&active_events(), "deny policy");
            false
        }

        pub(crate) fn controller_peer_authorizer() -> bool {
            record(&active_events(), "controller peer policy");
            true
        }
    }
}

mod test_support {
    use super::{
        Address, Arc, Broker, Directory, Events, Handle, Identity, active_events,
        handlers::RequestContext, install_events, record,
    };

    pub(crate) fn principal(name: &str) -> Identity {
        let events = active_events();
        record(&events, format!("identity:{name}"));
        Identity {
            name: name.into(),
            events,
        }
    }

    pub(crate) fn peer() -> Address {
        let events = active_events();
        record(&events, "address");
        Address(events)
    }

    pub(crate) async fn wait_for_controller_leader(broker: &Broker) {
        record(&broker.events, "leader ready");
        tokio::task::yield_now().await;
    }

    pub(crate) fn request_context<'a>(
        identity: &'a Identity,
        address: &'a Address,
        client: &'a str,
    ) -> RequestContext<'a> {
        RequestContext::new(
            identity,
            address,
            client,
            "test-connection",
            false,
            "PLAINTEXT",
        )
    }

    pub(crate) struct Authorization(pub Events);
    pub(crate) struct ShareStarter {
        authorizer: Option<Authorization>,
        enabled: bool,
    }

    pub(crate) fn start_share_broker() -> ShareStarter {
        ShareStarter {
            authorizer: None,
            enabled: true,
        }
    }

    impl ShareStarter {
        pub(crate) fn authorizer(mut self, authorizer: Authorization) -> Self {
            self.authorizer = Some(authorizer);
            self
        }

        pub(crate) fn support(mut self, enabled: bool) -> Self {
            self.enabled = enabled;
            self
        }

        pub(crate) async fn call(self) -> (Handle, Directory) {
            tokio::task::yield_now().await;
            let events = self.authorizer.unwrap().0;
            install_events(&events);
            record(&events, format!("start:{}", self.enabled));
            (
                Handle(Arc::new(Broker {
                    events: Arc::clone(&events),
                    enabled: self.enabled,
                })),
                Directory(events),
            )
        }
    }
}

fn test_context<'a>(identity: &'a Identity, address: &'a Address) -> handlers::RequestContext<'a> {
    test_support::request_context(identity, address, "builder-client")
}

async fn configured_start(enabled: bool) -> (Handle, Directory) {
    tokio::task::yield_now().await;
    let events = active_events();
    record(&events, format!("start:{enabled}"));
    (
        Handle(Arc::new(Broker {
            events: Arc::clone(&events),
            enabled,
        })),
        Directory(events),
    )
}

#[tokio::test]
async fn combined_policy_keeps_the_selected_starter_and_readiness_before_context() {
    let events = Events::default();
    install_events(&events);
    {
        krabka_macros::broker_test_setup! {
            bind(handle, directory, broker, context);
            boot [configured_start] allow_all;
            ready controller_leader;
            user ["admin"];
        }
        assert!(broker.enabled && context.identity.name == "admin");
        assert!(Arc::strong_count(&directory.0) > 1);
        handle.shutdown().await;
    }
    assert!(
        *events.lock().unwrap()
            == [
                "allow policy",
                "start:true",
                "broker",
                "leader ready",
                "identity:admin",
                "address",
                "context:builder-client:test-connection",
                "shutdown",
                "address dropped",
                "identity dropped:admin",
                "directory dropped"
            ]
    );
    events.lock().unwrap().clear();
    {
        krabka_macros::broker_test_setup! {
            bind(handle, _directory, broker, context);
            boot [configured_start] deny_all;
            user ["denied"];
        }
        assert!(!broker.enabled && context.identity.name == "denied");
        handle.shutdown().await;
    }
    assert!(events.lock().unwrap()[..3] == ["deny policy", "start:false", "broker"]);
    events.lock().unwrap().clear();
    {
        krabka_macros::broker_test_setup! {
            bind(handle, _directory, broker, principal, peer, context);
            boot [configured_start] controller_peer;
            ready controller_leader;
            user [{ record(&events, "name expression"); "alice" }];
            client [{ record(&events, "client expression"); "visible-client" }];
        }
        assert!(broker.enabled && context.client == "visible-client");
        assert_context_borrows(&context, &principal, &peer);
        handle.shutdown().await;
    }
    assert!(
        events.lock().unwrap()[..8]
            == [
                "controller peer policy",
                "start:true",
                "broker",
                "leader ready",
                "name expression",
                "identity:alice",
                "address",
                "client expression",
            ]
    );
    events.lock().unwrap().clear();
    {
        krabka_macros::broker_test_setup! {
            bind(handle, _directory, broker, principal, peer);
            boot [configured_start] allow_all;
            user ["without-context"];
        }
        assert!(broker.enabled && principal.name == "without-context");
        assert!(Arc::ptr_eq(&principal.events, &peer.0));
        handle.shutdown().await;
    }
}

#[tokio::test]
async fn boot_runs_once_before_readiness_and_keeps_ready_bindings_and_directory_alive() {
    let events = Events::default();
    {
        krabka_macros::broker_test_setup! {
            bind(handle, directory, broker);
            boot [{
                record(&events, "starter expression");
                start::<1, 2>(Arc::clone(&events))
            }];
            ready {
                let partition_directory = &directory;
                if broker.enabled {
                    record(&events, "readiness");
                } else {
                    panic!("enabled broker expected");
                }
                tokio::task::yield_now().await;
            };
        }
        assert!(ptr::eq(
            ptr::from_ref(partition_directory),
            ptr::from_ref(&directory)
        ));
        assert!(
            !events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "directory dropped")
        );
        handle.shutdown().await;
    }
    assert!(
        *events.lock().unwrap()
            == [
                "starter expression",
                "start",
                "broker",
                "readiness",
                "shutdown",
                "directory dropped"
            ]
    );
}

#[test]
fn contexts_borrow_visible_or_hygienic_bindings_and_keep_expression_order() {
    let events = Events::default();
    install_events(&events);
    {
        let __request_principal = 42;
        let __request_peer = 19;
        krabka_macros::request_test_identity! {
            bind(first);
            user ["admin"];
        }
        krabka_macros::request_test_identity! {
            bind(second);
            identity [test_support::principal("replica")];
            address [test_support::peer()];
            client [{ record(&events, "client expression"); "replica-client" }];
        }
        assert!(first.identity.name == "admin" && second.identity.name == "replica");
        assert!((__request_principal, __request_peer) == (42, 19));
        assert!(first.client == "builder-client" && second.client == "replica-client");
        assert!(
            first.connection == "test-connection"
                && !first.sendfile
                && first.listener == "PLAINTEXT"
        );
        krabka_macros::request_test_identity! {
            bind(principal, peer, context);
            identity [test_support::principal("visible")];
            address [test_support::peer()];
            client ["explicit-client"];
            connection [{ record(&events, "connection expression"); "explicit-connection" }];
        }
        assert_context_borrows(&context, &principal, &peer);
        assert!(context.connection == "explicit-connection");
        krabka_macros::request_test_identity! {
            bind(user, address);
            identity [test_support::principal("identity-only")];
            address [test_support::peer()];
        }
        assert!(user.name == "identity-only");
        assert!(ptr::eq(Arc::as_ptr(&address.0), Arc::as_ptr(&events)));
    }
    let events = events.lock().unwrap();
    assert!(
        events[..7]
            == [
                "identity:admin",
                "address",
                "context:builder-client:test-connection",
                "identity:replica",
                "address",
                "client expression",
                "context:replica-client:test-connection"
            ]
    );
    assert!(
        events
            .iter()
            .filter(|event| event.starts_with("identity dropped:"))
            .count()
            == 4
    );
    assert!(
        events
            .iter()
            .position(|event| event == "identity dropped:admin")
            .unwrap()
            > events
                .iter()
                .position(|event| event == "connection expression")
                .unwrap()
    );
}

async fn dispatch(
    broker: &Broker,
    value: u8,
    context: &handlers::RequestContext<'_>,
) -> Result<u8, &'static str> {
    assert!(context.identity.name == "alice");
    record(&broker.events, format!("dispatch:{value}"));
    tokio::task::yield_now().await;
    Ok(if broker.enabled { value } else { 0 })
}

#[tokio::test]
async fn refusal_matrix_preserves_each_cases_setup_response_assertion_and_cleanup() {
    let events = Events::default();
    let cases = [
        (
            "enabled",
            test_support::Authorization(Arc::clone(&events)),
            true,
            7,
            7,
        ),
        (
            "disabled",
            test_support::Authorization(Arc::clone(&events)),
            false,
            9,
            0,
        ),
    ];
    krabka_macros::share_refusal_matrix! {
        case(label, authorizer, enabled, value, expected) [cases];
        bind(handle, _directory, broker, context, response);
        invoke [dispatch(&broker, value, &context)];
    }
    assert!(
        *events.lock().unwrap()
            == [
                "start:true",
                "broker",
                "identity:alice",
                "address",
                "context:builder-client:test-connection",
                "dispatch:7",
                "shutdown",
                "address dropped",
                "identity dropped:alice",
                "directory dropped",
                "start:false",
                "broker",
                "identity:alice",
                "address",
                "context:builder-client:test-connection",
                "dispatch:9",
                "shutdown",
                "address dropped",
                "identity dropped:alice",
                "directory dropped"
            ]
    );
}
