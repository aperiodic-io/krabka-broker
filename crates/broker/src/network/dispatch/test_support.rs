//! Fixtures shared by the unit tests of the dispatch module and its children.

pub(super) const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

use {
    crate::{
        SslPrincipalMapper,
        broker::Broker,
        config::ListenerSpec,
        network::codec::{KafkaCodec, frame},
    },
    krabka_security::{ListenerProtocol, Principal, SaslMechanism},
    std::{net::SocketAddr, sync::Arc},
    tokio::net::{TcpListener, TcpStream},
    tokio_util::codec::Framed,
};

/// Bind the real serve loop and return its future so callers can instrument
/// it before spawning. Multiple-connection cases keep one listener and run
/// each accepted stream in order, just as their original fixtures did.
pub(super) async fn serve_loop(
    broker: Arc<Broker>,
    name: &'static str,
    protocol: ListenerProtocol,
    sasl_mechanisms: Option<Vec<SaslMechanism>>,
    mtls_principal: Option<Principal>,
    connections: usize,
) -> (SocketAddr, impl Future<Output = ()> + Send + 'static) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener addr");
    let spec = ListenerSpec {
        name: name.to_string(),
        bind_addr: addr,
        advertised: "127.0.0.1:9092".to_string(),
        protocol,
        tls_config: None,
        sasl_mechanisms,
        principal_mapper: SslPrincipalMapper::default(),
    };
    (addr, async move {
        for _ in 0..connections {
            let (stream, peer) = listener.accept().await.expect("accept");
            super::serve_connection_stream(
                broker.clone(),
                stream,
                spec.clone(),
                peer,
                mtls_principal.clone(),
            )
            .await;
        }
    })
}

pub(super) async fn plaintext_loop(
    broker: Arc<Broker>,
) -> (tokio::task::JoinHandle<()>, Framed<TcpStream, KafkaCodec>) {
    let (addr, serve) = serve_loop(
        broker,
        "PLAINTEXT",
        ListenerProtocol::Plaintext,
        None,
        None,
        1,
    )
    .await;
    let task = tokio::spawn(serve);
    (
        task,
        connect_framed(addr, "connect to the serve loop").await,
    )
}

pub(super) async fn connect_framed(
    addr: SocketAddr,
    context: &str,
) -> Framed<TcpStream, KafkaCodec> {
    let client = TcpStream::connect(addr).await.expect(context);
    frame(client, DEFAULT_MAX_FRAME_BYTES)
}
