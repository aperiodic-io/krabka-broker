//! Shared lifecycle for the broker's auxiliary HTTP listeners.

pub(crate) fn spawn(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    shutdown: tokio_util::sync::CancellationToken,
    on_error: impl FnOnce(std::io::Error) + Send + 'static,
) {
    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });
        if let Err(error) = server.await {
            on_error(error);
        }
    });
}
