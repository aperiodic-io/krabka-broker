//! Binding and supervising the admin HTTP server that serves the pprof routes.
//!
//! The module holds the `serve_*` and `spawn_*` entry points, the shared bind
//! step behind them, and the `KRABKA_ADMIN_LISTEN_ADDR` lookup that the
//! environment-address variants use. It is separate from the routes so that
//! the listener lifecycle stays in one place.

use {
    super::{
        config::{ProfilingConfig, ProfilingError},
        routes::{pprof_router, pprof_router_with_config},
    },
    axum::Router,
    std::net::SocketAddr,
};

/// Bind an admin HTTP server on `addr`.
///
/// The server serves `pprof_router()` merged with `extra`, for example a
/// `/metrics` route. This function spawns the server and returns after the
/// bind.
/// # Errors
/// Returns an error if the listener cannot bind or the HTTP server fails.
pub async fn serve_admin(addr: SocketAddr, extra: Router) -> std::io::Result<()> {
    serve_router(addr, pprof_router().merge(extra)).await
}

async fn serve_router(addr: SocketAddr, app: Router) -> std::io::Result<()> {
    let task = spawn_router(addr, app).await?;
    tokio::spawn(async move {
        match task.await {
            Ok(Ok(())) => tracing::warn!("admin server stopped unexpectedly"),
            Ok(Err(error)) => tracing::warn!(%error, "admin server error"),
            Err(error) => tracing::warn!(%error, "admin server task failed"),
        }
    });
    Ok(())
}

async fn spawn_router(
    addr: SocketAddr,
    app: Router,
) -> std::io::Result<tokio::task::JoinHandle<std::io::Result<()>>> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "profiling admin server listening");
    Ok(tokio::spawn(
        async move { axum::serve(listener, app).await },
    ))
}

/// Bind a profiling admin server with explicit policy.
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
pub async fn serve_admin_with_config(
    addr: SocketAddr,
    extra: Router,
    config: ProfilingConfig,
) -> Result<(), ProfilingError> {
    let app = pprof_router_with_config(config)?.merge(extra);
    serve_router(addr, app).await?;
    Ok(())
}

/// Bind a profiling admin server and return its task for lifecycle supervision.
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
pub async fn spawn_admin_with_config(
    addr: SocketAddr,
    extra: Router,
    config: ProfilingConfig,
) -> Result<tokio::task::JoinHandle<std::io::Result<()>>, ProfilingError> {
    let app = pprof_router_with_config(config)?.merge(extra);
    Ok(spawn_router(addr, app).await?)
}

/// Wait for a supervised admin task, treating every terminal outcome as an error.
///
/// # Errors
/// Returns the server error, join error, or an unexpected clean-exit error.
pub async fn await_admin_exit(
    task: tokio::task::JoinHandle<std::io::Result<()>>,
) -> std::io::Result<()> {
    match task.await {
        Ok(Ok(())) => Err(std::io::Error::other("admin server stopped unexpectedly")),
        Ok(Err(error)) => Err(error),
        Err(error) => Err(std::io::Error::other(format!(
            "admin server task failed: {error}"
        ))),
    }
}

fn admin_address(default_addr: &str) -> SocketAddr {
    let raw =
        std::env::var("KRABKA_ADMIN_LISTEN_ADDR").unwrap_or_else(|_| default_addr.to_string());
    raw.parse()
        .unwrap_or_else(|e| panic!("invalid KRABKA_ADMIN_LISTEN_ADDR `{raw}`: {e}"))
}

/// Like [`serve_admin`], but with the bind address from the environment.
///
/// This function reads `KRABKA_ADMIN_LISTEN_ADDR` and falls back to
/// `default_addr`.
/// # Errors
/// Returns an error if the listener cannot bind or the HTTP server fails.
///
/// # Panics
///
/// Panics if the environment variable, or `default_addr` when it is absent,
/// is not a socket address.
pub async fn serve_admin_from_env(default_addr: &str) -> std::io::Result<()> {
    serve_admin_from_env_with(default_addr, Router::new()).await
}

/// Like [`serve_admin_from_env`], but it also merges `extra` with the pprof routes.
///
/// `extra` is, for example, a `GET /metrics` route. Services that expose
/// Prometheus metrics call this function with their `/metrics` router, and the
/// exporter thus shares the admin port.
/// # Errors
/// Returns an error if the listener cannot bind or the HTTP server fails.
/// # Panics
/// Panics if `KRABKA_ADMIN_LISTEN_ADDR`, or `default_addr` when it is absent,
/// is not a socket address. Router merging also panics if `extra` conflicts
/// with an existing profiling route.
pub async fn serve_admin_from_env_with(default_addr: &str, extra: Router) -> std::io::Result<()> {
    let addr = admin_address(default_addr);
    serve_admin(addr, extra).await
}

/// Like [`serve_admin_from_env_with`] with explicit profiling policy.
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
///
/// # Panics
/// Panics when `KRABKA_ADMIN_LISTEN_ADDR`, or the fallback `default_addr`, is not a socket address. This
/// behavior is the same as the default-compatible wrapper's behavior.
pub async fn serve_admin_from_env_with_config(
    default_addr: &str,
    extra: Router,
    config: ProfilingConfig,
) -> Result<(), ProfilingError> {
    let addr = admin_address(default_addr);
    serve_admin_with_config(addr, extra, config).await
}

/// Environment-address variant of [`spawn_admin_with_config`].
///
/// # Errors
/// Returns an error for invalid profiling configuration or listener failure.
///
/// # Panics
/// Panics when `KRABKA_ADMIN_LISTEN_ADDR`, or the fallback `default_addr`, is not a socket address.
pub async fn spawn_admin_from_env_with_config(
    default_addr: &str,
    extra: Router,
    config: ProfilingConfig,
) -> Result<tokio::task::JoinHandle<std::io::Result<()>>, ProfilingError> {
    let addr = admin_address(default_addr);
    spawn_admin_with_config(addr, extra, config).await
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[tokio::test]
    async fn supervised_admin_exit_classifies_clean_error_and_join_outcomes() {
        let clean = tokio::spawn(async { Ok(()) });
        assert!(
            await_admin_exit(clean).await.unwrap_err().to_string()
                == "admin server stopped unexpectedly"
        );

        let io_error = tokio::spawn(async { Err(std::io::Error::other("socket failed")) });
        assert!(await_admin_exit(io_error).await.unwrap_err().to_string() == "socket failed");

        let panic = tokio::spawn(async {
            panic!("admin panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        assert!(
            await_admin_exit(panic)
                .await
                .unwrap_err()
                .to_string()
                .starts_with("admin server task failed:")
        );
    }
}
