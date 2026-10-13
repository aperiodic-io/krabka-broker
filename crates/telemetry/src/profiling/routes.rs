//! The pprof HTTP routes and the sampling they drive.
//!
//! The module holds the `GET /debug/pprof/profile` CPU handler, the optional
//! `GET /debug/pprof/heap` handler behind the `heap-profiling` feature, the
//! gzip step that produces the pprof file format, and the router builders that
//! bind a `ProfilingConfig` to those handlers as axum state.

#[cfg(unix)]
use krabka_units::convert::TimeExt as _;
#[cfg(any(unix, test))]
use krabka_units::{Time, secs};
use {
    super::config::{ProfilingConfig, ProfilingError},
    axum::{
        Router,
        extract::{Query, State},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
    },
    serde::Deserialize,
};

#[derive(Debug, Deserialize)]
struct CpuQuery {
    #[cfg_attr(not(unix), allow(dead_code))]
    seconds: Option<u64>,
}

#[cfg(all(unix, feature = "heap-profiling"))]
#[derive(Debug, Deserialize)]
struct HeapQuery {
    seconds: Option<u64>,
}

/// CPU profile in pprof protobuf, sampled for `?seconds=N`.
///
/// The default is 30 seconds, and the default configuration clamps the value
/// to `1..=60` seconds.
#[cfg(unix)]
async fn cpu_profile(
    State(config): State<ProfilingConfig>,
    Query(q): Query<CpuQuery>,
) -> axum::response::Response {
    // pprof::protos::Message re-exports the prost 0.12 Message trait bundled
    // inside the pprof crate, which is the version Profile was generated with.
    use pprof::protos::Message as _;

    let duration = requested_duration(
        q.seconds,
        config.profiling_cpu_default_duration,
        config.profiling_cpu_max_duration,
    );
    let blocklist = config
        .profiling_native_frame_blocklist
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(config.profiling_cpu_sample_frequency.hertz())
        .blocklist(&blocklist)
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("profiler: {e}")).into_response();
        }
    };
    tokio::time::sleep(duration.to_std()).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("report: {e}")).into_response();
        }
    };
    let profile = match report.pprof() {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("pprof: {e}")).into_response();
        }
    };
    let body = gzip(&profile.encode_to_vec());
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        body,
    )
        .into_response()
}

/// Gzip a buffer.
///
/// The pprof file format is a gzipped `Profile` protobuf.
#[cfg(unix)]
fn gzip(raw: &[u8]) -> Vec<u8> {
    use std::io::Write as _;

    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(raw.len() / 2),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(raw)
        .expect("gzip of in-memory buffer is infallible");
    encoder
        .finish()
        .expect("gzip finish of in-memory buffer is infallible")
}

/// Stub for non-Unix targets: CPU profiling is unavailable.
// cargo-mutants: non-Unix stub is not built or exercised on the default Linux mutation run.
#[cfg(not(unix))]
#[cfg_attr(test, mutants::skip)]
async fn cpu_profile(
    _config: State<ProfilingConfig>,
    _q: Query<CpuQuery>,
) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "CPU profiling requires a Unix target",
    )
        .into_response()
}

// cargo-mutants: optional heap-profiling route is feature-gated out of the default mutation run.
#[cfg(all(unix, feature = "heap-profiling"))]
#[cfg_attr(test, mutants::skip)]
async fn heap_profile(
    State(config): State<ProfilingConfig>,
    Query(q): Query<HeapQuery>,
) -> axum::response::Response {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "jemalloc profiling not enabled (build with --features heap-profiling and set MALLOC_CONF)",
        )
            .into_response();
    };
    let mut ctl = ctl.lock().await;
    let activated_here = !ctl.activated();
    if activated_here {
        if let Err(e) = ctl.activate() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("jemalloc prof activate: {e}"),
            )
                .into_response();
        }
        let duration = requested_duration(
            q.seconds,
            config.profiling_heap_default_duration,
            config.profiling_heap_max_duration,
        );
        tokio::time::sleep(duration.to_std()).await;
    }
    let dump = ctl.dump_pprof();
    if activated_here && let Err(e) = ctl.deactivate() {
        tracing::warn!(error = %e, "could not deactivate jemalloc profiling after heap dump");
    }
    match dump {
        Ok(pprof) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            pprof,
        )
            .into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("heap dump: {e}")).into_response(),
    }
}

/// The pprof routes with an explicit policy.
///
/// The router always has the CPU route, which returns 503 on non-Unix targets.
/// The router has the heap route only with the `heap-profiling` feature, and
/// only on Unix.
///
/// # Errors
/// Returns an error when related profiling duration bounds are invalid.
pub fn pprof_router_with_config(config: ProfilingConfig) -> Result<Router, ProfilingError> {
    config.validate().map_err(ProfilingError::Config)?;
    Ok(pprof_router_unchecked(config))
}

fn pprof_router_unchecked(config: ProfilingConfig) -> Router {
    let router = Router::new().route("/debug/pprof/profile", get(cpu_profile));
    #[cfg(all(unix, feature = "heap-profiling"))]
    let router = router.route("/debug/pprof/heap", get(heap_profile));
    router.with_state(config)
}

/// The pprof routes with the compatible default policy.
pub fn pprof_router() -> Router { pprof_router_unchecked(ProfilingConfig::default()) }

#[cfg(any(unix, test))]
fn requested_duration(seconds: Option<u64>, default: Time, maximum: Time) -> Time {
    seconds
        .map_or(default, |seconds| {
            secs(u32::try_from(seconds.max(1)).unwrap_or(u32::MAX))
        })
        .min(maximum)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{millis, minutes},
    };

    #[test]
    fn requested_profile_duration_uses_configured_default_floor_and_cap() {
        assert!(requested_duration(None, secs(2), secs(5)) == secs(2));
        assert!(requested_duration(Some(0), secs(2), secs(5)) == secs(1));
        assert!(requested_duration(Some(3), secs(2), secs(5)) == secs(3));
        assert!(requested_duration(Some(9), secs(2), secs(5)) == secs(5));
        assert!(
            (ProfilingConfig {
                profiling_cpu_max_duration: millis(500),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err())
        );
        assert!(
            (ProfilingConfig {
                profiling_heap_default_duration: minutes(1),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err())
        );
    }
}
