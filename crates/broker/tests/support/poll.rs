//! Bounded wire-response retries, whose decisions can lag a committed metadata image.

use std::{
    future::Future,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct RetrySetup {
    #[default(Duration::from_secs(10))]
    pub timeout: Duration,
    #[default(Duration::from_millis(50))]
    pub backoff: Duration,
}

/// Return the first accepted response, or the final response after the original deadline.
/// The response predicate runs before the timeout test, and request failures propagate.
///
/// # Errors
/// Returns the request error without retrying it.
#[bon::builder]
pub async fn retry_response<R, E, F, Fut>(
    #[builder(default = Duration :: from_secs (10))] timeout: Duration,
    #[builder(default = Duration :: from_millis (50))] backoff: Duration,
    mut request: F,
    ready: impl Fn(&R) -> bool,
) -> Result<R, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    let setup = RetrySetup { timeout, backoff };
    let deadline = Instant::now() + setup.timeout;
    loop {
        let response = request().await?;
        if ready(&response) || Instant::now() > deadline {
            return Ok(response);
        }
        tokio::time::sleep(setup.backoff).await;
    }
}

/// Bound one cluster step with the caller's timeout and diagnostic.
pub async fn within<F: Future>(what: &str, timeout: Duration, future: F) -> F::Output {
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| panic!("{what} did not finish within {timeout:?}"))
}
