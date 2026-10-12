//! Bounded retries for authorization decisions that may lag metadata apply.

use std::{fmt::Debug, future::Future, time::Duration};

use assert2::assert;

pub(crate) trait ErrorCodeRows {
    fn all_cluster_authorization_failed(&self) -> bool;
}

impl<Key> ErrorCodeRows for Vec<(Key, i16)> {
    fn all_cluster_authorization_failed(&self) -> bool {
        self.iter().all(|(_, error_code)| *error_code == 31)
    }
}

pub(crate) fn all_cluster_authorization_failed<Rows: ErrorCodeRows>(rows: &Rows) -> bool {
    rows.all_cluster_authorization_failed()
}

#[bon::builder]
pub(crate) async fn retry_until<T, Attempt, AttemptFuture, Complete>(
    mut attempt: Attempt,
    mut is_complete: Complete,
    #[builder(default = Duration::from_secs(5))] timeout: Duration,
    #[builder(default = Duration::from_millis(100))] interval: Duration,
) -> T
where
    T: Debug,
    Attempt: FnMut() -> AttemptFuture,
    AttemptFuture: Future<Output = T>,
    Complete: FnMut(&T) -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let response = attempt().await;
        if is_complete(&response) {
            return response;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "response did not reach its expected state before the retry deadline: {response:?}"
        );
        tokio::time::sleep(interval).await;
    }
}
