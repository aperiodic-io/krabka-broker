//! Shared restore setup for bootstrap and report assertions.

use std::path::Path;

/// Restore an archive into a fresh child directory with the requested cluster id.
pub(crate) async fn restore_fixture(
    archive_root: &Path,
    target_parent: &Path,
    cluster_id: uuid::Uuid,
) -> krabka_restore::RestoreReport {
    let args = crate::args::restore_args(
        archive_root,
        &target_parent.join("restored"),
        crate::args::RestoreOptions {
            extra: &["--cluster-id", &cluster_id.to_string()],
            ..Default::default()
        },
    );
    krabka_restore::restore(&args).await.expect("restore")
}

/// Own the target directory for as long as a test observes the restored archive.
pub(crate) struct FreshRestore {
    _target: tempfile::TempDir,
    pub(crate) log_dir: std::path::PathBuf,
    pub(crate) cluster_id: uuid::Uuid,
    pub(crate) report: krabka_restore::RestoreReport,
}

pub(crate) async fn fresh_restore(archive_root: &Path) -> FreshRestore {
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let cluster_id = uuid::Uuid::new_v4();
    let report = restore_fixture(archive_root, target.path(), cluster_id).await;
    FreshRestore {
        _target: target,
        log_dir,
        cluster_id,
        report,
    }
}

/// Restore beside a bound controller listener, preserving the endpoint across startup.
pub(crate) async fn restore_for_controller(
    archive_root: &Path,
    extra: &[&str],
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    crate::args::ControllerListener,
    krabka_restore::RestoreReport,
) {
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let controller = crate::args::ControllerListener::bind().await;
    let args = controller.restore_args(archive_root, &log_dir, extra);
    let report = krabka_restore::restore(&args).await.expect("restore");
    (target, log_dir, controller, report)
}
