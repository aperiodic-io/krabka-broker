//! Target-side flags shared by restore integration tests.

use std::path::Path;

/// A local archive restored into standalone node 1 with an explicit controller listener.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct RestoreOptions<'a> {
    #[default("127.0.0.1:9093")]
    pub controller_listener: &'a str,
    pub extra: &'a [&'a str],
}

pub fn restore_argv(
    archive_root: &Path,
    log_dir: &Path,
    options: RestoreOptions<'_>,
) -> Vec<String> {
    let RestoreOptions {
        controller_listener,
        extra,
    } = options;
    let mut argv = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        archive_root.display().to_string(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        "1".to_owned(),
        "--standalone".to_owned(),
        "--controller-listener".to_owned(),
        controller_listener.to_owned(),
    ];
    argv.extend(extra.iter().map(|argument| (*argument).to_owned()));
    argv
}
