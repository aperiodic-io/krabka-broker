//! Load failures retain their declaration's log target and selected message.

use {
    assert2::assert,
    std::{
        future::pending,
        io::{self, Write},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    },
    tokio::task::{JoinError, JoinHandle},
};

static FINISH_CALLS: AtomicUsize = AtomicUsize::new(0);

mod task_util {
    use super::{FINISH_CALLS, JoinError, JoinHandle, Ordering, assert};

    // A one-handle harness isolates the derive's forwarding contract; broker
    // unit tests exercise the real helper's ordering and completion behavior.
    pub async fn finish_tasks(tasks: Vec<JoinHandle<()>>, mut on_error: impl FnMut(JoinError)) {
        FINISH_CALLS.fetch_add(1, Ordering::SeqCst);
        assert!(tasks.len() == 1);
        let task = tasks.into_iter().next().unwrap();
        let error = task.await.expect_err("the supplied load was cancelled");
        assert!(error.is_cancelled());
        on_error(error);
    }
}

mod share_loads {
    use super::JoinHandle;

    #[derive(Debug, Default, krabka_macros::ScheduledLoads)]
    #[scheduled_loads(failure_message = "__share_group_state load task failed")]
    pub(super) struct Loads(pub(super) Vec<JoinHandle<()>>);
}

mod transaction_loads {
    use super::JoinHandle;

    #[derive(Debug, Default, krabka_macros::ScheduledLoads)]
    #[scheduled_loads(failure_message = "__transaction_state load task failed")]
    pub(super) struct Loads(pub(super) Vec<JoinHandle<()>>);
}

#[derive(Clone)]
struct LogOutput(Arc<Mutex<Vec<u8>>>);

impl Write for LogOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

#[tokio::test(flavor = "current_thread")]
async fn finished_forwards_handles_once_and_warns_at_each_declarations_target() {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let output = LogOutput(Arc::clone(&bytes));
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || output.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    assert!(share_loads::Loads::default().0.is_empty());
    let task = tokio::spawn(pending::<()>());
    task.abort();
    share_loads::Loads(vec![task]).finished().await;
    let task = tokio::spawn(pending::<()>());
    task.abort();
    transaction_loads::Loads(vec![task]).finished().await;
    assert!(FINISH_CALLS.load(Ordering::SeqCst) == 2);
    let captured = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
    let lines: Vec<_> = captured.lines().collect();
    assert!(lines.len() == 2);
    for (line, module, message) in [
        (
            lines[0],
            "share_loads",
            "__share_group_state load task failed",
        ),
        (
            lines[1],
            "transaction_loads",
            "__transaction_state load task failed",
        ),
    ] {
        assert!(line.contains("WARN"));
        assert!(line.contains(&format!("{}::{module}:", module_path!())));
        assert!(line.contains(message));
        assert!(line.contains("error=") && line.contains("cancelled"));
    }
}
