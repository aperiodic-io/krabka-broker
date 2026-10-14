//! Running a soak in a child process with one allocator arena.
//!
//! The soak judges the resident set of the process the brokers run in, so
//! that process should hold the brokers and nothing else, and its allocator
//! should not add a growth of its own. This test binary meets neither as it
//! is: the harness runs other tests beside the soak, and glibc gives a thread
//! that allocates an arena of its own, which keeps the memory it was given.
//! The blocking pool adds threads all through a run, so with the default
//! arenas the resident set after each drain rose 17 MiB over 20 minutes with
//! deduplication on, and the same 17 MiB with it off. With one arena it rose
//! 3.7 MiB, which leaves room for a tolerance that a leak can exceed.
//!
//! So each soak test runs this binary again, filtered to [`entry`], with
//! [`VARIANT_ENV`] naming the soak and `MALLOC_ARENA_MAX=1`. It prints the
//! child's standard output as its own, so `--nocapture` shows the series as
//! they are sampled, and fails when the child fails. [`entry`] does nothing
//! when the variable is unset, so in an ordinary run it is an empty test.

use {
    crate::{run, settings::Settings},
    assert2::assert,
    std::{
        collections::VecDeque,
        io::{BufRead, BufReader, Read},
        process::{Command, Stdio},
        sync::{Mutex, PoisonError},
        thread,
    },
};

/// The environment variable that names the soak a child runs.
pub const VARIANT_ENV: &str = "KRABKA_DEDUP_SOAK_VARIANT";

/// The lines of the child's standard error kept for a failure message.
const STDERR_TAIL_LINES: usize = 200;

/// The two soaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// Every churn step but broker restarts.
    Churn,
    /// The churn steps and a broker restart every four cycles.
    Restarts,
}

impl Variant {
    const ALL: [Self; 2] = [Self::Churn, Self::Restarts];

    fn name(self) -> &'static str {
        match self {
            Self::Churn => "churn",
            Self::Restarts => "restarts",
        }
    }

    fn settings(self) -> Settings {
        Settings::from_env(match self {
            Self::Churn => None,
            Self::Restarts => Some(4),
        })
    }
}

/// The child side: run the soak that [`VARIANT_ENV`] names.
#[test]
fn entry() {
    let Ok(name) = std::env::var(VARIANT_ENV) else {
        return;
    };
    let variant = Variant::ALL
        .into_iter()
        .find(|variant| variant.name() == name)
        .unwrap_or_else(|| panic!("no soak is called {name}"));
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a tokio runtime")
        .block_on(Box::pin(run::soak(variant.settings())));
}

/// Both soaks keep a machine busy, so one runs at a time.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// The last `keep` lines `stream` yields.
fn tail(stream: impl Read, keep: usize) -> String {
    let mut lines = VecDeque::with_capacity(keep);
    for line in BufReader::new(stream).lines().map_while(Result::ok) {
        if lines.len() == keep {
            lines.pop_front();
        }
        lines.push_back(line);
    }
    Vec::from(lines).join("\n")
}

/// The parent side: run `variant` in a child and fail when it fails.
pub fn soak(variant: Variant) {
    let _alone = ONE_AT_A_TIME.lock().unwrap_or_else(PoisonError::into_inner);
    let exe = std::env::current_exe().expect("the test binary's path");
    let mut child = Command::new(exe)
        .args(["child::entry", "--exact", "--nocapture", "--test-threads=1"])
        .env(VARIANT_ENV, variant.name())
        .env("MALLOC_ARENA_MAX", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start the soak child");
    let stderr = child.stderr.take().expect("the child's standard error");
    let stderr = thread::spawn(move || tail(stderr, STDERR_TAIL_LINES));
    let stdout = child.stdout.take().expect("the child's standard output");
    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
        println!("{line}");
    }
    let status = child.wait().expect("wait for the soak child");
    let stderr = stderr.join().expect("read the child's standard error");
    assert!(
        status.success(),
        "the {} soak failed ({status}); the end of its standard error:\n{stderr}",
        variant.name()
    );
}

mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn the_tail_keeps_the_last_lines() {
        assert!(tail("a\nb\nc\nd\n".as_bytes(), 2) == "c\nd");
        assert!(tail("a\n".as_bytes(), 5) == "a");
    }

    #[test]
    fn every_variant_is_found_by_its_name() {
        let names: Vec<&str> = Variant::ALL.into_iter().map(Variant::name).collect();
        assert!(names == ["churn", "restarts"]);
        assert!(Variant::Churn.settings().restart_every.is_none());
        assert!(Variant::Restarts.settings().restart_every == Some(4));
    }
}
