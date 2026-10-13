//! Ordered rollover flushes that own file handles instead of the log mutex.

#[cfg(not(target_os = "wasi"))]
use crate::{
    io::{IoTarget, LogIo},
    producer_snapshot::PreparedSnapshot,
};
#[cfg(not(target_os = "wasi"))]
use std::{collections::VecDeque, fs::File};
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
};

#[cfg(not(target_os = "wasi"))]
mod executor;

/// A sealed segment and the producer state at its exclusive end.
#[cfg(not(target_os = "wasi"))]
#[derive(Debug)]
pub(super) struct Flush {
    pub files: [Arc<File>; 3],
    pub io: Arc<dyn LogIo>,
    pub snapshot: PreparedSnapshot,
    // Retain the reservation through the running job, including error unwinds.
    pub _permit: executor::Permit,
}

#[cfg(not(target_os = "wasi"))]
impl Flush {
    fn run(self) -> io::Result<()> {
        // Never publish a durable snapshot ahead of the records it describes.
        self.io.sync_data(&self.files[0])?;
        self.io.sync_file(IoTarget::OffsetIndex, &self.files[1])?;
        self.io.sync_file(IoTarget::TimeIndex, &self.files[2])?;
        self.snapshot.write(&*self.io)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct State {
    #[cfg(not(target_os = "wasi"))]
    jobs: VecDeque<Flush>,
    running: bool,
    error: Option<FlushError>,
}

/// The kind and message of the I/O error that failed a flush job.
#[derive(Debug)]
struct FlushError {
    kind: io::ErrorKind,
    message: String,
}

impl State {
    fn result(&self) -> io::Result<()> {
        self.error.as_ref().map_or(Ok(()), |error| {
            Err(io::Error::new(error.kind, error.message.clone()))
        })
    }
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

/// Ordered per-log jobs on a bounded process-wide executor.
/// WASI flushes inline in the rollover caller and needs no executor.
#[derive(Debug, Default)]
pub(super) struct Flusher {
    shared: Arc<Shared>,
    #[cfg(not(target_os = "wasi"))]
    executor: Option<Arc<executor::Executor>>,
}

impl Flusher {
    pub fn check(&self) -> io::Result<()> { self.shared.state.lock().unwrap().result() }

    pub fn finish(&self) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        while state.running {
            state = self.shared.changed.wait(state).unwrap();
        }
        state.result()
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn reserve(&mut self, bytes: usize) -> io::Result<executor::Permit> {
        self.check()?;
        if self.executor.is_none() {
            self.executor = Some(executor::Executor::shared()?);
        }
        Ok(self.executor.as_ref().unwrap().reserve(bytes))
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn submit(&self, flush: Flush) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        state.result()?;
        state.jobs.push_back(flush);
        if !state.running {
            state.running = true;
            self.executor
                .as_ref()
                .unwrap()
                .submit(Arc::clone(&self.shared));
        }
        Ok(())
    }
}

#[cfg(not(target_os = "wasi"))]
fn run(shared: &Shared) -> bool {
    let job = shared.state.lock().unwrap().jobs.pop_front().unwrap();
    // An injected I/O panic must wake waiters just like an I/O error.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.run()))
        .unwrap_or_else(|_| Err(io::Error::other("rollover flush panicked")));
    let mut state = shared.state.lock().unwrap();
    if let Err(error) = result {
        tracing::error!(%error, "rollover flush failed");
        state.error = Some(FlushError {
            kind: error.kind(),
            message: error.to_string(),
        });
        state.jobs.clear();
    }
    if state.jobs.is_empty() {
        state.running = false;
        shared.changed.notify_all();
        false
    } else {
        true
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::warn!(%error, "log closed after a failed rollover flush");
        }
    }
}

#[cfg(all(test, not(target_os = "wasi")))]
mod tests;
