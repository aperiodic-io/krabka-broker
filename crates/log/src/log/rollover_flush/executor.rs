//! Process-wide limits include snapshots being prepared, queued, and flushed.

use {
    super::Shared,
    std::{
        collections::VecDeque,
        io,
        sync::{Arc, Condvar, Mutex},
        thread::JoinHandle,
    },
};

static EXECUTOR: Mutex<Option<Arc<Executor>>> = Mutex::new(None);

#[derive(Debug, Default)]
struct Queue {
    jobs: VecDeque<Arc<Shared>>,
    stopped: bool,
}

#[derive(Debug, Default)]
struct Workers {
    queue: Mutex<Queue>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct Usage {
    next_ticket: usize,
    serving_ticket: usize,
    jobs: usize,
    bytes: usize,
}

#[derive(Debug)]
struct Budget {
    usage: Mutex<Usage>,
    changed: Condvar,
    max_jobs: usize,
    max_bytes: usize,
}

/// Held from before snapshot allocation until its flush finishes or fails.
#[derive(Debug)]
pub(in crate::log) struct Permit {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut usage = self.budget.usage.lock().unwrap();
        usage.jobs -= 1;
        usage.bytes -= self.bytes;
        self.budget.changed.notify_all();
    }
}

#[derive(Debug)]
pub(super) struct Executor {
    workers: Arc<Workers>,
    threads: Vec<JoinHandle<()>>,
    budget: Arc<Budget>,
}

impl Executor {
    pub fn shared() -> io::Result<Arc<Self>> {
        // Serialize initialization, including partial-start cleanup. Concurrent
        // first rolls must not each start a separate pool.
        let mut executor = EXECUTOR.lock().unwrap();
        if executor.is_none() {
            *executor = Some(Self::new(4, 256, 64 * 1024 * 1024)?);
        }
        Ok(Arc::clone(executor.as_ref().unwrap()))
    }

    pub fn new(workers: usize, max_jobs: usize, max_bytes: usize) -> io::Result<Arc<Self>> {
        let mut executor = Self {
            workers: Arc::new(Workers::default()),
            threads: Vec::new(),
            budget: Arc::new(Budget {
                usage: Mutex::default(),
                changed: Condvar::new(),
                max_jobs,
                max_bytes,
            }),
        };
        for index in 0..workers {
            let workers = Arc::clone(&executor.workers);
            executor.threads.push(
                std::thread::Builder::new()
                    .name(format!("log-roll-flush-{index}"))
                    .spawn(move || worker(&workers))?,
            );
        }
        Ok(Arc::new(executor))
    }

    pub fn reserve(&self, bytes: usize) -> Permit {
        let mut usage = self.budget.usage.lock().unwrap();
        // FIFO admission lets an oversized snapshot drain the budget instead
        // of starving behind a continuous stream of smaller reservations.
        let ticket = usage.next_ticket;
        usage.next_ticket = usage.next_ticket.wrapping_add(1);
        // A snapshot larger than the budget is admitted exclusively. It must
        // still make progress, but cannot overlap any other captured snapshot.
        while ticket != usage.serving_ticket
            || usage.jobs >= self.budget.max_jobs
            || (usage.jobs != 0 && bytes > self.budget.max_bytes.saturating_sub(usage.bytes))
        {
            usage = self.budget.changed.wait(usage).unwrap();
        }
        usage.serving_ticket = usage.serving_ticket.wrapping_add(1);
        usage.jobs += 1;
        usage.bytes += bytes;
        self.budget.changed.notify_all();
        Permit {
            budget: Arc::clone(&self.budget),
            bytes,
        }
    }

    pub fn submit(&self, log: Arc<Shared>) {
        // Every scheduled log owns a permit, so this queue cannot exceed the
        // global job limit. Submission never waits while holding a log lock.
        self.workers.queue.lock().unwrap().jobs.push_back(log);
        self.workers.changed.notify_one();
    }
}

fn worker(workers: &Workers) {
    loop {
        let log = {
            let mut queue = workers.queue.lock().unwrap();
            while queue.jobs.is_empty() && !queue.stopped {
                queue = workers.changed.wait(queue).unwrap();
            }
            let Some(log) = queue.jobs.pop_front() else {
                return;
            };
            log
        };
        // One flush per turn prevents busy partitions from monopolizing the
        // pool, while each partition has at most one scheduled or running task.
        if super::run(&log) {
            workers.queue.lock().unwrap().jobs.push_back(log);
            workers.changed.notify_one();
        }
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.workers.queue.lock().unwrap().stopped = true;
        self.workers.changed.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        std::{
            sync::mpsc,
            time::{Duration, Instant},
        },
    };

    #[test]
    fn an_oversized_waiter_is_not_overtaken_by_smaller_reservations() {
        let executor = Executor::new(1, 256, 2).unwrap();
        let running = executor.reserve(1);
        let (large_tx, large_rx) = mpsc::channel();
        let large_executor = executor.clone();
        let large = std::thread::spawn(move || {
            large_tx.send(large_executor.reserve(3)).unwrap();
        });
        // Wait for the large request to enter admission, before starting the
        // small request that would otherwise fit alongside the running job.
        let deadline = Instant::now() + Duration::from_secs(5);
        while executor.budget.usage.lock().unwrap().next_ticket < 2 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        let waiting = executor.budget.usage.lock().unwrap().next_ticket == 2;
        let (small_tx, small_rx) = mpsc::channel();
        let small_executor = executor.clone();
        let small = std::thread::spawn(move || {
            small_tx.send(small_executor.reserve(1)).unwrap();
        });
        let pending = small_rx.recv_timeout(Duration::from_millis(100));
        drop(running);
        // Release both sides before asserting, including the negative control
        // where the small reservation overtakes the large one.
        let small_overtook = pending.is_ok();
        drop(pending);
        let large_permit = large_rx.recv_timeout(Duration::from_secs(5));
        let large_admitted = large_permit.is_ok();
        drop(large_permit);
        if !small_overtook {
            drop(small_rx.recv_timeout(Duration::from_secs(5)));
        }
        large.join().unwrap();
        small.join().unwrap();
        assert!(waiting);
        assert!(!small_overtook);
        assert!(large_admitted);
    }
}
