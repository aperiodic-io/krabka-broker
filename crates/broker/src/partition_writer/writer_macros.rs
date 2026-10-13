//! The `run_writer!` shorthand that writer-loop tests use to spawn the writer
//! task with its five argument tuples.

macro_rules! run_writer {
    (
        $topic:expr,
        $partition:expr,
        $log:expr,
        $log_dir:expr,
        $rx:expr,
        $append:expr,
        $replica:expr,
        $hw:expr,
        $delivery:expr,
        $status:expr,
        $producer:expr,
        $wal:expr
        $(; $cap:expr, $sequencer:expr)? $(,)?
    ) => {{
        let (cap, sequencer) = None
            $(.or(Some(($cap, $sequencer))))?
            .unwrap_or_else(|| {
                ($crate::config::BrokerConfig::default().max_produce_group, None)
            });
        run_with_sequencer(
            ($topic, $partition),
            ($log, $log_dir),
            $rx,
            ($append, $replica, $hw, $delivery),
            ($status, $producer, $wal),
            cap,
            sequencer,
        )
    }};
    (
        $topic:expr,
        $partition:expr,
        $log:expr,
        $log_dir:expr,
        $rx:expr,
        $append:expr,
        $replica:expr,
        $hw:expr,
        $status:expr,
        $producer:expr,
        $wal:expr $(,)?
    ) => {
        run_writer!(
            $topic,
            $partition,
            $log,
            $log_dir,
            $rx,
            $append,
            $replica,
            $hw,
            DeliveryHandles::new(),
            $status,
            $producer,
            $wal,
        )
    };
}

pub(super) use run_writer;
