//! Property tests over the visibility window that every fetch computes,
//! run on random valid watermark tuples rather than on a fixed table.

use {
    super::{FetchWatermarks, Offset, compute_visibility_window},
    proptest::prelude::*,
};

proptest! {
    /// The per-fetch visibility contract over large-N random valid
    /// watermark tuples, `log_start <= lso <= hw <= log_end`, together
    /// with a delivery watermark inside `[log_start, hw]` and the fetch
    /// parameters.
    #[test]
    fn visibility_contract_holds(
        a in 0i64..1_000_000,
        b in 0i64..1_000_000,
        c in 0i64..1_000_000,
        d in 0i64..1_000_000,
        fo in 0i64..1_000_000,
        deliverable_raw in 0i64..1_000_000,
        is_follower in any::<bool>(),
        rc_raw in any::<bool>(),
    ) {
        let mut v = [a, b, c, d];
        v.sort_unstable();
        let (log_start, lso, hw, log_end) = (v[0], v[1], v[2], v[3]);
        // KFC-1: `plan_read` clamps the delivery watermark into the range
        // the log still holds, so the kernel sees `log_start <= d <= hw`.
        let deliverable = deliverable_raw.clamp(log_start, hw);
        let read_committed = rc_raw && !is_follower; // read_committed ⟹ !follower
        let watermarks = FetchWatermarks {
            log_start: Offset(log_start),
            hw: Offset(hw),
            lso: Offset(lso),
            log_end: Offset(log_end),
            deliverable: Offset(deliverable),
        };
        let w = compute_visibility_window(is_follower, read_committed, watermarks, Offset(fo));
        // Unwrap the `Offset` window fields into this proptest's `i64` world.
        let (limit_offset, response_hw, response_lso, effective_lso) = (
            w.limit_offset.0,
            w.response_hw.0,
            w.response_lso.0,
            w.effective_lso.0,
        );
        prop_assert!(limit_offset >= 0 && response_hw >= 0 && response_lso >= 0);
        prop_assert_eq!(w.out_of_range, fo < log_start || fo > log_end);
        // Every fetcher is told the partition's own high watermark and last
        // stable offset (Kafka's `Partition.readRecords`), and neither moves
        // with the delivery watermark, so consumer lag stays honest and a
        // KIP-392 follower never adopts an uncommitted offset.
        prop_assert_eq!(response_hw, hw);
        prop_assert_eq!(response_lso, lso.min(hw));
        let upper = if is_follower { log_end } else { deliverable };
        if !w.out_of_range {
            prop_assert_eq!(w.empty, fo >= upper);
        }
        if is_follower {
            // Replication is not gated by the delivery watermark.
            prop_assert_eq!(limit_offset, log_end);
            prop_assert!(limit_offset >= hw);
        } else {
            prop_assert!(limit_offset <= hw, "consumer fetch must not expose beyond HW");
            prop_assert!(
                limit_offset <= deliverable,
                "consumer fetch must not expose a record before it is due"
            );
            if read_committed {
                prop_assert_eq!(effective_lso, lso.min(hw));
                prop_assert!(limit_offset <= lso.min(hw));
            }
        }
    }

    /// The consumer bound needs no precondition: whatever offsets the caller
    /// passes, in whatever order, a consumer never reads at or past the high
    /// watermark, the delivery watermark, or, under `read_committed`, the
    /// last stable offset.
    #[test]
    fn consumer_bound_holds_for_any_offsets(
        log_start in -1_000i64..1_000_000,
        hw in -1_000i64..1_000_000,
        lso in -1_000i64..1_000_000,
        log_end in -1_000i64..1_000_000,
        deliverable in -1_000i64..1_000_000,
        fo in -1_000i64..1_000_000,
        read_committed in any::<bool>(),
    ) {
        let w = compute_visibility_window(
            false,
            read_committed,
            FetchWatermarks {
                log_start: Offset(log_start),
                hw: Offset(hw),
                lso: Offset(lso),
                log_end: Offset(log_end),
                deliverable: Offset(deliverable),
            },
            Offset(fo),
        );
        let limit_offset = w.limit_offset.0;
        prop_assert!(limit_offset <= hw && limit_offset <= deliverable);
        if read_committed {
            prop_assert!(limit_offset <= lso);
        }
        prop_assert!(w.response_hw.0 == hw && w.response_lso.0 == lso.min(hw));
    }

    /// `KIP-227` monotonicity: an advance of `hw`, `lso`, or `log_end` never
    /// lowers the reported HW or LSO for any fixed fetch shape.
    #[test]
    fn response_monotonic(
        base in 0i64..100_000,
        d_end in 0i64..100_000,
        d_adv in 0i64..100_000,
        d_end2 in 0i64..100_000,
        is_follower in any::<bool>(),
        rc_raw in any::<bool>(),
    ) {
        let read_committed = rc_raw && !is_follower;
        let log_start = 0;
        // Valid baseline: lso == hw == base, log_end >= hw.
        let (hw, lso, log_end) = (base, base, base + d_end);
        // Advance all of hw/lso/log_end (still valid: lso == hw).
        let (hw2, lso2, log_end2) = (hw + d_adv, lso + d_adv, log_end + d_adv + d_end2);
        // The delivery watermark goes the other way: nothing is deliverable
        // in the second window even though every other watermark advanced.
        // The reported HW and LSO must not follow it down.
        let w1 = compute_visibility_window(
            is_follower,
            read_committed,
            FetchWatermarks {
                log_start: Offset(log_start),
                hw: Offset(hw),
                lso: Offset(lso),
                log_end: Offset(log_end),
                deliverable: Offset(hw),
            },
            Offset(0),
        );
        let w2 = compute_visibility_window(
            is_follower,
            read_committed,
            FetchWatermarks {
                log_start: Offset(log_start),
                hw: Offset(hw2),
                lso: Offset(lso2),
                log_end: Offset(log_end2),
                deliverable: Offset(log_start),
            },
            Offset(0),
        );
        prop_assert!(w2.response_hw >= w1.response_hw, "response_hw regressed");
        prop_assert!(w2.response_lso >= w1.response_lso, "response_lso regressed");
    }
}
