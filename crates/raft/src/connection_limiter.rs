//! Live-connection accounting for the `max.connections` and
//! `max.connections.per.ip` caps, together with the RAII guard that releases a
//! slot when a connection ends. It is a self-contained counter pair, so it sits
//! apart from the accept loop that consults it.

use {
    dashmap::DashMap,
    std::{
        net::IpAddr,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    },
};

/// Live-connection accounting backing the `max.connections` (global) and
/// `max.connections.per.ip` caps. A clone shares the same counters, which are
/// `Arc`-wrapped internally, so every listener accept loop and every
/// [`ConnectionGuard`] account against one set of totals.
#[derive(Clone)]
pub struct ConnectionLimiter {
    /// Global ceiling. `usize::MAX` means unlimited.
    max_connections: usize,
    /// Per-IP ceiling. `usize::MAX` means unlimited.
    max_connections_per_ip: usize,
    /// Current live connection total across all listeners.
    total: Arc<AtomicUsize>,
    /// Current live connection count per client IP. Entries are removed
    /// when they hit 0 so the map doesn't grow unbounded.
    per_ip: Arc<DashMap<IpAddr, usize>>,
}

/// The connection ceiling that refused an inbound connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionLimit {
    Global,
    PerIp,
}

impl ConnectionLimiter {
    #[must_use]
    pub fn new(max_connections: usize, max_connections_per_ip: usize) -> Self {
        Self {
            max_connections,
            max_connections_per_ip,
            total: Arc::new(AtomicUsize::new(0)),
            per_ip: Arc::new(DashMap::new()),
        }
    }

    /// Try to reserve a connection slot for `ip`. On success returns a
    /// [`ConnectionGuard`] that releases both the global and per-IP slot
    /// on drop.
    ///
    /// # Errors
    /// Returns the [`ConnectionLimit`] that refused the connection, and
    /// reserves nothing, when either the global or the per-IP cap is already
    /// reached.
    pub fn try_acquire(&self, ip: IpAddr) -> Result<ConnectionGuard, ConnectionLimit> {
        // Global cap. `fetch_update` keeps the increment atomic so two
        // concurrent accepts can't both slip past the ceiling.
        let global_ok = self
            .total
            .try_update(Ordering::AcqRel, Ordering::Acquire, |cur| {
                (cur < self.max_connections).then_some(cur + 1)
            })
            .is_ok();
        if !global_ok {
            return Err(ConnectionLimit::Global);
        }
        // Per-IP cap. The DashMap entry lock serializes the read-modify
        // on a single IP. On rejection we must undo the global reserve.
        let mut entry = self.per_ip.entry(ip).or_insert(0);
        if *entry >= self.max_connections_per_ip {
            drop(entry);
            self.total.fetch_sub(1, Ordering::AcqRel);
            return Err(ConnectionLimit::PerIp);
        }
        *entry += 1;
        drop(entry);
        Ok(ConnectionGuard {
            limiter: self.clone(),
            ip,
        })
    }

    /// Diagnostic accessor: current global live-connection count.
    #[must_use]
    pub fn total(&self) -> usize { self.total.load(Ordering::Acquire) }

    /// Test/diagnostic accessor: current per-IP live-connection count.
    #[cfg(test)]
    fn per_ip_count(&self, ip: IpAddr) -> usize { self.per_ip.get(&ip).map_or(0, |e| *e) }
}

/// RAII release for one accepted connection. Moved into the spawned
/// per-connection task, so it fires however the connection ends: clean
/// close, error, panic, or task abort. On drop it decrements the
/// global counter and the per-IP counter, and it removes the per-IP map
/// entry when that count reaches 0.
pub struct ConnectionGuard {
    limiter: ConnectionLimiter,
    ip: IpAddr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.limiter.total.fetch_sub(1, Ordering::AcqRel);
        // Decrement the per-IP entry; remove it at 0 to bound map growth.
        if let dashmap::mapref::entry::Entry::Occupied(mut occ) = self.limiter.per_ip.entry(self.ip)
        {
            let v = occ.get_mut();
            *v -= 1;
            if *v == 0 {
                occ.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[test]
    fn connection_guard_increments_and_decrements_global_and_per_ip() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, usize::MAX));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(limiter.total() == 0);
        assert!(limiter.per_ip_count(ip) == 0);

        let g1 = limiter
            .try_acquire(ip)
            .expect("acquire under unlimited caps");
        assert!(limiter.total() == 1);
        assert!(limiter.per_ip_count(ip) == 1);

        let g2 = limiter.try_acquire(ip).expect("second acquire");
        assert!(limiter.total() == 2);
        assert!(limiter.per_ip_count(ip) == 2);

        drop(g1);
        assert!(limiter.total() == 1);
        assert!(limiter.per_ip_count(ip) == 1);

        drop(g2);
        // Per-IP entry must be removed (not left at 0) when it hits zero.
        check!(limiter.total() == 0);
        check!(limiter.per_ip_count(ip) == 0);
        check!(limiter.per_ip.get(&ip).is_none());
    }

    #[test]
    fn global_cap_rejects_at_limit() {
        let limiter = Arc::new(ConnectionLimiter::new(1, usize::MAX));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let _g = limiter.try_acquire(a).expect("first connection accepted");
        // Global ceiling of 1 reached — a different IP is still rejected,
        // and the rejection reserves nothing (per-IP entry not created).
        check!(limiter.try_acquire(b).err() == Some(ConnectionLimit::Global));
        check!(limiter.total() == 1);
        check!(limiter.per_ip_count(b) == 0);
        check!(limiter.per_ip.get(&b).is_none());
    }

    #[test]
    fn per_ip_cap_rejects_but_other_ip_allowed() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, 1));
        let a: IpAddr = "10.0.0.1".parse().unwrap();
        let b: IpAddr = "10.0.0.2".parse().unwrap();
        let _g1 = limiter.try_acquire(a).expect("first from a");
        // Second from the same IP rejected; global must be rolled back so
        // the count reflects only the one live connection.
        check!(limiter.try_acquire(a).err() == Some(ConnectionLimit::PerIp));
        check!(limiter.total() == 1);
        check!(limiter.per_ip_count(a) == 1);
        // A different IP is still under its own per-IP ceiling.
        let _g2 = limiter.try_acquire(b).expect("first from b allowed");
        assert!(limiter.total() == 2);
        assert!(limiter.per_ip_count(b) == 1);
    }

    #[test]
    fn ipv6_peer_acquires_and_releases() {
        let limiter = Arc::new(ConnectionLimiter::new(usize::MAX, usize::MAX));
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        let g = limiter.try_acquire(ip).expect("ipv6 acquire");
        assert!(limiter.per_ip_count(ip) == 1);
        drop(g);
        assert!(limiter.per_ip_count(ip) == 0);
    }
}
