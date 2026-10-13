//! The heartbeat state machine of a broker: fenced, unfenced, in controlled
//! shutdown, or allowed to shut down now.
//!
//! This is Kafka's `BrokerHeartbeatManager.calculateNextBrokerState`. The
//! controller reads the current state from the registry, decides the next one
//! from the heartbeat, writes the records the transition needs, and then
//! [`touch`](ControllerLivenessState::touch)es the registry with the result.

use super::{BrokerLivenessState, ControllerLivenessState};

/// Kafka's `BrokerControlState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerControlState {
    Fenced,
    Unfenced,
    ControlledShutdown,
    ShutdownNow,
}

impl BrokerControlState {
    /// Whether a broker in this state is fenced. A broker that may shut down
    /// now is fenced, so nothing elects it while it stops.
    pub(crate) const fn fenced(self) -> bool { matches!(self, Self::Fenced | Self::ShutdownNow) }

    pub(crate) const fn should_shut_down(self) -> bool { matches!(self, Self::ShutdownNow) }
}

/// What one heartbeat asks for: `wantFence` and `wantShutDown`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct HeartbeatWants {
    pub(crate) fence: bool,
    pub(crate) shut_down: bool,
}

/// What one heartbeat asks for and what the controller knows about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeartbeatFacts {
    pub(crate) wants: HeartbeatWants,
    /// The broker reached the offset of its own registration record.
    pub(crate) caught_up: bool,
    /// The broker still leads a partition that another replica can take.
    pub(crate) has_leaderships: bool,
    /// The offset at which the broker's controlled shutdown is complete,
    /// while it is in controlled shutdown.
    pub(crate) controlled_shutdown_offset: Option<i64>,
    /// [`ControllerLivenessState::lowest_active_offset`].
    pub(crate) lowest_active_offset: i64,
}

/// Kafka's `BrokerHeartbeatManager.calculateNextBrokerState`.
///
/// A fenced broker that asks to shut down may do so at once, and one that
/// caught up to its registration record unfences. An unfenced broker that asks
/// to shut down with leaderships enters controlled shutdown, and one with none
/// may shut down at once. A broker in controlled shutdown may shut down once it
/// leads nothing and every active broker has replicated the records that its
/// controlled shutdown wrote.
pub(crate) const fn next_broker_state(
    current: BrokerControlState,
    facts: HeartbeatFacts,
) -> BrokerControlState {
    use BrokerControlState::{ControlledShutdown, Fenced, ShutdownNow, Unfenced};
    match current {
        Fenced => {
            if facts.wants.shut_down {
                ShutdownNow
            } else if !facts.wants.fence && facts.caught_up {
                Unfenced
            } else {
                Fenced
            }
        }
        Unfenced => {
            if facts.wants.fence {
                if facts.wants.shut_down {
                    ShutdownNow
                } else {
                    Fenced
                }
            } else if facts.wants.shut_down {
                if facts.has_leaderships {
                    ControlledShutdown
                } else {
                    ShutdownNow
                }
            } else {
                Unfenced
            }
        }
        ControlledShutdown => {
            if facts.has_leaderships {
                ControlledShutdown
            } else {
                match facts.controlled_shutdown_offset {
                    Some(offset) if offset > facts.lowest_active_offset => ControlledShutdown,
                    _ => ShutdownNow,
                }
            }
        }
        ShutdownNow => ShutdownNow,
    }
}

impl ControllerLivenessState {
    /// The state a heartbeat from `broker_id` starts from. A broker the
    /// registry does not know is fenced.
    pub(crate) async fn control_state(&self, broker_id: u64) -> BrokerControlState {
        let map = self.brokers.lock().await;
        match map.get(&broker_id) {
            Some(entry) if entry.controlled_shutdown_offset.is_some() => {
                BrokerControlState::ControlledShutdown
            }
            Some(entry) if !entry.fenced => BrokerControlState::Unfenced,
            _ => BrokerControlState::Fenced,
        }
    }

    /// The offset at which the controlled shutdown of `broker_id` is complete,
    /// if it is in controlled shutdown.
    pub(crate) async fn controlled_shutdown_offset(&self, broker_id: u64) -> Option<i64> {
        self.brokers
            .lock()
            .await
            .get(&broker_id)
            .and_then(|entry| entry.controlled_shutdown_offset)
    }

    /// Kafka's `BrokerHeartbeatManager.lowestActiveOffset`: the lowest
    /// metadata offset an active broker reported. An active broker is alive,
    /// unfenced and not in controlled shutdown. With no active broker the
    /// answer is `i64::MAX`, so nothing waits on a broker that is not there.
    pub(crate) async fn lowest_active_offset(&self) -> i64 {
        self.brokers
            .lock()
            .await
            .values()
            .filter(|entry| {
                entry.state == BrokerLivenessState::Alive
                    && !entry.fenced
                    && entry.controlled_shutdown_offset.is_none()
            })
            .map(|entry| entry.metadata_offset)
            .min()
            .unwrap_or(i64::MAX)
    }

    /// Kafka's `BrokerHeartbeatManager.touch`: record the state a heartbeat
    /// left `broker_id` in, and the metadata offset it reported. A fenced
    /// broker leaves controlled shutdown.
    pub(crate) async fn touch(
        &self,
        broker_id: u64,
        next: BrokerControlState,
        metadata_offset: i64,
    ) {
        let mut map = self.brokers.lock().await;
        let Some(entry) = map.get_mut(&broker_id) else {
            return;
        };
        entry.fenced = next.fenced();
        entry.metadata_offset = metadata_offset;
        if entry.fenced || next != BrokerControlState::ControlledShutdown {
            entry.controlled_shutdown_offset = None;
        }
    }

    /// Kafka's `BrokerHeartbeatManager.maybeUpdateControlledShutdownOffset`:
    /// `broker_id` is in controlled shutdown, and the records its controlled
    /// shutdown wrote end at `offset`.
    ///
    /// Kafka keeps the first offset. The controller here writes the drain
    /// again when a leadership came back to the broker, so the offset moves
    /// forward to the end of the latest write and never back.
    pub(crate) async fn enter_controlled_shutdown(&self, broker_id: u64, offset: i64) {
        if let Some(entry) = self.brokers.lock().await.get_mut(&broker_id) {
            entry.controlled_shutdown_offset = Some(
                entry
                    .controlled_shutdown_offset
                    .map_or(offset, |previous| previous.max(offset)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        BrokerControlState::{ControlledShutdown, Fenced, ShutdownNow, Unfenced},
        assert2::{assert, check},
    };

    const fn facts() -> HeartbeatFacts {
        HeartbeatFacts {
            wants: HeartbeatWants {
                fence: false,
                shut_down: false,
            },
            caught_up: true,
            has_leaderships: false,
            controlled_shutdown_offset: None,
            lowest_active_offset: i64::MAX,
        }
    }

    /// krabka-io/krabka-broker#824: the transitions of Kafka's
    /// `BrokerHeartbeatManager.calculateNextBrokerState`, one row per branch.
    #[test]
    fn the_next_state_follows_kafka() {
        struct Case {
            what: &'static str,
            current: BrokerControlState,
            facts: HeartbeatFacts,
            expected: BrokerControlState,
        }
        let cases: &[Case] = &[
            Case {
                what: "fenced, caught up",
                current: Fenced,
                facts: facts(),
                expected: Unfenced,
            },
            Case {
                what: "fenced, behind its registration",
                current: Fenced,
                facts: HeartbeatFacts {
                    caught_up: false,
                    ..facts()
                },
                expected: Fenced,
            },
            Case {
                what: "fenced, asking to stay fenced",
                current: Fenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: true,
                        shut_down: false,
                    },
                    ..facts()
                },
                expected: Fenced,
            },
            Case {
                what: "fenced, asking to shut down",
                current: Fenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: false,
                        shut_down: true,
                    },
                    has_leaderships: true,
                    ..facts()
                },
                expected: ShutdownNow,
            },
            Case {
                what: "unfenced",
                current: Unfenced,
                facts: facts(),
                expected: Unfenced,
            },
            Case {
                what: "unfenced, asking to be fenced",
                current: Unfenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: true,
                        shut_down: false,
                    },
                    ..facts()
                },
                expected: Fenced,
            },
            Case {
                what: "unfenced, asking to be fenced and to shut down",
                current: Unfenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: true,
                        shut_down: true,
                    },
                    has_leaderships: true,
                    ..facts()
                },
                expected: ShutdownNow,
            },
            Case {
                what: "unfenced, asking to shut down with leaderships",
                current: Unfenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: false,
                        shut_down: true,
                    },
                    has_leaderships: true,
                    ..facts()
                },
                expected: ControlledShutdown,
            },
            Case {
                what: "unfenced, asking to shut down with no leadership",
                current: Unfenced,
                facts: HeartbeatFacts {
                    wants: HeartbeatWants {
                        fence: false,
                        shut_down: true,
                    },
                    lowest_active_offset: 0,
                    ..facts()
                },
                expected: ShutdownNow,
            },
            Case {
                what: "in controlled shutdown with leaderships",
                current: ControlledShutdown,
                facts: HeartbeatFacts {
                    has_leaderships: true,
                    controlled_shutdown_offset: Some(10),
                    ..facts()
                },
                expected: ControlledShutdown,
            },
            Case {
                what: "in controlled shutdown, an active broker behind",
                current: ControlledShutdown,
                facts: HeartbeatFacts {
                    controlled_shutdown_offset: Some(10),
                    lowest_active_offset: 9,
                    ..facts()
                },
                expected: ControlledShutdown,
            },
            Case {
                what: "in controlled shutdown, every active broker at the offset",
                current: ControlledShutdown,
                facts: HeartbeatFacts {
                    controlled_shutdown_offset: Some(10),
                    lowest_active_offset: 10,
                    ..facts()
                },
                expected: ShutdownNow,
            },
            Case {
                what: "in controlled shutdown, no active broker",
                current: ControlledShutdown,
                facts: HeartbeatFacts {
                    controlled_shutdown_offset: Some(10),
                    ..facts()
                },
                expected: ShutdownNow,
            },
        ];
        for Case {
            what,
            current,
            facts,
            expected,
        } in cases
        {
            check!(next_broker_state(*current, *facts) == *expected, "{what}");
        }
    }

    /// The lowest active offset reads only alive, unfenced brokers that are not
    /// in controlled shutdown, and a fence takes a broker out of controlled
    /// shutdown.
    #[tokio::test]
    async fn only_active_brokers_hold_back_a_controlled_shutdown() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        check!(liveness.lowest_active_offset().await == i64::MAX);

        for (broker, offset) in [(1, 50), (2, 40), (3, 30), (4, 20)] {
            liveness.record_fenced_heartbeat(broker).await;
            liveness.touch(broker, Unfenced, offset).await;
        }
        liveness.touch(3, Fenced, 30).await;
        liveness.touch(4, ControlledShutdown, 20).await;
        liveness.enter_controlled_shutdown(4, 60).await;

        check!(liveness.lowest_active_offset().await == 40);
        check!(liveness.control_state(4).await == ControlledShutdown);
        check!(liveness.controlled_shutdown_offset(4).await == Some(60));
        check!(!liveness.is_alive(4).await);
        check!(!liveness.alive_snapshot().await.contains(&4));
        // A broker in controlled shutdown is not fenced, so it is not offline.
        check!(!liveness.unavailable_snapshot().await.contains(&4));

        // The drain was written again later: the offset only moves forward.
        liveness.enter_controlled_shutdown(4, 55).await;
        check!(liveness.controlled_shutdown_offset(4).await == Some(60));

        liveness.touch(4, ShutdownNow, 60).await;
        assert!(liveness.control_state(4).await == Fenced);
        assert!(liveness.controlled_shutdown_offset(4).await == None);
        check!(liveness.control_state(99).await == Fenced);
    }
}
