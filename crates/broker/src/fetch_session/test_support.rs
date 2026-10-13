//! Shared fixtures for the fetch-session unit tests: a cache on a manually
//! advanced LRU clock, and builders for the `FetchRequest` and `FetchTopic`
//! values that the tests feed to it.

use {
    super::{
        CachedPartitionState, FetchSessionId, FetchSessionKey, SessionDecision,
        cache::FetchSessionCache,
    },
    krabka_ids::PartitionIndex,
    krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
        primitives::uuid::Uuid as WireUuid,
    },
    qubit_clock::ManualMonotonicClock,
    std::sync::Arc,
};

#[derive(Clone, Copy)]
pub(super) struct SessionSlotCount(pub usize);

/// Builds a cache whose LRU clock is a [`ManualMonotonicClock`] sitting at its
/// own origin. The returned `Arc` is both the clock the cache stamps from and
/// the handle that advances it, so a test can put successive allocations on
/// distinct points in the cache's recency order. The test advances logical
/// time with `clock.advance(..)` instead of a sleep between allocations.
pub(super) fn manual_cache(max_slots: SessionSlotCount) -> ManualCache {
    let clock = ManualMonotonicClock::new_shared();
    let cache = FetchSessionCache::with_clock(max_slots.0, clock.clone());
    ManualCache { cache, clock }
}

/// A cache, and the manual clock that stamps and advances it.
pub(super) struct ManualCache {
    pub(super) cache: FetchSessionCache,
    pub(super) clock: Arc<ManualMonotonicClock>,
}

/// A `Fetch` version that names topics, which the fixtures below build.
pub(super) const NAME_FETCH_VERSION: i16 = super::FIRST_TOPIC_ID_FETCH_VERSION - 1;

/// A one-nanosecond tick: the smallest advance that still gives the next
/// allocation a strictly greater last-use stamp than the previous one.
pub(super) const TICK: std::time::Duration = std::time::Duration::from_nanos(1);

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
pub(super) struct RequestSessionId(pub FetchSessionId);

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
pub(super) struct RequestSessionEpoch(pub super::FetchSessionEpoch);

#[derive(Default)]
pub(super) struct SessionRequestSetup {
    pub session_id: RequestSessionId,
    pub session_epoch: RequestSessionEpoch,
    pub topics: Vec<FetchTopic>,
    pub forgotten: Vec<ForgottenTopic>,
}

impl SessionRequestSetup {
    /// The first incremental request for a session allocated by the cache.
    pub(super) fn incremental(session_id: RequestSessionId) -> Self {
        Self {
            session_id,
            session_epoch: RequestSessionEpoch(1),
            ..Default::default()
        }
    }
}

macro_rules! incremental_req {
    ($id:expr, $epoch:expr $(, $field:ident($value:expr))*) => {{
        let builder = $crate::fetch_session::test_support::req()
            .session_id($crate::fetch_session::test_support::RequestSessionId($id))
            .session_epoch($epoch);
        $(let builder = builder.$field($value);)*
        builder.call()
    }};
}

pub(crate) use incremental_req;

#[bon::builder]
pub(super) fn req(
    #[builder(into, default)] session_id: RequestSessionId,
    #[builder(into, default)] session_epoch: RequestSessionEpoch,
    #[builder(default)] topics: Vec<FetchTopic>,
    #[builder(default)] forgotten: Vec<ForgottenTopic>,
) -> FetchRequest {
    let setup = SessionRequestSetup {
        session_id,
        session_epoch,
        topics,
        forgotten,
    };
    let SessionRequestSetup {
        session_id,
        session_epoch,
        topics,
        forgotten,
    } = setup;
    FetchRequest {
        session_id: session_id.0,
        session_epoch: session_epoch.0,
        topics,
        forgotten_topics_data: forgotten,
        ..Default::default()
    }
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct ForgottenTopicSetup {
    #[default("t".into())]
    pub topic: String,
    pub topic_id: WireUuid,
    #[default(vec![PartitionIndex(0)])]
    pub partitions: Vec<PartitionIndex>,
}

#[bon::builder]
pub(super) fn forgotten_topic(
    #[builder(default = "t" . into ())] topic: String,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default = vec ! [PartitionIndex (0)])] partitions: Vec<PartitionIndex>,
) -> ForgottenTopic {
    let setup = ForgottenTopicSetup {
        topic,
        topic_id,
        partitions,
    };
    let ForgottenTopicSetup {
        topic,
        topic_id,
        partitions,
    } = setup;
    ForgottenTopic {
        topic,
        topic_id,
        partitions: partitions.into_iter().map(|index| index.0).collect(),
        ..Default::default()
    }
}

pub(super) fn topic(name: &str, partitions: &[PartitionIndex]) -> FetchTopic {
    FetchTopic {
        topic: name.to_string(),
        topic_id: WireUuid::ZERO,
        partitions: partitions
            .iter()
            .map(|&p| FetchPartition {
                partition: p.0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn seed_resolved_partition(
    cache: &FetchSessionCache,
    topic_id: WireUuid,
) -> FetchSessionId {
    allocate_session()
        .cache(cache)
        .partitions(vec![(
            FetchSessionKey {
                topic_name: "t".into(),
                topic_id,
                partition: 0,
            },
            CachedPartitionState {
                fetch_offset: 5,
                max_bytes: 1024,
                ..Default::default()
            },
        )])
        .call()
}

pub(super) fn error_code(cache: &FetchSessionCache, request: &FetchRequest, version: i16) -> i16 {
    match cache.classify(request, version) {
        SessionDecision::Error { code } => code,
        other => panic!("expected Error, got {other:?}"),
    }
}

#[derive(Clone, Copy, Default)]
pub(super) enum SessionPrivilege {
    #[default]
    Consumer,
    Follower,
}

#[derive(Clone, Copy, Default)]
pub(super) enum TopicAddressing {
    #[default]
    Name,
    Id,
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct SessionAllocationSetup {
    pub privilege: SessionPrivilege,
    pub addressing: TopicAddressing,
    #[default("alice".into())]
    pub principal: String,
    pub partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
}

#[bon::builder]
pub(super) fn allocate_session(
    cache: &FetchSessionCache,
    #[builder(default)] privilege: SessionPrivilege,
    #[builder(default)] addressing: TopicAddressing,
    #[builder(default = "alice" . into ())] principal: String,
    #[builder(default)] partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
) -> FetchSessionId {
    let setup = SessionAllocationSetup {
        privilege,
        addressing,
        principal,
        partitions,
    };
    cache.try_allocate(
        matches!(setup.privilege, SessionPrivilege::Follower),
        matches!(setup.addressing, TopicAddressing::Id),
        setup.principal,
        setup.partitions,
    )
}

/// A ten-slot cache with the case's initial session already installed.
#[bon::builder]
pub(super) fn allocated_cache(
    #[builder(default)] privilege: SessionPrivilege,
    #[builder(default)] addressing: TopicAddressing,
    #[builder(default = "alice" . into ())] principal: String,
    #[builder(default)] partitions: Vec<(FetchSessionKey, CachedPartitionState)>,
) -> AllocatedCache {
    let setup = SessionAllocationSetup {
        privilege,
        addressing,
        principal,
        partitions,
    };
    let cache = FetchSessionCache::new(10);
    let id = allocate_session()
        .cache(&cache)
        .privilege(setup.privilege)
        .addressing(setup.addressing)
        .principal(setup.principal)
        .partitions(setup.partitions)
        .call();
    AllocatedCache { cache, id }
}

/// A cache, and the id of the session installed in it.
pub(super) struct AllocatedCache {
    pub(super) cache: FetchSessionCache,
    pub(super) id: FetchSessionId,
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct SessionKeySetup {
    #[default("t".into())]
    pub topic: String,
    pub topic_id: WireUuid,
    pub partition: PartitionIndex,
}

#[bon::builder]
pub(super) fn session_key(
    #[builder(default = "t" . into ())] topic: String,
    #[builder(default)] topic_id: WireUuid,
    #[builder(into, default)] partition: PartitionIndex,
) -> FetchSessionKey {
    let setup = SessionKeySetup {
        topic,
        topic_id,
        partition,
    };
    FetchSessionKey {
        topic_name: setup.topic,
        topic_id: setup.topic_id,
        partition: setup.partition.0,
    }
}
