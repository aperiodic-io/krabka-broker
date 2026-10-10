use super::{
    FeatureKind as K, FeatureLevels, FeatureUpdateDecision as D, FeatureUpdateFacts,
    FeatureUpdateType as T, feature_update_decision,
};

#[derive(Clone, Copy)]
struct FeatureLevel(i16);

#[derive(Clone, Copy, Default)]
enum FeatureFixtureKind {
    #[default]
    DependenciesMet,
    DependenciesUnmet,
    LosslessMetadataChange,
    LossyMetadataChange,
    KRaftVersion,
}

impl FeatureFixtureKind {
    const fn facts(self) -> K {
        match self {
            Self::DependenciesMet => K::Other {
                dependencies_met: true,
            },
            Self::DependenciesUnmet => K::Other {
                dependencies_met: false,
            },
            Self::LosslessMetadataChange => K::MetadataVersion {
                metadata_changed: false,
            },
            Self::LossyMetadataChange => K::MetadataVersion {
                metadata_changed: true,
            },
            Self::KRaftVersion => K::KRaftVersion,
        }
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FeatureRowSetup {
    #[default(T::Upgrade)]
    update_type: T,
    #[default(FeatureLevel(1))]
    requested: FeatureLevel,
    #[default(FeatureLevel(0))]
    current: FeatureLevel,
    kind: FeatureFixtureKind,
}

const fn row(setup: FeatureRowSetup) -> FeatureUpdateFacts {
    FeatureUpdateFacts {
        update_type: Some(setup.update_type),
        levels: FeatureLevels {
            requested: setup.requested.0,
            current: setup.current.0,
        },
        all_nodes_support: true,
        kind: setup.kind.facts(),
    }
}

const UNMET: FeatureFixtureKind = FeatureFixtureKind::DependenciesUnmet;
const LOSSY: FeatureFixtureKind = FeatureFixtureKind::LossyMetadataChange;
const LOSSLESS: FeatureFixtureKind = FeatureFixtureKind::LosslessMetadataChange;

mod shared_checks_follow_update_feature_order;
