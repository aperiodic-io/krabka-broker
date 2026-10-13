//! Flat constructor tables for the four existing broker fixture setup domains.

use moxy::{
    ast::{Attributes, Parse, ParseError, Parser, Path, Token, Type, Visibility},
    token::{Ident, TokenStream},
};

enum Domain {
    TemporaryPartition,
    OpenPartition,
    Election,
    Supervisor,
    Quota,
}

struct Constructor {
    attributes: Attributes,
    visibility: Visibility,
    name: Ident,
    output: Type,
    helper: Path,
}

struct ConstructorBody {
    parameters: TokenStream,
    arguments: TokenStream,
}

fn body(domain: &Domain) -> ConstructorBody {
    match domain {
        Domain::TemporaryPartition | Domain::OpenPartition => {
            let directory = matches!(domain, Domain::OpenPartition);
            ConstructorBody {
                parameters: moxy::template! {
                    @if directory { log_dir: &::std::path::Path, }
                    #[builder(default = StandalonePartitionSetup::default().topic)] topic: &'_ str,
                    #[builder(into, default = StandalonePartitionSetup::default().partition)] partition: ::krabka_ids::PartitionIndex,
                    #[builder(default = StandalonePartitionSetup::default().storage)] storage: StorageMode,
                },
                arguments: moxy::template! { @if directory { log_dir, } topic, partition, storage },
            }
        }
        Domain::Election => ConstructorBody {
            parameters: moxy::template! {
                #[builder(default = ElectionSetup::default().topic)] topic: &'_ str,
                #[builder(into, default = ElectionSetup::default().partition)] partition: ::krabka_ids::PartitionIndex,
                #[builder(into, default = ElectionSetup::default().leader)] leader: NodeId,
                #[builder(default = ElectionSetup::default().replicas)] replicas: &'_ [NodeId],
                #[builder(default = ElectionSetup::default().isr)] isr: &'_ [NodeId],
                #[builder(default = ElectionSetup::default().dirs)] dirs: &'_ [Uuid],
                #[builder(default = ElectionSetup::default().configs)] configs: &'_ [TopicConfigOverride<'_>],
                #[builder(default = ElectionSetup::default().elr)] elr: ElrFinalization,
            },
            arguments: moxy::template! { ElectionSetup { topic, partition, leader, replicas, isr, dirs, configs, elr } },
        },
        Domain::Supervisor => ConstructorBody {
            parameters: moxy::template! {
                #[builder(default = "t")] topic: &'_ str,
                #[builder(into, default)] partition: PartitionIndex,
                #[builder(into, default = NodeId(1))] leader: NodeId,
                #[builder(default = vec![NodeId(1), NodeId(2), NodeId(3)])] replicas: Vec<NodeId>,
                #[builder(into, default)] epoch: LeaderEpoch,
            },
            arguments: moxy::template! { SupervisorPartitionSetup { topic, partition, leader, replicas, epoch } },
        },
        Domain::Quota => ConstructorBody {
            // Quota fixture entities borrow their explicit type/name coordinates.
            parameters: moxy::template! {
                #[builder(default = vec![QuotaEntityInput::user(Some("alice"))])] entity: Vec<QuotaEntityInput<'_>>,
                #[builder(default = "producer_byte_rate")] key: &'_ str,
                #[builder(into, default = QuotaValue(1_000.0))] value: QuotaValue,
            },
            arguments: moxy::template! { QuotaRecordSetup { entity, key, value } },
        },
    }
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let domain = Ident::parse(&parser)?;
    let domain = match domain.text() {
        "temporary_partition" => Domain::TemporaryPartition,
        "open_partition" => Domain::OpenPartition,
        "election" => Domain::Election,
        "supervisor" => Domain::Supervisor,
        "quota" => Domain::Quota,
        _ => {
            return Err(ParseError::new(
                domain.span(),
                "expected an existing broker fixture setup domain",
            ));
        }
    };
    <Token![:]>::parse(&parser)?;
    let mut declarations = Vec::new();
    while !parser.is_empty() {
        let attributes = Attributes::parse(&parser)?;
        let visibility = Visibility::parse(&parser)?;
        let name = Ident::parse(&parser)?;
        if declarations
            .iter()
            .any(|declaration: &Constructor| declaration.name.text() == name.text())
        {
            return Err(ParseError::new(
                name.span(),
                "fixture constructor name must be distinct",
            ));
        }
        <Token![->]>::parse(&parser)?;
        let output = Type::parse(&parser)?;
        <Token![=]>::parse(&parser)?;
        let helper = Path::parse(&parser)?;
        <Token![;]>::parse(&parser)?;
        declarations.push(Constructor {
            attributes,
            visibility,
            name,
            output,
            helper,
        });
    }
    if declarations.is_empty() {
        return Err(parser.error("declare at least one fixture constructor"));
    }
    let ConstructorBody {
        parameters,
        arguments,
    } = body(&domain);
    Ok(moxy::template! {
        @for declaration in &declarations {
            {{ declaration.attributes }}
            #[bon::builder]
            {{ declaration.visibility }} fn {{ declaration.name }}({{ parameters }}) -> {{ declaration.output }} {
                {{ declaration.helper }}({{ arguments }})
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn fixture_domains_keep_defaults_argument_order_and_visible_return_types() {
        for domain in [
            "temporary_partition",
            "open_partition",
            "election",
            "supervisor",
            "quota",
        ] {
            let input =
                format!("{domain}: pub first -> Output = construct; second -> Other = other;");
            let output = compact(&expand(&input.parse().unwrap()).unwrap());
            assert!(output.find("fnfirst").unwrap() < output.find("fnsecond").unwrap());
            assert!(output.contains("->Output{construct(") && output.contains("->Other{other("));
            assert!(output.contains("#[bon::builder]"));
            assert!(output.contains("log_dir:&::std::path::Path") == (domain == "open_partition"));
        }
        let election =
            compact(&expand(&"election: seed -> Record = constructor;".parse().unwrap()).unwrap());
        assert!(
            election
                .contains("ElectionSetup{topic,partition,leader,replicas,isr,dirs,configs,elr}")
        );
        assert!(election.contains("ElectionSetup::default().elr"));
    }

    #[test]
    fn constructor_table_rejects_unknown_empty_duplicate_and_malformed_rows() {
        for input in [
            "unknown: a -> Output = make;",
            "election:",
            "quota: a -> Output = make; a -> Output = again;",
            "supervisor: a -> Output = make + other;",
            "open_partition: a = make;",
        ] {
            assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
        }
    }
}
