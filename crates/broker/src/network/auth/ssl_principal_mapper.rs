//! KIP-371 `ssl.principal.mapping.rules`: the Subject DN of an mTLS peer
//! certificate turned into the principal name ACLs are written against.
//!
//! Without a rule the principal is the whole RFC 2253 DN, so every ACL entry
//! and every `super_users` line has to pin `CN=alice,OU=x,O=y` verbatim and a
//! certificate reissue that reorders an RDN invalidates them. A rule list
//! rewrites the DN into a short name once, at accept time, so the rest of the
//! broker only ever sees `alice`.
//!
//! The grammar is Kafka's, and it is *not* the Kerberos `auth_to_local`
//! grammar that [`super::gssapi`] leans on: an entry is either the literal
//! `DEFAULT`, which passes the DN through, or `RULE:pattern/replacement/[L|U]`,
//! where `pattern` has to match the whole DN, `replacement` may reference
//! capture groups as `$1`, and the trailing `L` or `U` lowercases or
//! uppercases the result. The first rule that matches wins.

use super::java_regex::{JavaPattern, ReplacementError};

/// Why a rule spec is not a rule.
#[derive(Debug, thiserror::Error)]
pub enum SslPrincipalRuleError {
    /// The spec is neither `DEFAULT` nor a well-formed `RULE:` entry.
    #[error("expected `DEFAULT` or `RULE:pattern/replacement/[L|U]`")]
    Syntax,
    /// The `pattern` between the first two slashes is not a regular
    /// expression.
    #[error("invalid pattern: {0}")]
    Pattern(String),
    /// The text after the second slash is not one of `L`, `U` or nothing.
    #[error("unknown case flag `{0}`, expected `L`, `U` or nothing")]
    CaseFlag(String),
}

/// What a rule does to the case of its result.
#[derive(Debug, Clone, Copy)]
enum Case {
    Preserve,
    Lower,
    Upper,
}

/// One parsed entry of `ssl.principal.mapping.rules`.
#[derive(Debug, Clone)]
enum Rule {
    /// `DEFAULT`: the Subject DN is the principal name.
    Default,
    /// `RULE:pattern/replacement/[L|U]`.
    Mapping {
        pattern: JavaPattern,
        replacement: String,
        case: Case,
    },
}

impl Rule {
    /// The principal `distinguished_name` maps to, `None` when this rule does
    /// not match and the next one should be tried, or an error when Java
    /// would throw while it rewrites the name.
    fn apply(&self, distinguished_name: &str) -> Result<Option<String>, ReplacementError> {
        match self {
            Self::Default => Ok(Some(distinguished_name.to_owned())),
            Self::Mapping {
                pattern,
                replacement,
                case,
            } => {
                // Kafka checks `Matcher.matches()`, a whole-input match that
                // backtracks to find one, before it rewrites, so a pattern
                // that cannot span the DN falls through to the next rule.
                // A search that hits the engine's backtrack limit is an error
                // here, and not a no match: Java answers it, so falling
                // through to the next rule could map the DN to a name the
                // rules never gave it.
                if !pattern.matches(distinguished_name)? {
                    return Ok(None);
                }
                // The rewrite is then `String.replaceAll` with the pattern as
                // written, so a lazy or alternating pattern replaces its
                // leftmost matches, not the whole-DN match.
                let mapped = pattern.replace(
                    distinguished_name,
                    &escape_literal_back_references(replacement, pattern.group_count())?,
                    true,
                )?;
                Ok(Some(match case {
                    Case::Preserve => mapped,
                    Case::Lower => mapped.to_lowercase(),
                    Case::Upper => mapped.to_uppercase(),
                }))
            }
        }
    }

    /// Parses one spec, the way Kafka's `SslPrincipalMapper.Rule` does.
    fn parse(spec: &str) -> Result<Self, SslPrincipalRuleError> {
        if spec == "DEFAULT" {
            return Ok(Self::Default);
        }
        let body = spec
            .strip_prefix("RULE:")
            .ok_or(SslPrincipalRuleError::Syntax)?;
        let RuleParts {
            pattern,
            replacement,
            flag,
        } = split_rule(body).ok_or(SslPrincipalRuleError::Syntax)?;
        let case = match flag {
            "" => Case::Preserve,
            "L" => Case::Lower,
            "U" => Case::Upper,
            other => return Err(SslPrincipalRuleError::CaseFlag(other.to_owned())),
        };
        Ok(Self::Mapping {
            // Kafka's grammar escapes a literal slash as `\/`. The engine
            // reads that as a plain slash, so the escape is undone here. The
            // replacement keeps it, because `replaceAll` takes `\/` as `/`.
            pattern: JavaPattern::compile(&pattern.replace("\\/", "/"))
                .map_err(SslPrincipalRuleError::Pattern)?,
            replacement: replacement.to_owned(),
            case,
        })
    }
}

/// Kafka's `SslPrincipalMapper.Rule.escapeLiteralBackReferences`: a `$n` that
/// names no group of a pattern with `groups` groups becomes a literal `$n`,
/// after `n` is cut back one digit at a time to a group that exists.
///
/// The port keeps Kafka's arithmetic, including its use of offsets in the
/// unescaped text to edit the text that already carries earlier escapes. Two
/// stray references in one replacement therefore escape the wrong character
/// and leave the second one to throw, exactly as they do on Kafka. The error
/// is the `NumberFormatException` Kafka throws for a reference too long to
/// be a number.
fn escape_literal_back_references(
    unescaped: &str,
    groups: usize,
) -> Result<String, ReplacementError> {
    if groups == 0 {
        return Ok(unescaped.to_owned());
    }
    let groups = i32::try_from(groups).unwrap_or(i32::MAX);
    let original: Vec<char> = unescaped.chars().collect();
    let mut value = original.clone();
    let mut at = 0;
    while let Some(dollar) = (at..original.len()).find(|&index| original[index] == '$') {
        let digits = original[dollar + 1..]
            .iter()
            .take_while(|character| character.is_ascii_digit())
            .count();
        at = dollar + 1 + digits;
        let reference = &original[dollar + 1..at];
        if reference.is_empty() || reference[0] == '0' {
            continue;
        }
        let mut index = reference
            .iter()
            .collect::<String>()
            .parse::<i32>()
            .map_err(|error| ReplacementError(error.to_string()))?;
        while index > groups && index >= 10 {
            index /= 10;
        }
        if index > groups {
            value.insert(dollar, '\\');
        }
    }
    Ok(value.into_iter().collect())
}

/// The three parts of a `RULE:` body.
struct RuleParts<'a> {
    pattern: &'a str,
    replacement: &'a str,
    flag: &'a str,
}

/// Splits a `RULE:` body into `pattern`, `replacement` and the case flag at
/// its first two unescaped slashes. `None` when there are fewer than two.
fn split_rule(body: &str) -> Option<RuleParts<'_>> {
    let mut slashes = [0_usize; 2];
    let mut seen = 0_usize;
    let mut escaped = false;
    for (index, character) in body.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '/' && seen < 2 {
            slashes[seen] = index;
            seen += 1;
        }
    }
    (seen == 2).then(|| RuleParts {
        pattern: &body[..slashes[0]],
        replacement: &body[slashes[0] + 1..slashes[1]],
        flag: &body[slashes[1] + 1..],
    })
}

/// The rule list of one listener's `ssl.principal.mapping.rules`, applied in
/// order with the first match winning.
///
/// The default is Kafka's default value for the property, the single rule
/// `DEFAULT`, which maps every DN to itself.
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub struct SslPrincipalMapper {
    #[default(vec![Rule::Default])]
    rules: Vec<Rule>,
}

impl SslPrincipalMapper {
    /// Parses each spec in order, rejecting the whole list on the first one
    /// that is not a rule.
    ///
    /// # Errors
    ///
    /// [`SslPrincipalRuleError`] when a spec is neither `DEFAULT` nor a
    /// well-formed `RULE:pattern/replacement/[L|U]`.
    pub fn parse<S: AsRef<str>>(specs: &[S]) -> Result<Self, SslPrincipalRuleError> {
        Ok(Self {
            rules: specs
                .iter()
                .map(|spec| Rule::parse(spec.as_ref()))
                .collect::<Result<_, _>>()?,
        })
    }

    /// The principal name for a peer certificate's Subject DN: the first
    /// matching rule's result, or `None` when no rule matches.
    ///
    /// Kafka's `SslPrincipalMapper.getName` throws `NoMatchingRule` in that
    /// case rather than falling back to the DN, and the DN pass-through is the
    /// `DEFAULT` rule's job. An operator whose rule list is exhaustive by
    /// design therefore gets a rejected connection, not a peer authenticated
    /// under its full DN.
    ///
    /// A replacement that Java refuses to expand, such as `$1` in a pattern
    /// with no groups, ends the mapping with no principal. It does not fall
    /// through to the next rule, because Kafka's `getName` propagates the
    /// exception and the connection fails.
    #[must_use]
    pub fn apply(&self, distinguished_name: &str) -> Option<String> {
        for rule in &self.rules {
            match rule.apply(distinguished_name) {
                Ok(None) => {}
                Ok(mapped) => return mapped,
                Err(_) => return None,
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    /// Rules, a distinguished name and the principal the rules map it to.
    struct MappingCase<'a> {
        specs: &'a [&'a str],
        distinguished_name: &'a str,
        expected: Option<&'a str>,
    }

    /// A [`MappingCase`] of `specs` on `distinguished_name`.
    const fn mapping<'a>(
        specs: &'a [&'a str],
        distinguished_name: &'a str,
        expected: Option<&'a str>,
    ) -> MappingCase<'a> {
        MappingCase {
            specs,
            distinguished_name,
            expected,
        }
    }

    fn check_mappings<'a>(cases: impl IntoIterator<Item = MappingCase<'a>>) {
        for MappingCase {
            specs,
            distinguished_name,
            expected,
        } in cases
        {
            let mapper =
                SslPrincipalMapper::parse(specs).unwrap_or_else(|_| panic!("{specs:?} parses"));
            check!(
                mapper.apply(distinguished_name).as_deref() == expected,
                "{specs:?} against {distinguished_name}"
            );
        }
    }

    /// Kafka's own documented `ssl.principal.mapping.rules` examples, each
    /// against a DN it matches and one it does not.
    #[test]
    fn kafka_documented_rules_map_the_subject_dn() {
        // (rules, distinguished name, principal)
        let cases = [
            mapping(
                &["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"][..],
                "CN=serviceuser,OU=ServiceUsers,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("serviceuser"),
            ),
            // No rule matches and there is no DEFAULT tail, so the mapping is
            // refused rather than falling back to the DN.
            mapping(
                &["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"][..],
                "CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                None,
            ),
            mapping(
                &["RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L"][..],
                "CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("adminuser@admin"),
            ),
            mapping(
                &["RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U"][..],
                "cn=User,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("USER"),
            ),
            mapping(
                &["DEFAULT"][..],
                "CN=alice,OU=x,O=y",
                Some("CN=alice,OU=x,O=y"),
            ),
            // First match wins: the specific rule shadows the DEFAULT tail.
            mapping(
                &["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"][..],
                "CN=alice,OU=integration,O=krabka",
                Some("alice"),
            ),
            // The DEFAULT tail is what passes an unmatched DN through.
            mapping(
                &["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"][..],
                "OU=integration,O=krabka",
                Some("OU=integration,O=krabka"),
            ),
            // An explicitly empty list has no rule to match, so it maps
            // nothing.
            mapping(&[][..], "CN=alice,OU=x,O=y", None),
        ];
        check_mappings(cases);
    }

    /// `Matcher.matches()` and `String.replaceAll`, which the `regex` crate
    /// reads differently: `$1_$2` is group 1, an underscore and group 2, a
    /// reference to no group stays literal, and a pattern that only spans the
    /// DN by backtracking still matches.
    #[test]
    fn java_match_and_replacement_semantics() {
        // (rules, distinguished name, principal)
        let cases = [
            mapping(
                &["RULE:^CN=(.*),OU=(.*)$/$1_$2/"][..],
                "CN=alice,OU=admin",
                Some("alice_admin"),
            ),
            mapping(
                &["RULE:^CN=(.*),OU=.*$/$1suffix/"][..],
                "CN=alice,OU=admin",
                Some("alicesuffix"),
            ),
            mapping(
                &["RULE:^CN=(.*),OU=(.*)$/$1@$4/"][..],
                "CN=alice,OU=admin",
                Some("alice@$4"),
            ),
            mapping(
                &["RULE:^CN=(.*),OU=(.*)$/$1@$22/"][..],
                "CN=alice,OU=admin",
                Some("alice@admin2"),
            ),
            // A lazy pattern with no anchors spans the DN by backtracking,
            // and `replaceAll` then rewrites its leftmost matches.
            mapping(&["RULE:CN=(.*?)/$1/"][..], "CN=abc", Some("abc")),
            mapping(&["RULE:a|ab/x/"][..], "ab", Some("xb")),
            mapping(&["RULE:a|ab/x/"][..], "abc", None),
            // `$1` where the pattern has no group is Java's
            // `IndexOutOfBoundsException`: no principal, and no fall-through.
            mapping(&["RULE:^CN=.*$/$1/", "DEFAULT"][..], "CN=alice", None),
            // Kafka's literal-escape edit lands on the wrong character when
            // two references are stray, so the second one throws.
            mapping(&["RULE:^(.*),(.*),(.*)$/$4@$5/"][..], "a,b,c", None),
        ];
        check_mappings(cases);
    }

    /// The configured default is Kafka's `["DEFAULT"]`, so a listener with no
    /// rules of its own still authenticates a peer under its Subject DN.
    #[test]
    fn the_default_mapper_passes_the_subject_dn_through() {
        assert!(
            SslPrincipalMapper::default().apply("CN=alice,OU=x,O=y")
                == Some("CN=alice,OU=x,O=y".to_owned())
        );
    }

    /// A pattern may carry an escaped slash, which the DN it matches carries
    /// literally.
    #[test]
    fn an_escaped_slash_is_part_of_the_pattern() {
        let mapper =
            SslPrincipalMapper::parse(&["RULE:^CN=(.*?)\\/svc$/$1/"]).expect("escaped rule parses");
        assert!(mapper.apply("CN=alice/svc") == Some("alice".to_owned()));
    }

    /// A DN that drives a rule's search past the engine's backtrack limit is
    /// refused. Reading it as "the rule does not match" would hand the DN to
    /// the `DEFAULT` rule after it, and Java, which does answer the search,
    /// would have applied the rule it never got to.
    #[test]
    fn a_dn_that_defeats_a_rule_is_not_passed_on_to_the_next_rule() {
        let hostile = format!("{}b", "a".repeat(40));
        let mapper = SslPrincipalMapper::parse(&["RULE:^(a|aa)+\\1$/x/", "DEFAULT"])
            .expect("the rules parse");

        assert!(mapper.apply(&hostile).is_none());
    }

    /// The pattern reads a DN as Java does: `\w` is ASCII, so a rule built
    /// on it does not take a name with a letter it does not list, and `.`
    /// stops at a line terminator, so a DN with one is not one the rule
    /// covers.
    #[test]
    fn a_rule_reads_the_dn_with_javas_ascii_classes_and_dot() {
        let mapper = SslPrincipalMapper::parse(&["RULE:^CN=(\\w+),.*$/$1/", "DEFAULT"])
            .expect("the rules parse");

        check!(mapper.apply("CN=alice,OU=x") == Some("alice".to_owned()));
        // `é` is not `\w`, so the rule does not match and DEFAULT does.
        check!(mapper.apply("CN=alicé,OU=x") == Some("CN=alicé,OU=x".to_owned()));
        // `.` stops at `\r`, so `.*$` cannot span it.
        check!(mapper.apply("CN=alice,OU=x\r,O=y") == Some("CN=alice,OU=x\r,O=y".to_owned()));
    }

    /// `(?i)` in a rule folds ASCII case only, as Java's does, so the long s
    /// (U+017F) in a DN is not the `s` of a rule. A backreference under `(?i)`
    /// compares the group's text ASCII-insensitively. Each answer is
    /// `SslPrincipalMapper.Rule.apply` on JDK 17.
    #[test]
    fn a_rule_folds_ascii_case_only_under_i() {
        let mapper = SslPrincipalMapper::parse(&["RULE:^CN=(.*?),OU=(?i)serviceusers$/$1/"])
            .expect("the rule parses");

        check!(mapper.apply("CN=alice,OU=ServiceUsers") == Some("alice".to_owned()));
        check!(mapper.apply("CN=alice,OU=\u{17f}erviceUsers").is_none());
    }

    #[yare::parameterized(
        ascii_case_flag = { "RULE:^(?i)(a),\\1$/$1/" },
        unicode_case_flag = { "RULE:^(?iu)(a),\\1$/$1/" },
    )]
    fn a_rule_compares_backreferences_ascii_insensitively(rule: &str) {
        let mapper = SslPrincipalMapper::parse(&[rule]).expect("the rule parses");
        check!(mapper.apply("A,a") == Some("A".to_owned()), "{rule}");
        check!(mapper.apply("A,b").is_none(), "{rule}");
    }

    #[yare::parameterized(
        not_a_rule = { "NOT_A_RULE:::" },
        missing_slash = { "RULE:^CN=(.*?)$" },
        unterminated_replacement = { "RULE:^CN=(.*?)$/$1" },
        invalid_flag = { "RULE:^CN=(.*?)$/$1/X" },
        malformed_pattern = { "RULE:^CN=([a-z$/$1/" },
        lowercase_default = { "default" },
    )]
    fn malformed_specs_are_rejected(spec: &str) {
        check!(SslPrincipalMapper::parse(&[spec]).is_err(), "{spec}");
    }
}
