//! Validation and parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! There are three keys only, which match
//! `org.apache.kafka.server.metrics.ClientMetricsConfigs`. `metrics` is a CSV
//! prefix list, where the single token `"*"` means all. `interval.ms` is an
//! int in `100..=3_600_000` with default 300000. `match` is a CSV of
//! `selector=regex`, where the regex is a `java.util.regex.Pattern`.

mod java_case;
mod java_fold;

use std::collections::BTreeMap;

use fancy_regex::Regex;

use self::java_fold::Escape;
use crate::config_keys::parse::{check_valid_list, int_value, java_trim, list_value, parse_int};

pub(crate) const KEY_METRICS: &str = "metrics";
pub(crate) const KEY_INTERVAL_MS: &str = "interval.ms";
pub(crate) const KEY_MATCH: &str = "match";

/// Kafka's `ClientMetricsConfigs.INTERVAL_MS_DEFAULT`: the push interval of a
/// subscription with no `interval.ms`, and the cap every client starts from.
pub(crate) const INTERVAL_MS_DEFAULT: i32 = 300_000;
const MIN_INTERVAL_MS: i32 = 100;
const MAX_INTERVAL_MS: i32 = 3_600_000;
pub(crate) const ALL_METRICS: &str = "*";

/// Why `ClientMetricsConfigs.validate` refused a subscription's configs. The
/// variant is the Kafka exception, so it decides the wire error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigError {
    /// `InvalidRequestException`: `INVALID_REQUEST` (42).
    InvalidRequest(String),
    /// `ConfigException` or `InvalidConfigurationException`: `INVALID_CONFIG`
    /// (40).
    InvalidConfig(String),
}

impl ConfigError {
    /// The wire error code Kafka's `ConfigurationControlManager` answers.
    pub(crate) fn code(&self) -> i16 {
        match self {
            Self::InvalidRequest(_) => crate::codes::INVALID_REQUEST,
            Self::InvalidConfig(_) => crate::codes::INVALID_CONFIG,
        }
    }

    /// The error message Kafka puts on the wire.
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::InvalidRequest(message) | Self::InvalidConfig(message) => message,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchSelector {
    InstanceId,
    Id,
    SoftwareName,
    SoftwareVersion,
    SourceAddress,
    SourcePort,
}

impl MatchSelector {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "client_instance_id" => Self::InstanceId,
            "client_id" => Self::Id,
            "client_software_name" => Self::SoftwareName,
            "client_software_version" => Self::SoftwareVersion,
            "client_source_address" => Self::SourceAddress,
            "client_source_port" => Self::SourcePort,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MatchRule {
    pub selector: MatchSelector,
    pub pattern: Regex,
}

/// Kafka's `ClientMetricsConfigs.validate(subscriptionName, configs)`, run on
/// the subscription's whole config map after the alteration is applied.
///
/// The checks run in Kafka's order: the subscription name, then every key
/// against the three known names (`InvalidRequestException`), then
/// `ConfigDef.parse` in definition order (`metrics`, `interval.ms`, `match`,
/// each a `ConfigException`), then the `interval.ms` range
/// (`InvalidRequestException`), then the `match` patterns
/// (`InvalidConfigurationException`).
pub(crate) fn validate(
    subscription: &str,
    configs: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    if subscription.is_empty() {
        return Err(ConfigError::InvalidRequest(
            "Subscription name can't be empty".into(),
        ));
    }
    if let Some(key) = configs
        .keys()
        .find(|key| !matches!(key.as_str(), KEY_METRICS | KEY_INTERVAL_MS | KEY_MATCH))
    {
        return Err(ConfigError::InvalidRequest(format!(
            "Unknown client metrics configuration: {key}"
        )));
    }
    if let Some(value) = configs.get(KEY_METRICS) {
        check_valid_list(KEY_METRICS, value, &[], true).map_err(ConfigError::InvalidConfig)?;
    }
    let interval = configs
        .get(KEY_INTERVAL_MS)
        .map(|value| parse_int(KEY_INTERVAL_MS, value).map_err(ConfigError::InvalidConfig))
        .transpose()?;
    let patterns = configs
        .get(KEY_MATCH)
        .map(|value| check_valid_list(KEY_MATCH, value, &[], true))
        .transpose()
        .map_err(ConfigError::InvalidConfig)?;
    if let Some(interval) = interval
        && !(MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&interval)
    {
        return Err(ConfigError::InvalidRequest(format!(
            "Invalid value {interval} for {KEY_INTERVAL_MS}, interval must be between 100 and \
             3600000 (1 hour)"
        )));
    }
    if let Some(patterns) = patterns {
        parse_match_patterns(&patterns)?;
    }
    Ok(())
}

/// Effective push interval for a subscription's override map. The function
/// returns `default_interval_ms` when the key is unset.
pub(crate) fn effective_interval_ms(
    configs: &BTreeMap<String, String>,
    default_interval_ms: i32,
) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| int_value(v.as_str()))
        .unwrap_or(default_interval_ms)
}

/// Parse the `metrics` value into prefixes, as `ConfigDef` parses a `LIST`.
/// `"*"` collapses to `["*"]`. An empty string gives an empty list, which
/// means no metrics.
pub(crate) fn parse_metrics(value: &str) -> Vec<String> {
    list_value(value).into_iter().map(str::to_string).collect()
}

/// Parse the `match` value into compiled selector rules. An empty value
/// matches all.
pub(crate) fn parse_match_rules(value: &str) -> Result<Vec<MatchRule>, ConfigError> {
    parse_match_patterns(&list_value(value))
}

/// Kafka's `ClientMetricsConfigs.parseMatchingPatterns`: each entry splits on
/// every `=` as Java's `String.split("=")` does (trailing empty parts
/// dropped) and must give exactly two parts, a known selector and a pattern
/// `java.util.regex.Pattern` compiles. Every refusal is
/// `InvalidConfigurationException` naming the whole entry.
fn parse_match_patterns(patterns: &[&str]) -> Result<Vec<MatchRule>, ConfigError> {
    let mut rules = Vec::new();
    for &entry in patterns {
        let illegal =
            || ConfigError::InvalidConfig(format!("Illegal client matching pattern: {entry}"));
        let mut parts: Vec<&str> = entry.split('=').collect();
        while parts.last().is_some_and(|part| part.is_empty()) {
            parts.pop();
        }
        let [name, pattern] = parts.as_slice() else {
            return Err(illegal());
        };
        let selector = MatchSelector::parse(java_trim(name)).ok_or_else(illegal)?;
        let pattern = java_to_fancy(java_trim(pattern)).ok_or_else(illegal)?;
        // Kafka tests a selector with `Matcher.matches()`, a full match. The
        // anchored group gives the same answer for an alternation such as
        // `app|app-1`, where a leftmost-first search would stop at `app`.
        let pattern = Regex::new(&format!("^(?:{pattern})$")).map_err(|_| illegal())?;
        // Kafka's `parseMatchingPatterns` puts each entry into a map by
        // selector, so a repeated selector keeps its last pattern.
        rules.retain(|rule: &MatchRule| rule.selector != selector);
        rules.push(MatchRule { selector, pattern });
    }
    Ok(rules)
}

/// Rewrite a `java.util.regex.Pattern` into `fancy_regex` syntax, or `None`
/// for a group opener Java refuses, a `)` that closes no group, a class range
/// Java refuses (`[a-\w]`), a `{` outside a class that does not open the
/// bounds of a quantifier (`a{x}`), a `\N{name}`, which the rewrite cannot
/// read, or a quantifier after inline flags that leave no text (`(?i){2}`),
/// which `fancy_regex` has no form for.
///
/// `fancy_regex` accepts Python and Oniguruma forms Java refuses, such as
/// `(?P<name>x)`, `(?P=name)`, `(?'name'x)` and `(?~x)`, so every `(?` outside
/// a character class must be one of Java's openers: `(?:`, `(?=`, `(?!`,
/// `(?>`, `(?<=`, `(?<!`, `(?<name>` with an ASCII-letter-then-alphanumeric
/// name, or inline flags from `idmsuxU-`. Java's `\Q...\E` quoting, which
/// `fancy_regex` lacks, becomes escaped literals before anything else is read,
/// as Java does it.
///
/// Five things read differently by default, and the rewrite gives the Java
/// reading, following the flags in force at each place (`(?s)`, `(?d)`,
/// `(?U)`, `(?i)`, `(?u)` and `(?x)`, alone or scoped to a group):
///
/// - `\w`, `\d`, `\s`, `\b` and their negations are ASCII, where
///   `fancy_regex`'s are Unicode, until `(?U)` (`UNICODE_CHARACTER_CLASS`)
///   asks for the Unicode ones.
/// - The POSIX classes `\p{Alpha}`, `\p{Alnum}`, `\p{Punct}` and the rest are
///   ASCII too, where `fancy_regex`'s are Unicode, and under `(?U)` they are
///   the Unicode properties that Java gives them.
/// - `.` does not match `\n`, `\r`, U+0085, U+2028 or U+2029, where
///   `fancy_regex`'s stops at `\n` only, until `(?s)` (`DOTALL`) lets it match
///   any, or `(?d)` (`UNIX_LINES`) leaves `\n` the only line terminator.
/// - `(?i)` (`CASE_INSENSITIVE`) folds ASCII case only, where `fancy_regex`'s
///   `i` folds Unicode case, until `(?u)` (`UNICODE_CASE`) or `(?U)` asks for
///   Unicode folding, and Java's Unicode folding is not `fancy_regex`'s. The
///   rewrite never turns the fancy `i` on. It writes the folding out: both
///   cases of each ASCII letter under an ASCII fold, and under a Unicode fold
///   the code points that Java accepts for a letter, a class member and a class
///   range, in a class and out of one ([`java_case`]). A backreference is
///   written `(?i:\1)`, and differs from Java's for some letters, which
///   [`java_fold`] describes.
/// - Under `(?x)` (`COMMENTS`) the rewrite drops the whitespace and the `#`
///   comments Java ignores, so that the text of a comment is never read as
///   pattern.
pub(crate) fn java_to_fancy(pattern: &str) -> Option<String> {
    let chars = java_fold::remove_qe_quoting(&pattern.chars().collect::<Vec<char>>());
    let mut translator = Translator {
        chars: &chars,
        out: String::with_capacity(pattern.len()),
        scope: Scope::default(),
        outer: Vec::new(),
        class_depth: 0,
        class_start: false,
        run: 0,
        prior_run: 0,
        at: 0,
    };
    while translator.at < chars.len() {
        translator.step()?;
    }
    Some(translator.out)
}

/// The state of one [`java_to_fancy`] pass over a pattern.
struct Translator<'a> {
    chars: &'a [char],
    out: String,
    /// The Java flags in force here.
    scope: Scope,
    /// The flags of the groups around here, the innermost last.
    outer: Vec<Scope>,
    /// How many character classes are open here.
    class_depth: usize,
    /// The next char is the first in a class, where a `]` is a member.
    class_start: bool,
    /// How many literal chars, counting the one just read, Java has read as
    /// one run since the last token that is not one.
    run: usize,
    /// What `run` was before the token now being read.
    prior_run: usize,
    /// The index of the next char to read.
    at: usize,
}

impl Translator<'_> {
    fn in_class(&self) -> bool {
        self.class_depth > 0
    }

    /// Copies `len` chars as they are.
    fn copy(&mut self, len: usize) {
        self.out.extend(&self.chars[self.at..self.at + len]);
        self.at += len;
    }

    /// Reads and writes the next token: a char, an escape, a group opener, or a
    /// run of `(?x)` whitespace and comment.
    fn step(&mut self) -> Option<()> {
        let c = self.chars[self.at];
        let first_in_class = std::mem::take(&mut self.class_start);
        let ignored = self.ignored_len(self.at);
        if ignored > 0 {
            self.class_start = first_in_class;
            self.at += ignored;
            return Some(());
        }
        self.prior_run = std::mem::take(&mut self.run);
        match c {
            '\\' => self.escape()?,
            '(' if !self.in_class() => self.group()?,
            // Every `(` that opens a group pushed its flags, so an empty stack
            // is a `)` with no group: `Unmatched closing ')'` in Java.
            ')' if !self.in_class() => {
                self.scope = self.outer.pop()?;
                self.copy(1);
            }
            '[' => self.open_class(),
            // First in a class, a `]` is a member, and may start a range.
            ']' if self.in_class() && !first_in_class => {
                self.class_depth -= 1;
                self.out.push(']');
                self.at += 1;
            }
            '.' if !self.in_class() && !self.scope.dot.dotall && !self.scope.dot.unix_lines => {
                self.out.push_str(JAVA_DOT);
                self.at += 1;
            }
            '{' if !self.in_class() => self.braces()?,
            '&' if self.in_class() => self.ampersand()?,
            _ => self.atom(u32::from(c), 1)?,
        }
        Some(())
    }

    /// The chars of `(?x)` whitespace and comment at `at`: none unless the
    /// `x` flag is on.
    fn ignored_len(&self, at: usize) -> usize {
        if self.scope.comments {
            java_fold::ignorable_len(&self.chars[at..], self.scope.dot.unix_lines)
        } else {
            0
        }
    }

    /// The escape at `at`.
    fn escape(&mut self) -> Option<()> {
        let in_class = self.in_class();
        let (escape, len) = java_fold::read_escape(self.chars, self.at, in_class);
        match escape {
            // `fancy_regex` reads `\N{name}` as `\N` and the text of the name,
            // and the rewrite has no table of Unicode names to write the char.
            Escape::Named => return None,
            // Java compares the text of the group case-insensitively, which
            // `fancy_regex` can do only for a whole reference. See
            // [`java_fold`] for what that leaves different.
            Escape::BackRef if self.scope.case.ignore => {
                self.out.push_str("(?i:");
                self.copy(len);
                self.out.push(')');
            }
            Escape::Char(code) => self.atom(code, len)?,
            Escape::Property { name, negated } => {
                match java_fold::property_class(&name, negated, in_class, self.scope) {
                    Some(class) => {
                        self.out.push_str(&class);
                        self.at += len;
                    }
                    None => self.copy(len),
                }
            }
            Escape::BackRef | Escape::Other => {
                let ascii = self
                    .chars
                    .get(self.at + 1)
                    .and_then(|&next| ascii_shorthand(next, in_class));
                match ascii {
                    Some(ascii) if !self.scope.unicode_classes => {
                        self.out.push_str(ascii);
                        self.at += len;
                    }
                    _ => self.copy(len),
                }
            }
        }
        Some(())
    }

    /// One char, `len` chars long in the pattern and standing for `code`, and
    /// in a class the range it starts. Under an ASCII fold a letter is both of
    /// its cases, and a range takes the other case of the letters in it. Under
    /// a Unicode fold each is the set of code points that Java's predicate
    /// accepts. `None` for a range that Java refuses.
    fn atom(&mut self, code: u32, len: usize) -> Option<()> {
        let (chars, start, in_class) = (self.chars, self.at, self.in_class());
        let fold = self.scope.ascii_fold();
        let unicode_fold = self.scope.unicode_fold();
        let after = start + len;
        let range = if in_class {
            self.range_end(after)
        } else {
            RangeEnd::Single
        };
        match range {
            RangeEnd::Illegal => return None,
            RangeEnd::To {
                end,
                code: end_code,
                len: end_len,
            } => {
                if unicode_fold {
                    // Java's `Illegal character range`, which the fold would
                    // otherwise hide in the explicit set.
                    if end_code < code {
                        return None;
                    }
                    java_case::push_members(&mut self.out, &java_case::range(code, end_code));
                } else {
                    let (first, last) = (&chars[start..after], &chars[end..end + end_len]);
                    java_fold::push_member(&mut self.out, first, code, true);
                    self.out.push('-');
                    java_fold::push_member(&mut self.out, last, end_code, true);
                    if fold {
                        java_fold::push_other_case_ranges(&mut self.out, code, end_code);
                    }
                }
                self.at = end + end_len;
            }
            RangeEnd::Single => {
                let source = &chars[start..after];
                if !in_class && !is_meta(source) {
                    self.run = self.prior_run + 1;
                }
                if fold {
                    java_fold::push_folded(&mut self.out, source, code, in_class);
                } else if let Some(cases) = unicode_fold.then(|| self.cases(code, after)).flatten()
                {
                    if in_class {
                        java_case::push_members(&mut self.out, &cases);
                    } else {
                        java_case::push_class(&mut self.out, &cases);
                    }
                } else {
                    java_fold::push_member(&mut self.out, source, code, in_class);
                }
                self.at = after;
            }
        }
        Some(())
    }

    /// The code points that the char `code`, which ends before `after`,
    /// matches under a Unicode fold, or `None` where it has no case and
    /// matches only itself.
    fn cases(&self, code: u32, after: usize) -> Option<java_case::Ranges> {
        if self.in_class() {
            java_case::member(code)
        } else {
            java_case::literal(code, || self.in_literal_run(after))
        }
    }

    /// Whether Java reads the literal char that ends before `after` in a run
    /// of two or more, as one slice, and not on its own. A run is the literal
    /// chars up to the next token that is not one, and Java takes the last
    /// char off it when a quantifier follows, as the quantifier binds to that
    /// char alone. `run` counts the chars of the run up to this one.
    fn in_literal_run(&self, after: usize) -> bool {
        let (chars, mut at, mut following) = (self.chars, after, 0);
        loop {
            at += self.ignored_len(at);
            match chars.get(at) {
                Some('\\') => match java_fold::read_escape(chars, at, false) {
                    (Escape::Char(_), len) => at += len,
                    _ => break,
                },
                Some(&c) if !is_meta(&[c]) && !matches!(c, '(' | ')' | '[') => at += 1,
                _ => break,
            }
            following += 1;
        }
        let quantified = matches!(chars.get(at), Some('*' | '+' | '?' | '{'));
        let unwound = quantified && following == 0;
        !unwound && self.run + following - usize::from(quantified) >= 2
    }

    /// A `&` in a class, which is the intersection operator when another `&`
    /// follows. Java reads `&&` before any member, so the second `&` is never
    /// the start of a range, as in `[a&&-1]`.
    fn ampersand(&mut self) -> Option<()> {
        let second = self.at + 1 + self.ignored_len(self.at + 1);
        if self.chars.get(second) == Some(&'&') {
            self.out.push_str("&&");
            self.at = second + 1;
            Some(())
        } else {
            self.atom(u32::from('&'), 1)
        }
    }

    /// A `{`, which starts the bounds of a quantifier, `{n}`, `{n,}` or
    /// `{n,m}`. They are copied whole, so that the digits in them are not
    /// read as literal chars of a run. Any other `{` outside a class is Java's
    /// `Illegal repetition`, and `None` here.
    fn braces(&mut self) -> Option<()> {
        let rest = &self.chars[self.at..];
        let end = rest.iter().position(|&c| c == '}')?;
        let bounds = &rest[1..end];
        if bounds.first().is_some_and(char::is_ascii_digit)
            && bounds.iter().all(|&c| c.is_ascii_digit() || c == ',')
        {
            self.copy(end + 1);
            Some(())
        } else {
            None
        }
    }

    /// How the class member that ends before `after` goes on. Java reads a `-`
    /// after a member and before a char that is not `]` or `[` as the range of
    /// the two. It looks at the char right after the `-`, without skipping
    /// `(?x)` white space, so `(?x)[a- ]` is a range from `a` to the `]` past
    /// the space, and an illegal one.
    fn range_end(&self, after: usize) -> RangeEnd {
        let dash = after + self.ignored_len(after);
        if self.chars.get(dash) != Some(&'-') {
            return RangeEnd::Single;
        }
        if matches!(self.chars.get(dash + 1), None | Some(']' | '[')) {
            return RangeEnd::Single;
        }
        let end = dash + 1 + self.ignored_len(dash + 1);
        match java_fold::read_member(self.chars, end) {
            Some((code, len)) => RangeEnd::To { end, code, len },
            // The end is an escape for more than one char, or none Java
            // reads: `[a-\w]` is `Illegal character range`, and `[a-\p{L}]`
            // is `Illegal/unsupported escape sequence`.
            None => RangeEnd::Illegal,
        }
    }

    /// A `[`, which opens a class, or a class in a class.
    fn open_class(&mut self) {
        self.class_depth += 1;
        self.class_start = true;
        self.copy(1);
        if self.chars.get(self.at) == Some(&'^') {
            self.copy(1);
        }
    }

    /// A `(`: a group, a lookaround, or inline flags.
    fn group(&mut self) -> Option<()> {
        let chars = self.chars;
        if chars.get(self.at + 1) != Some(&'?') {
            self.outer.push(self.scope);
            self.copy(1);
            return Some(());
        }
        let rest = &chars[self.at + 2..];
        let opener = match rest.first() {
            Some(':' | '=' | '!' | '>') => 3,
            Some('<') => match rest.get(1) {
                Some('=' | '!') => 4,
                Some(first) if first.is_ascii_alphabetic() => {
                    let name = rest[2..].iter().position(|c| !c.is_ascii_alphanumeric())?;
                    if rest[2 + name] != '>' {
                        return None;
                    }
                    name + 5
                }
                _ => return None,
            },
            Some(_) => return self.flag_group(rest),
            None => return None,
        };
        self.outer.push(self.scope);
        self.copy(opener);
        Some(())
    }

    /// Inline flags, `rest` starting just after their `(?`.
    fn flag_group(&mut self, rest: &[char]) -> Option<()> {
        let group = java_flag_group(rest)?;
        // `(?flags:x)` is a group the flags last to the end of, `(?flags)`
        // lasts to the end of the group it is in.
        if group.scoped {
            self.outer.push(self.scope);
        }
        for &(flag, on) in &group.changes {
            self.scope.set(flag, on);
        }
        let translation = group.translation();
        // Java refuses `(?i)*` as a dangling quantifier; an emptied group must
        // not hand it to the atom before it. Java repeats the empty match for
        // `(?i){2}`, which `fancy_regex` has no form for, and it would read the
        // braces as text.
        let after = self.at + 2 + group.len;
        if translation.is_empty()
            && matches!(
                self.chars.get(after + self.ignored_len(after)),
                Some('*' | '+' | '?' | '{')
            )
        {
            return None;
        }
        self.out.push_str(&translation);
        self.at += 2 + group.len;
        Some(())
    }
}

/// What a `-` after a class member makes of the member, as
/// [`Translator::range_end`] reads it.
enum RangeEnd {
    /// No range: there is no `-`, or it is a member of its own.
    Single,
    /// A range that ends with the char of `len` chars at `end`, which stands
    /// for `code`.
    To { end: usize, code: u32, len: usize },
    /// A range Java refuses.
    Illegal,
}

/// The Java flags that change what [`java_to_fancy`] writes for a construct.
#[derive(Debug, Clone, Copy, Default)]
struct Scope {
    /// `s` and `d`: what `.` matches.
    dot: Dot,
    /// `U` (`UNICODE_CHARACTER_CLASS`): `\w`, `\d`, `\s` and `\b` are Unicode.
    unicode_classes: bool,
    /// `i` and `u`: how letters match.
    case: Case,
    /// `x` (`COMMENTS`): whitespace and `#` comments are ignored.
    comments: bool,
}

/// The [`Scope`] flags for `.` and the line terminators.
#[derive(Debug, Clone, Copy, Default)]
struct Dot {
    /// `s` (`DOTALL`): `.` matches a line terminator.
    dotall: bool,
    /// `d` (`UNIX_LINES`): `\n` is the only line terminator.
    unix_lines: bool,
}

/// The [`Scope`] flags for the case of letters.
#[derive(Debug, Clone, Copy, Default)]
struct Case {
    /// `i` (`CASE_INSENSITIVE`): letters match in both cases.
    ignore: bool,
    /// `u` (`UNICODE_CASE`), which `U` turns on too: `i` folds Unicode case,
    /// and not only ASCII.
    unicode: bool,
}

impl Scope {
    fn set(&mut self, flag: char, on: bool) {
        match flag {
            's' => self.dot.dotall = on,
            'd' => self.dot.unix_lines = on,
            'i' => self.case.ignore = on,
            'u' => self.case.unicode = on,
            'x' => self.comments = on,
            // As in `Pattern.addFlag` and `Pattern.subFlag`, `U` takes
            // `UNICODE_CASE` with it, in both directions.
            'U' => {
                self.unicode_classes = on;
                self.case.unicode = on;
            }
            _ => {}
        }
    }

    /// `i` without `u`: Java folds ASCII case only, which `fancy_regex` cannot
    /// do, so the rewrite writes both cases and leaves the fancy `i` off.
    fn ascii_fold(self) -> bool {
        self.case.ignore && !self.case.unicode
    }

    /// `i` with `u`: Java folds Unicode case, by a relation other than the
    /// simple case folding of the fancy `i`, so the rewrite writes the code
    /// points that Java accepts and leaves the fancy `i` off.
    fn unicode_fold(self) -> bool {
        self.case.ignore && self.case.unicode
    }
}

/// Whether `source`, the pattern text of one token that [`Translator::atom`]
/// reads, is a char that Java does not take into a run of literal chars: a
/// quantifier, an anchor, `.` or `|`.
fn is_meta(source: &[char]) -> bool {
    matches!(source, ['*' | '+' | '?' | '{' | '$' | '.' | '^' | '|'])
}

/// What Java's `.` matches by default: anything but the line terminators
/// `\n`, `\r`, U+0085, U+2028 and U+2029 (`Pattern.Dot`, without `DOTALL`
/// or `UNIX_LINES`).
const JAVA_DOT: &str = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]";

/// The ASCII form of a Java shorthand class, or word boundary, which is what
/// Java reads without `(?U)`: `\w` is `[a-zA-Z_0-9]`, `\d` is `[0-9]`, `\s` is
/// `[ \t\n\x0B\f\r]`, and `\b` is the edge of a run of `\w` (as of JDK 19,
/// where it stopped reading Unicode letters and digits).
///
/// `escape` is the character after the backslash. Inside a character class
/// the positive forms are the bare members, and the negations are a nested
/// negated class. `None` for anything else, and for a boundary in a class.
fn ascii_shorthand(escape: char, in_class: bool) -> Option<&'static str> {
    Some(match (escape, in_class) {
        ('w', false) => "[0-9A-Za-z_]",
        ('w', true) => "0-9A-Za-z_",
        ('W', _) => "[^0-9A-Za-z_]",
        ('d', false) => "[0-9]",
        ('d', true) => "0-9",
        ('D', _) => "[^0-9]",
        ('s', false) => "[ \\t\\n\\x0B\\x0C\\r]",
        ('s', true) => " \\t\\n\\x0B\\x0C\\r",
        ('S', _) => "[^ \\t\\n\\x0B\\x0C\\r]",
        ('b', false) => "(?:(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(?<![0-9A-Za-z_])(?=[0-9A-Za-z_]))",
        ('B', false) => "(?:(?<=[0-9A-Za-z_])(?=[0-9A-Za-z_])|(?<![0-9A-Za-z_])(?![0-9A-Za-z_]))",
        _ => return None,
    })
}

/// Translate a Java inline flag group, `rest` starting just after its `(?`,
/// into the flags that `fancy_regex` shares and the flags [`Scope`] follows.
/// Returns `None` where `Pattern.compile` answers "Unknown inline modifier".
///
/// Java's `Pattern.addFlag` takes `idmsuxcU`, then optionally one `-` and
/// the same letters to clear, then `)` or `:`; an empty group such as `(?)`
/// or `(?-:x)` is valid. `fancy_regex` refuses empty groups and gives `U` a
/// different meaning, so only the flags it shares keep their letter:
///
/// - `m` and `s` (`MULTILINE`, `DOTALL`) pass through, and [`Scope`] follows
///   `s`.
/// - `x` (`COMMENTS`) is read by [`Scope`] and dropped from the output: the
///   rewrite removes the whitespace and the comments itself. `fancy_regex`'s
///   `x` would also hide the space and the `#` that the rewrite writes after
///   the flag's scope has ended, as `fancy_regex` does not end the flags set
///   inside a capturing group at its `)`.
/// - `i` (`CASE_INSENSITIVE`) and `u` (`UNICODE_CASE`) are read by
///   [`Scope`] and dropped from the output: `fancy_regex`'s `i` folds Unicode
///   case by another relation than Java's, with or without `u`, so
///   [`Translator`] writes the folding out. See [`java_fold`].
/// - `d` (`UNIX_LINES`) is dropped from the output, and read by
///   [`java_to_fancy`] to translate `.`.
/// - `U` (`UNICODE_CHARACTER_CLASS`) is dropped from the output, and read by
///   [`java_to_fancy`] to keep `\w`, `\d`, `\s` and `\b` Unicode where it
///   otherwise makes them ASCII, and to fold Unicode case, which Java's `U`
///   turns on. Passing it through would swap greediness in `fancy_regex`.
/// - `c` (`CANON_EQ`) is dropped: canonical-equivalence matching has no
///   `fancy_regex` form, and it changes nothing for text already in one
///   normalization form.
fn java_flag_group(rest: &[char]) -> Option<FlagGroup> {
    let mut on = String::new();
    let mut off = String::new();
    let mut changes = Vec::new();
    let mut clearing = false;
    for (n, &c) in rest.iter().enumerate() {
        match c {
            'm' | 's' => {
                if clearing {
                    off.push(c);
                } else {
                    on.push(c);
                }
                changes.push((c, !clearing));
            }
            'i' | 'u' | 'd' | 'x' | 'U' => changes.push((c, !clearing)),
            'c' => {}
            '-' if !clearing => clearing = true,
            ')' | ':' => {
                return Some(FlagGroup {
                    on,
                    off,
                    len: n + 1,
                    scoped: c == ':',
                    changes,
                });
            }
            _ => return None,
        }
    }
    None
}

/// A Java inline flag group, as [`java_flag_group`] reads it.
struct FlagGroup {
    /// The flags `fancy_regex` shares that it turns on.
    on: String,
    /// The flags `fancy_regex` shares that it turns off.
    off: String,
    /// The chars of the group after its `(?`, through the closing `)` or `:`.
    len: usize,
    /// Whether it ends in `:`, a group that the flags last to the end of.
    scoped: bool,
    /// The flags [`Scope`] follows that it sets (`true`) or clears (`false`),
    /// in the order Java applies them.
    changes: Vec<(char, bool)>,
}

impl FlagGroup {
    /// The `fancy_regex` text for the group. It is empty for a `(?flags)`
    /// group that has no flag of `fancy_regex`'s to write, which
    /// `fancy_regex` refuses.
    fn translation(&self) -> String {
        let (on, off) = (&self.on, &self.off);
        let end = if self.scoped { ':' } else { ')' };
        match (on.is_empty() && off.is_empty(), self.scoped) {
            (true, false) => String::new(),
            (true, true) => "(?:".to_string(),
            (false, _) if off.is_empty() => format!("(?{on}{end}"),
            (false, _) => format!("(?{on}-{off}{end}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn validate_one(key: &str, value: &str) -> Result<(), ConfigError> {
        validate(
            "sub",
            &maplit::btreemap! {key.to_string() => value.to_string()},
        )
    }

    fn check_client_id_patterns(cases: &[(&str, &str, bool)]) {
        for &(pattern, input, expected) in cases {
            let rules = parse_match_rules(&format!("client_id={pattern}")).unwrap();
            check!(
                rules[0].pattern.is_match(input).unwrap() == expected,
                "{pattern:?} on {input:?}"
            );
        }
    }

    #[test]
    fn validate_matches_client_metrics_configs() {
        let request = |m: &str| Err(ConfigError::InvalidRequest(m.to_string()));
        let config = |m: &str| Err(ConfigError::InvalidConfig(m.to_string()));
        let range = |n: &str| {
            request(&format!(
                "Invalid value {n} for interval.ms, interval must be between 100 and 3600000 (1 \
                 hour)"
            ))
        };
        let not_int = |v: &str| {
            config(&format!(
                "Invalid value {v} for configuration interval.ms: Not a number of type INT"
            ))
        };
        let illegal = |e: &str| config(&format!("Illegal client matching pattern: {e}"));
        let cases = [
            ("interval.ms", "300000", Ok(())),
            ("interval.ms", "100", Ok(())),
            ("interval.ms", "3600000", Ok(())),
            ("interval.ms", " 1000 ", Ok(())),
            ("interval.ms", "99", range("99")),
            ("interval.ms", "3600001", range("3600001")),
            ("interval.ms", "not-a-number", not_int("not-a-number")),
            ("interval.ms", "3000000000", not_int("3000000000")),
            (
                "bogus.key",
                "x",
                request("Unknown client metrics configuration: bogus.key"),
            ),
            ("metrics", "*", Ok(())),
            ("metrics", "", Ok(())),
            (
                "metrics",
                "org.apache.kafka.consumer., org.apache.kafka.producer.",
                Ok(()),
            ),
            (
                "metrics",
                "a.,",
                config("Configuration 'metrics' values must not be empty."),
            ),
            (
                "metrics",
                "a.,a.",
                config("Configuration 'metrics' values must not be duplicated."),
            ),
            ("match", "client_id=my-app.*", Ok(())),
            (
                "match",
                "client_software_name=apache-kafka-java,client_id=svc-.*",
                Ok(()),
            ),
            ("match", " client_id = a ", Ok(())),
            ("match", "client_id=(?i)app(?!-x)", Ok(())),
            // Kafka splits on every `=`, so a lookahead `(?=` can never pass.
            ("match", "client_id=a(?=b)", illegal("client_id=a(?=b)")),
            ("match", "client_id=(a)\\1", Ok(())),
            ("match", "client_id=(?i)(a)\\1", Ok(())),
            ("match", "client_id=(?iu)(a)\\1", Ok(())),
            ("match", "client_id=a==", Ok(())),
            ("match", "client_foo=x", illegal("client_foo=x")),
            ("match", "client_id", illegal("client_id")),
            ("match", "=x", illegal("=x")),
            ("match", "client_id=a=b", illegal("client_id=a=b")),
            (
                "match",
                "client_id=[unclosed",
                illegal("client_id=[unclosed"),
            ),
            ("match", "client_id=(?P<n>x)", illegal("client_id=(?P<n>x)")),
            ("match", "client_id=(?P=n)", illegal("client_id=(?P=n)")),
            ("match", "client_id=(?'n'x)", illegal("client_id=(?'n'x)")),
            ("match", "client_id=(?<1n>x)", illegal("client_id=(?<1n>x)")),
            ("match", "client_id=[(?P<n>]", Ok(())),
            ("match", "client_id=\\Q(?P<\\E", Ok(())),
            // A `)` that closes no group is Java's `Unmatched closing ')'`, and
            // a range that ends in a shorthand class is `Illegal character
            // range`; a `)` in a class or an escape is a character.
            ("match", "client_id=a)|(b", illegal("client_id=a)|(b")),
            ("match", "client_id=(a))", illegal("client_id=(a))")),
            ("match", "client_id=[a-\\w]", illegal("client_id=[a-\\w]")),
            ("match", "client_id=[)]", Ok(())),
            ("match", "client_id=\\)", Ok(())),
            ("match", "client_id=[\\w-.]", Ok(())),
            // A range that runs backwards is `Illegal character range`, under a
            // Unicode fold as under none, and a `&` alone is a class member.
            ("match", "client_id=[z-a]", illegal("client_id=[z-a]")),
            (
                "match",
                "client_id=(?iu)[z-a]",
                illegal("client_id=(?iu)[z-a]"),
            ),
            ("match", "client_id=[a&b]", Ok(())),
            ("match", "client_id=(?iu)[a&b]", Ok(())),
            // A `{` outside a class and an escape starts the bounds of a
            // quantifier, and any other is `Illegal repetition`.
            ("match", "client_id=a{2}", Ok(())),
            ("match", "client_id=[{]", Ok(())),
            ("match", "client_id=\\{", Ok(())),
            ("match", "client_id=\\Q{\\E", Ok(())),
            ("match", "client_id={", illegal("client_id={")),
            ("match", "client_id=a{x}", illegal("client_id=a{x}")),
            (
                "match",
                "client_id=(?iu)a{x}",
                illegal("client_id=(?iu)a{x}"),
            ),
        ];
        for (key, value, expected) in cases {
            check!(validate_one(key, value) == expected, "{key}={value:?}");
        }
    }

    #[test]
    fn empty_subscription_name_is_an_invalid_request() {
        check!(
            validate("", &BTreeMap::new())
                == Err(ConfigError::InvalidRequest(
                    "Subscription name can't be empty".into()
                ))
        );
    }

    #[test]
    fn unknown_key_is_reported_before_a_bad_value() {
        let configs = maplit::btreemap! {
            "interval.ms".to_string() => "abc".to_string(),
            "zzz".to_string() => "x".to_string(),
        };
        check!(
            validate("sub", &configs)
                == Err(ConfigError::InvalidRequest(
                    "Unknown client metrics configuration: zzz".into()
                ))
        );
    }

    #[test]
    fn error_codes_follow_the_exception() {
        check!(ConfigError::InvalidRequest(String::new()).code() == crate::codes::INVALID_REQUEST);
        check!(ConfigError::InvalidConfig(String::new()).code() == crate::codes::INVALID_CONFIG);
    }

    #[test]
    fn effective_interval_defaults_and_trims() {
        let mut m = BTreeMap::new();
        check!(effective_interval_ms(&m, INTERVAL_MS_DEFAULT) == 300_000);
        check!(effective_interval_ms(&m, 600_000) == 600_000);
        m.insert("interval.ms".to_string(), " 60000 ".to_string());
        check!(effective_interval_ms(&m, INTERVAL_MS_DEFAULT) == 60_000);
    }

    #[test]
    fn match_rules_fully_match_with_java_constructs() {
        let rules = parse_match_rules("client_id=(?!test).*, client_software_name=java").unwrap();
        let selectors: Vec<MatchSelector> = rules.iter().map(|r| r.selector).collect();
        check!(selectors == vec![MatchSelector::Id, MatchSelector::SoftwareName]);
        for (input, expected) in [("app", true), ("test-app", false), ("", true)] {
            check!(
                rules[0].pattern.is_match(input).unwrap() == expected,
                "{input}"
            );
        }
        assert!(!rules[1].pattern.is_match("java-1").unwrap());
        let quoted = parse_match_rules("client_id=\\Qa.b\\E").unwrap();
        for (input, expected) in [("a.b", true), ("axb", false)] {
            check!(
                quoted[0].pattern.is_match(input).unwrap() == expected,
                "{input}"
            );
        }
    }

    /// A selector pattern reads as `java.util.regex.Pattern` does by default:
    /// `\w`, `\d` and `\s` are ASCII, and `.` stops at every line terminator.
    /// The flags that change that are `(?U)`, `(?s)` and `(?d)`.
    #[test]
    fn match_patterns_read_ascii_classes_and_a_dot_that_stops_at_a_terminator() {
        // (pattern, input, whole input matches)
        let cases = [
            ("\\w+", "app_1", true),
            ("\\w+", "appé", false),
            ("(?U)\\w+", "appé", true),
            ("\\d+", "١٢٣", false),
            ("app\\s1", "app\u{a0}1", false),
            ("app.1", "app\r1", false),
            ("app.1", "app\u{2028}1", false),
            ("(?s)app.1", "app\r1", true),
            ("(?d)app.1", "app\r1", true),
            ("[.]", "\r", false),
            ("[a&b]", "&", true),
            ("[a&b]", "b", true),
            ("[a&b]", "c", false),
            ("(?iu)[a&b]", "&", true),
            ("a{2}", "aa", true),
            ("a{2}", "a", false),
        ];
        check_client_id_patterns(&cases);
    }

    /// `(?i)` in a selector folds ASCII case only, as `Pattern.CASE_INSENSITIVE`
    /// does. Long s (U+017F) and the Kelvin sign (U+212A) match `s` and `k`
    /// only under `(?u)`, which `Pattern.UNICODE_CASE` sets. Each row is
    /// `Matcher.matches` on JDK 17.
    #[test]
    fn match_patterns_fold_ascii_case_unless_unicode_case_is_on() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?i)app-", "APP-", true),
            ("(?i)service-.*", "SERVICE-1", true),
            ("(?i)service-.*", "\u{17f}ervice-1", false),
            ("(?iu)service-.*", "\u{17f}ervice-1", true),
            ("(?i)k", "\u{212a}", false),
            ("(?i)[a-c]+", "AbC", true),
            ("(?i)[^a]", "A", false),
            ("(?i)\\p{Lower}+", "aBc", true),
            ("(?i)(?<Id>app)-\\d", "APP-1", true),
            ("app(?i)-x", "app-X", true),
            ("app(?i)-x", "APP-X", false),
            // A quoted member takes part in a range, a `]` first in a class may
            // start one, `\p{Lower}` and `\w` stay ASCII under `(?iu)`, and a
            // backreference folds ASCII case.
            ("(?i)[\\Qa\\E-c]", "B", true),
            ("(?i)[\\Qa\\E-c]", "X", false),
            ("(?i)[]-c]", "A", true),
            ("(?i)[]-c]", "X", false),
            ("(?iu)\\p{Lower}", "\u{17f}", false),
            ("(?iu)\\p{Lower}", "A", true),
            ("(?iu)\\w", "\u{17f}", false),
            ("(?i)(a)\\1", "aA", true),
            ("(?i)(a)\\1", "ab", false),
            ("(?i)(\\d+)-\\1", "12-12", true),
            ("(?i)(\\d+)-\\1", "12-13", false),
        ];
        check_client_id_patterns(&cases);
    }

    /// `(?iu)` in a selector folds Unicode case as Java does, by
    /// `Character.toLowerCase` of `Character.toUpperCase`, in a class as well:
    /// `\w` and `\p{Lower}` in a class do not match long s or the Kelvin sign, a
    /// range that has `K` and not `k` does not match the Kelvin sign, and `İ`
    /// matches `i`. The POSIX classes are ASCII unless `(?U)` is on. Each row
    /// is `Matcher.matches` on JDK 17.
    #[test]
    fn match_patterns_fold_unicode_case_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?iu)[\\w]", "\u{17f}", false),
            ("(?iu)[\\w-]", "\u{212a}", false),
            ("(?iu)[\\w-]", "-", true),
            ("(?iu)[\\W]", "\u{17f}", true),
            ("(?iu)[\\p{Lower}]", "\u{17f}", false),
            ("(?iu)[\\p{Upper}]", "\u{212a}", false),
            ("(?iu)[a-z]", "\u{212a}", true),
            ("(?iu)[A-Z]", "\u{212a}", false),
            ("(?iu)[A-K]", "\u{212a}", false),
            ("(?iu)[A-c]", "\u{212a}", false),
            ("(?iu)[A-Z]", "\u{17f}", true),
            ("(?iu)[a-z]", "\u{130}", true),
            ("(?iu)[A-Z]", "\u{130}", false),
            ("(?iu)[a-z&&[^k]]", "\u{212a}", false),
            ("(?iu)i", "\u{130}", true),
            ("(?iu)[i]", "\u{131}", true),
            ("(?iu)service-.*", "\u{17f}ervice-1", true),
            ("(?iu)[\\x{391}-\\x{3a9}]", "\u{3c2}", true),
            ("\\p{Alpha}", "\u{e9}", false),
            ("\\p{Alnum}+", "a1", true),
            ("\\p{Punct}", "\u{a1}", false),
            ("[\\p{Digit}]", "\u{663}", false),
            ("(?U)\\p{Alpha}", "\u{e9}", true),
            ("(?U)\\p{Digit}", "\u{663}", true),
        ];
        check_client_id_patterns(&cases);
    }

    /// Java's inline flags, each case checked against `Pattern.compile` and
    /// `Matcher.matches` on JDK 21: `None` is a refusal, otherwise the
    /// inputs the compiled pattern must and must not fully match.
    #[test]
    fn java_inline_flags_mean_what_they_mean_in_java() {
        type Case = (&'static str, Option<&'static [(&'static str, bool)]>);
        let cases: [Case; 18] = [
            ("(?d)^foo$", Some(&[("foo", true), ("xfoo", false)])),
            ("(?i-d)foo", Some(&[("FOO", true)])),
            ("(?U)\\w+", Some(&[("é", true), ("a b", false)])),
            // `U` swapped greediness would make the atomic group take both.
            ("(?U)(?>a+?)a", Some(&[("aa", true)])),
            ("(?iu)é", Some(&[("É", true), ("e", false)])),
            ("(?-i)A", Some(&[("A", true), ("a", false)])),
            ("(?i)(?-i)A", Some(&[("A", true), ("a", false)])),
            ("(?i:abc)d", Some(&[("ABCd", true), ("ABCD", false)])),
            ("(?-:a)b", Some(&[("ab", true)])),
            ("(?)a", Some(&[("a", true)])),
            ("(?c)a", Some(&[("a", true)])),
            ("(?ix)A b", Some(&[("ab", true), ("a b", false)])),
            ("(?x)\\Q a#\\E", Some(&[(" a#", true), ("a", false)])),
            ("(?q)a", None),
            ("(?i--x)a", None),
            ("(?-i-x)a", None),
            ("(?i", None),
            ("a(?d)*", None),
        ];
        for (pattern, expected) in cases {
            let entry = format!("client_id={pattern}");
            match (parse_match_rules(&entry), expected) {
                (Ok(rules), Some(inputs)) => {
                    for &(input, matches) in inputs {
                        check!(
                            rules[0].pattern.is_match(input).unwrap() == matches,
                            "{pattern} on {input:?}"
                        );
                    }
                }
                (Err(error), None) => {
                    check!(
                        error
                            == ConfigError::InvalidConfig(format!(
                                "Illegal client matching pattern: {entry}"
                            ))
                    );
                }
                (result, _) => {
                    check!(result.is_ok() == expected.is_some(), "{pattern}");
                }
            }
        }
    }

    #[test]
    fn parse_metrics_splits_like_config_def() {
        check!(parse_metrics("*") == vec!["*".to_string()]);
        check!(parse_metrics("  ") == Vec::<String>::new());
        check!(parse_metrics(" a. , b. ") == vec!["a.".to_string(), "b.".to_string()]);
    }
}
