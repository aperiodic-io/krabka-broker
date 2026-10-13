//! What `#[derive(RefinedNewtype)]` accepts, refuses, parses and prints.

use {
    assert2::assert,
    krabka_macros::RefinedNewtype,
    krabka_units::{
        fmt::Human as _,
        prelude::{
            ByteSize, ByteSizeExt as _, Frequency, FrequencyExt as _, Time, TimeExt as _, bytes,
            kibibytes, mebibytes, millis, per_sec, secs,
        },
    },
    refined_type::rule::{GreaterI32, GreaterU32, MinMaxU32, MinMaxU64},
};

/// The rule's own error, a `u32` getter named `into_value`, and a free parse
/// function.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(MinMaxU32<0, 100>), parse_fn = parse_percent)]
struct Percent(u32);

/// A labelled `String` error, a renamed getter, and every optional impl.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterU32<0>),
    string_error,
    label = "miss limit",
    getter = get,
    default = 3,
    from_str,
    display
)]
struct MissLimit(u32);

/// A `String` error without a label.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterU32<0>), string_error, from_str)]
struct Attempts(u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(MinMaxU32<0, MAX>), string_error, default = 0, from_str,
          display, parse_fn = parse_bounded)]
struct Bounded<const MAX: u32>(u32);

#[test]
fn generic_rules_apply_to_construction_traits_and_the_free_parser() {
    assert!(Bounded::<7>::new(7) == Ok(Bounded(7)));
    assert!(Bounded::<7>::new(8).is_err());
    assert!(Bounded::<7>::default().into_value() == 0);
    assert!("5".parse::<Bounded<7>>() == Ok(Bounded(5)));
    assert!(Bounded::<7>(5).to_string() == "5");
    assert!(parse_bounded::<7>("7") == Ok(Bounded(7)));
    assert!(parse_bounded::<7>("8").is_err());
}

#[test]
fn new_accepts_what_the_rule_accepts_and_returns_it_unchanged() {
    assert!(Percent::new(100).ok().map(Percent::into_value) == Some(100));
    assert!(MissLimit::new(7).map(MissLimit::get) == Ok(7));
    assert!(Attempts::new(1).map(|attempts| attempts.0) == Ok(1));
}

#[test]
fn new_refuses_with_the_rules_error_its_label_or_neither() {
    let rule_error = MinMaxU32::<0, 100>::new(101).unwrap_err();
    let rule_text = GreaterU32::<0>::new(0).unwrap_err().to_string();

    assert!(Percent::new(101).unwrap_err().to_string() == rule_error.to_string());
    assert!(MissLimit::new(0) == Err(format!("miss limit: {rule_text}")));
    assert!(Attempts::new(0) == Err(rule_text));
}

#[test]
fn default_display_and_from_str() {
    assert!(MissLimit::default().get() == 3);
    assert!(MissLimit::default().to_string() == "3");
    assert!(format!("{:>3}", MissLimit::default()) == "  3");
    assert!("9".parse::<MissLimit>() == Ok(MissLimit(9)));
    assert!("9".parse::<Attempts>() == Ok(Attempts(9)));
}

#[test]
fn text_is_refused_by_the_integer_parse_then_by_the_rule() {
    let rule_text = GreaterU32::<0>::new(0).unwrap_err().to_string();
    let not_a_number = "x".parse::<u32>().unwrap_err().to_string();

    for (text, expected) in [
        ("50", Ok(Percent(50))),
        ("x", Err(not_a_number.clone())),
        (
            "101",
            Err(MinMaxU32::<0, 100>::new(101).unwrap_err().to_string()),
        ),
    ] {
        assert!(parse_percent(text) == expected);
    }
    assert!("x".parse::<MissLimit>() == Err(not_a_number));
    assert!("0".parse::<MissLimit>() == Err(format!("miss limit: {rule_text}")));
    assert!("0".parse::<Attempts>() == Err(rule_text));
}

/// A whole-byte quantity held as an `i32`, with every quantity impl.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterI32<0>),
    quantity = ByteSize,
    label = "fetch max",
    getter = bytes,
    quantity_getter = size,
    default = mebibytes(8),
    from_str,
    display
)]
struct FetchMax(i32);

/// A whole-millisecond quantity held as a `u64`, without a label.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(MinMaxU64<1, 60_000>), quantity = Time, quantity_getter = time, display)]
struct Timeout(u64);

/// A whole-Hz quantity held as an `i32`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterI32<0>), quantity = Frequency, label = "sample frequency", from_str)]
struct SampleFrequency(i32);

#[test]
fn quantity_new_takes_a_whole_number_of_the_unit() {
    assert!(FetchMax::new(kibibytes(4)) == Ok(FetchMax(4096)));
    assert!(FetchMax::try_from(bytes(1)) == Ok(FetchMax(1)));
    assert!(Timeout::new(secs(2)) == Ok(Timeout(2000)));
    assert!(SampleFrequency::new(per_sec(99)) == Ok(SampleFrequency(99)));
}

#[test]
fn quantity_new_refuses_fractions_infinities_overflow_then_the_rule() {
    let whole_bytes = "fetch max: must be a whole number of bytes that fits in i32".to_owned();
    let whole_millis = "must be a whole number of milliseconds that fits in u64".to_owned();
    let positive = GreaterI32::<0>::new(0).unwrap_err().to_string();

    for (size, expected) in [
        (ByteSize::from_bytes_f64(1.5), whole_bytes.clone()),
        (ByteSize::from_bytes_f64(f64::INFINITY), whole_bytes.clone()),
        (ByteSize::from_bytes_f64(f64::NAN), whole_bytes.clone()),
        (
            ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1),
            whole_bytes.clone(),
        ),
        (bytes(0), format!("fetch max: {positive}")),
    ] {
        assert!(FetchMax::new(size) == Err(expected), "{size:?}");
    }
    for (time, expected) in [
        (Time::from_micros(1500), whole_millis.clone()),
        (Time::from_millis(-1), whole_millis),
        (
            secs(61),
            MinMaxU64::<1, 60_000>::new(61_000).unwrap_err().to_string(),
        ),
    ] {
        assert!(Timeout::new(time) == Err(expected), "{time:?}");
    }
    assert!(
        SampleFrequency::new(Frequency::from_per_sec(1.5))
            == Err("sample frequency: must be a whole number of Hz that fits in i32".to_owned())
    );
}

#[test]
fn quantity_getters_default_display_and_from_str() {
    assert!(FetchMax::default().bytes() == 8 * 1024 * 1024);
    assert!(FetchMax::default().size() == mebibytes(8));
    assert!(FetchMax::default().to_string() == "8MiB");
    assert!(Timeout(1500).time() == millis(1500));
    assert!(Timeout(1500).to_string() == millis(1500).human().to_string());
    assert!("4KiB".parse::<FetchMax>() == Ok(FetchMax(4096)));
    assert!("101Hz".parse::<SampleFrequency>() == Ok(SampleFrequency(101)));
    assert!(
        "4.5B".parse::<FetchMax>()
            == Err("fetch max: must be a whole number of bytes that fits in i32".to_owned())
    );
    let unparsable = krabka_units::parse::frequency("fast")
        .unwrap_err()
        .to_string();
    assert!("fast".parse::<SampleFrequency>() == Err(unparsable));
}
