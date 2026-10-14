use assert2::assert;

#[krabka_macros::test_scalar]
struct Count(i64);

#[test]
fn derives_shared_scalar_traits() {
    let count = Count::from(7_i64);
    let copied = count;
    let inner: i64 = copied.into();

    assert!(count == copied);
    assert!(Count::default() == Count(0));
    assert!(format!("{count}") == "7");
    assert!(inner == 7);
}
