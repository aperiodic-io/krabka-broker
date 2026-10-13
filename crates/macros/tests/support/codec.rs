//! Fixed-width reads shared by the independent codec consumer fixtures.

pub(crate) type BrokerError = &'static str;

pub(crate) fn take<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N], BrokerError> {
    let (value, rest) = buf.split_first_chunk::<N>().ok_or("truncated field")?;
    *buf = rest;
    Ok(*value)
}
