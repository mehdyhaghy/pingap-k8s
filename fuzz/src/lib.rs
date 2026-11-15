//! Shared parts of the fuzz targets: a generator of Gateway API worlds and an in-memory
//! etcd that checks the stored Pingap config after every plan step.

pub mod store;
pub mod world;

use arbitrary::{Result, Unstructured};

/// One of `pool`, or now and then an arbitrary string.
pub fn text(u: &mut Unstructured<'_>, pool: &[&str]) -> Result<String> {
    if rarely(u, 12)? {
        // Short, so one string does not use up the rest of the input.
        let len = u.int_in_range(0..=16)?;
        if let Ok(bytes) = u.bytes(len) {
            return Ok(String::from_utf8_lossy(bytes).into_owned());
        }
    }
    Ok(u.choose(pool)?.to_string())
}

/// True once in `n` times, and false when the input is used up (so a short input
/// gives the usual case, not the rare one).
pub fn rarely(u: &mut Unstructured<'_>, n: u32) -> Result<bool> {
    Ok(!u.ratio(n - 1, n)?)
}

/// One of `pool`.
pub fn pick<T: Clone>(u: &mut Unstructured<'_>, pool: &[T]) -> Result<T> {
    Ok(u.choose(pool)?.clone())
}

/// `Some` about half of the time.
pub fn maybe<T>(
    u: &mut Unstructured<'_>,
    f: impl FnOnce(&mut Unstructured<'_>) -> Result<T>,
) -> Result<Option<T>> {
    if u.arbitrary()? {
        Ok(Some(f(u)?))
    } else {
        Ok(None)
    }
}

/// Up to `max` items.
pub fn list<T>(
    u: &mut Unstructured<'_>,
    max: usize,
    mut f: impl FnMut(&mut Unstructured<'_>) -> Result<T>,
) -> Result<Vec<T>> {
    let n = u.int_in_range(0..=max)?;
    (0..n).map(|_| f(u)).collect()
}

/// One to `max` items.
pub fn some<T>(
    u: &mut Unstructured<'_>,
    max: usize,
    mut f: impl FnMut(&mut Unstructured<'_>) -> Result<T>,
) -> Result<Vec<T>> {
    let n = u.int_in_range(1..=max)?;
    (0..n).map(|_| f(u)).collect()
}

/// Pingap's rule for entry names (no path separator or NUL), as `etcd::check_name`.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty() && !name.contains(['/', '\\', '\0'])
}
