//! Pingap upstream `health_check` values, checked the way Pingap 0.16.1 parses them.
//!
//! Pingap validates only that `health_check` is a URL when it loads a config, and parses
//! the rest when it builds the upstream. A value it then rejects fails the whole upstream
//! reload, so every other upstream stops getting updates too. The controller therefore
//! never writes a value that does not pass [`validate`].
//!
//! The format: `<scheme>://<host><path>?<params>` with scheme `tcp`, `http`, `https`,
//! `grpc`, `ws` or `wss`. The host is sent as the `Host` header of HTTP checks (the
//! address comes from `addrs`). Params: `connection_timeout`, `read_timeout`,
//! `check_frequency` (durations above zero, such as `1s` or `500ms`), `success`,
//! `failure` (counts of at least 1), `reuse`, `tls`, `parallel`, `service` (gRPC), and for
//! HTTP checks only `expect_status` (`200`, `200-299`, `200,204`), `check_port` and
//! `expect_body`. Other params are sent as the query of the check request.

use url::Url;

/// Longest value accepted (an annotation or config value, not a request).
pub const MAX_LEN: usize = 1024;

const SCHEMES: &[&str] = &["tcp", "http", "https", "grpc", "ws", "wss"];

/// `Ok(())` when Pingap 0.16.1 builds a health check from `value`; otherwise why not.
pub fn validate(value: &str) -> Result<(), String> {
    if value.len() > MAX_LEN {
        return Err(format!("longer than {MAX_LEN} characters"));
    }
    if value.chars().any(|c| c.is_control()) {
        return Err("contains a control character".to_string());
    }
    // A service name or an address. Also keeps `url` from handing such a host to the
    // ASCII-only `idna` backend, which asserts (in debug builds) on some of them.
    if authority(value).contains(|c: char| c == '%' || !c.is_ascii()) {
        return Err("the host must be ASCII, without percent-encoding".to_string());
    }
    let url = Url::parse(value).map_err(|e| format!("not a URL: {e}"))?;
    if !SCHEMES.contains(&url.scheme()) {
        return Err(format!(
            "scheme {:?} is not one of {}",
            url.scheme(),
            SCHEMES.join(", ")
        ));
    }
    let http = matches!(url.scheme(), "http" | "https");
    for (key, value) in url.query_pairs() {
        let invalid = |reason: &str| Err(format!("{key}={value:?} {reason}"));
        match key.as_ref() {
            "connection_timeout" | "read_timeout" | "check_frequency" => {
                match humantime::parse_duration(&value) {
                    Ok(d) if d.is_zero() => return invalid("must be greater than zero"),
                    Ok(_) => {}
                    Err(e) => return invalid(&format!("is not a duration: {e}")),
                }
            }
            "success" | "failure" => match value.parse::<usize>() {
                Ok(0) => return invalid("must be at least 1"),
                Ok(_) => {}
                Err(_) => return invalid("is not a count"),
            },
            "expect_status" | "check_port" | "expect_body" if !http => {
                return Err(format!("{key} is for http and https checks only"));
            }
            "expect_status" => expect_status(&value).map_err(|e| format!("{key}: {e}"))?,
            "check_port" => {
                if !value.parse::<u16>().is_ok_and(|p| p > 0) {
                    return invalid("is not a port");
                }
            }
            "expect_body" if value.is_empty() => return invalid("is empty"),
            _ => {}
        }
    }
    Ok(())
}

/// The authority (user info, host and port) the way `url` finds it: after `<scheme>:` and
/// any slashes, up to the path, query or fragment.
fn authority(value: &str) -> &str {
    let rest = value.split_once(':').map_or(value, |(_, rest)| rest);
    let rest = rest.trim_start_matches(['/', '\\']);
    rest.split(['/', '\\', '?', '#']).next().unwrap_or("")
}

/// `200`, `200-299`, `200,204-206`: statuses 100 to 599, ranges not reversed.
fn expect_status(value: &str) -> Result<(), String> {
    let status = |text: &str| {
        text.trim()
            .parse::<u16>()
            .ok()
            .filter(|s| (100..600).contains(s))
            .ok_or_else(|| format!("{text:?} is not a status"))
    };
    for part in value.split(',') {
        let (from, to) = match part.split_once('-') {
            Some((from, to)) => (status(from)?, status(to)?),
            None => {
                let single = status(part)?;
                (single, single)
            }
        };
        if from > to {
            return Err(format!("{part:?} ends before it begins"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_names_are_ascii() {
        // `idna` is built without Unicode tables (deny.toml, Cargo.toml).
        assert!(validate("http://xn--bcher-kva.example/healthz").is_ok());
        // Percent-encoding and non-ASCII stay allowed in the path and the query.
        assert!(validate("http://api/h%20z/\u{fc}?from=b%C3%BC").is_ok());
        for value in [
            "http://b\u{fc}cher.example/healthz",
            "http://b%C3%BCcher.example/healthz",
            // Found by fuzzing: `idna`'s ASCII-only backend asserted on these.
            "http://0N%9ftnm",
            "http:K\u{455}.%8ckyi",
            "http:/XN--Acers=_____B____0O\u{c6}ozkhhhhhhhhhhmnm",
            "http:/@0N%deqsm",
            "https:\\\\A%41/x",
        ] {
            let err = validate(value).unwrap_err();
            assert!(err.contains("must be ASCII"), "{value}: {err}");
        }
    }

    #[test]
    fn accepted() {
        for value in [
            "tcp://api",
            "tcp://api?connection_timeout=1s&check_frequency=5s&success=2&failure=3",
            "http://api/healthz",
            "http://api/healthz?connection_timeout=500ms&read_timeout=1s&reuse&parallel",
            "https://api/ready?expect_status=200-299,301&check_port=9090&expect_body=ok",
            "http://api/healthz?from=pingap",
            "grpc://api?service=grpc.health.v1.Health&tls",
            "ws://api/socket",
            "wss://api/socket?connection_timeout=2s",
            // Pingap's own example.
            "https://upstreamname/ping?connection_timeout=3s&read_timeout=1s&success=2&failure=1&check_frequency=10s&from=nginx&reuse",
        ] {
            assert_eq!(validate(value), Ok(()), "{value}");
        }
    }

    #[test]
    fn rejected() {
        for (value, why) in [
            ("api/healthz", "not a URL"),
            ("", "not a URL"),
            ("http:///", "empty host"),
            ("udp://api", "scheme"),
            ("ftp://api", "scheme"),
            ("http://api/?check_frequency=5", "not a duration"),
            ("http://api/?connection_timeout=0s", "greater than zero"),
            ("http://api/?read_timeout=soon", "not a duration"),
            ("http://api/?failure=three", "not a count"),
            ("http://api/?success=0", "at least 1"),
            ("http://api/?success=-1", "not a count"),
            ("http://api/?expect_status=600", "not a status"),
            ("http://api/?expect_status=299-200", "ends before"),
            ("http://api/?expect_status=", "not a status"),
            ("http://api/?check_port=0", "not a port"),
            ("http://api/?check_port=70000", "not a port"),
            ("http://api/?expect_body=", "is empty"),
            ("tcp://api?expect_status=200", "http and https checks only"),
            ("grpc://api?check_port=81", "http and https checks only"),
            ("ws://api?expect_body=ok", "http and https checks only"),
            ("http://api/\n", "control character"),
        ] {
            let err = validate(value).unwrap_err();
            assert!(err.contains(why), "{value}: {err}");
        }
        assert!(validate(&format!("http://api/{}", "a".repeat(MAX_LEN))).is_err());
    }
}
