//! The controller config: the TOML file, `services` entries and the etcd URL.
//!
//! Checks: no panic; a parsed `services` entry written back parses to the same; the
//! etcd URL with its password redacted never yields the password again.
#![no_main]

use libfuzzer_sys::fuzz_target;
use pingap_k8s::config::{Config, parse_service_entry};
use pingap_k8s::etcd::{parse_etcd_url, redact_url};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };

    if let Ok(config) = Config::from_toml_str(text, "default") {
        let _ = parse_etcd_url(&config.etcd.url);
        for upstream in config.upstreams.values() {
            for service in &upstream.services {
                assert!(!service.key.name.is_empty() && !service.key.namespace.is_empty());
            }
        }
    }

    for line in text.lines() {
        if let Ok(entry) = parse_service_entry(line, "default") {
            let mut written = format!("{}.{}:{}", entry.key.name, entry.key.namespace, entry.port);
            if let Some(weight) = entry.weight {
                written.push_str(&format!(" {weight}"));
            }
            let again = parse_service_entry(&written, "other")
                .unwrap_or_else(|e| panic!("{line:?} -> {written:?}: {e}"));
            assert_eq!(again, entry, "{line:?} -> {written:?}");
        }

        let target = parse_etcd_url(line);
        let redacted = redact_url(line);
        if let Ok(target) = &target
            && let Some(password) = &target.password
            && let Ok(again) = parse_etcd_url(&redacted)
        {
            assert_ne!(
                again.password.as_ref(),
                Some(password),
                "{line:?}: the password is still in {redacted:?}"
            );
        }
    }
});
