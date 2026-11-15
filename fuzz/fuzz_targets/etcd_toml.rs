//! The edits of stored Pingap upstreams: `read_addrs`, `replace_addrs`,
//! `add_missing_alpn` and `minimal_upstream`.
//!
//! Input: a first line with the upstream name, a second with `|`-separated addresses
//! and the stored TOML after it. Checks: no panic; an edit reads back as written, a
//! repeated edit changes nothing, and nothing but the edited field changes.

use pingap_k8s::etcd::{add_missing_alpn, minimal_upstream, read_addrs, replace_addrs};
use pingap_k8s::types::{NewUpstream, UpstreamTls};

/// The document without `upstreams.<name>.<field>`, by value.
fn without(toml: &str, name: &str, field: &str) -> toml::Table {
    let mut doc: toml::Table = toml.parse().unwrap_or_else(|e| panic!("{e}:\n{toml}"));
    if let Some(entry) = doc
        .get_mut("upstreams")
        .and_then(|u| u.as_table_mut())
        .and_then(|u| u.get_mut(name))
        .and_then(|e| e.as_table_mut())
    {
        entry.remove(field);
    }
    doc
}

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let Ok(text) = std::str::from_utf8(data) else {
            return;
        };
        let mut parts = text.splitn(3, '\n');
        let (Some(name), Some(addrs), Some(toml)) = (parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let addrs: Vec<String> = if addrs.is_empty() {
            Vec::new()
        } else {
            addrs.split('|').map(str::to_string).collect()
        };
        let parses = toml.parse::<toml::Table>().is_ok();

        let read = read_addrs(toml, name);
        match replace_addrs(toml, name, &addrs) {
            Ok(Some(new)) => {
                assert_eq!(
                    read_addrs(&new, name).unwrap(),
                    Some(addrs.clone()),
                    "{new}"
                );
                assert!(
                    replace_addrs(&new, name, &addrs).unwrap().is_none(),
                    "{new}"
                );
                if parses {
                    assert_eq!(
                        without(toml, name, "addrs"),
                        without(&new, name, "addrs"),
                        "{new}"
                    );
                }
            }
            Ok(None) => assert_eq!(read.unwrap(), Some(addrs.clone())),
            Err(_) => {}
        }

        if let Ok(Some(new)) = add_missing_alpn(toml, name, "H2") {
            assert!(
                add_missing_alpn(&new, name, "H2").unwrap().is_none(),
                "{new}"
            );
            if parses {
                assert_eq!(
                    without(toml, name, "alpn"),
                    without(&new, name, "alpn"),
                    "{new}"
                );
            }
            let doc: toml::Table = new.parse().unwrap();
            assert_eq!(doc["upstreams"][name]["alpn"].as_str(), Some("H2"), "{new}");
        }

        let tls = UpstreamTls {
            sni: addrs.first().cloned().unwrap_or_default(),
            ca: Some(toml.to_string()),
            client_cert: None,
        };
        for (health_check, tls, algo, alpn) in [
            (None, None, None, None),
            (Some(toml), Some(&tls), Some(toml), Some("H2")),
        ] {
            let new = NewUpstream {
                health_check,
                tls,
                algo,
            };
            let minimal = minimal_upstream(name, &addrs, new, alpn);
            let doc: toml::Table = minimal
                .parse()
                .unwrap_or_else(|e| panic!("{e}:\n{minimal}"));
            assert_eq!(
                read_addrs(&minimal, name).unwrap(),
                Some(addrs.clone()),
                "{minimal}"
            );
            let entry = &doc["upstreams"][name];
            assert_eq!(
                entry.get("health_check").and_then(|v| v.as_str()),
                health_check
            );
            assert_eq!(
                entry.get("ca").and_then(|v| v.as_str()),
                tls.and_then(|t| t.ca.as_deref())
            );
        }
    });
}
