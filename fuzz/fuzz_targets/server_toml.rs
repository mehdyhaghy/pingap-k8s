//! The reads and edits of stored Pingap entries by the Gateway provider:
//! `server_info`, `server_locations`, `replace_owned_locations` and the location,
//! plugin and certificate reads.
//!
//! Input: a first line with the entry name, a second with `|`-separated location names
//! to list and the stored TOML after it. Checks: no panic; the new locations list keeps
//! the hand-made names in order and then lists the owned ones asked for; a repeated
//! edit changes nothing; nothing but `locations` changes.

use pingap_k8s::gateway::model::is_owned;
use pingap_k8s::gateway::render::{
    certificate_remark, location_owner, location_plugins, location_upstream, plugin_upstream,
    replace_owned_locations, same_entry, server_info, server_locations,
};

/// The document without `servers.<name>.locations`, by value.
fn without_locations(toml: &str, name: &str) -> toml::Table {
    let mut doc: toml::Table = toml.parse().unwrap_or_else(|e| panic!("{e}:\n{toml}"));
    if let Some(entry) = doc
        .get_mut("servers")
        .and_then(|s| s.as_table_mut())
        .and_then(|s| s.get_mut(name))
        .and_then(|e| e.as_table_mut())
    {
        entry.remove("locations");
    }
    doc
}

fn main() {
    afl::fuzz!(|data: &[u8]| {
        let Ok(text) = std::str::from_utf8(data) else {
            return;
        };
        let mut parts = text.splitn(3, '\n');
        let (Some(name), Some(list), Some(toml)) = (parts.next(), parts.next(), parts.next())
        else {
            return;
        };
        let desired: Vec<String> = if list.is_empty() {
            Vec::new()
        } else {
            list.split('|').map(str::to_string).collect()
        };

        if let Some(info) = server_info(toml, name) {
            assert_eq!(info.name, name);
            assert!(!info.ports.is_empty() && !info.ports.contains(&0));
        }
        let _ = certificate_remark(toml, name);
        let _ = location_upstream(toml, name);
        let _ = location_plugins(toml, name);
        let _ = location_owner(toml, name);
        let _ = plugin_upstream(toml, name);
        assert_eq!(same_entry(toml, toml), toml.parse::<toml::Table>().is_ok());

        let Ok(current) = server_locations(toml, name) else {
            return;
        };
        let mut wanted: Vec<String> = current.iter().filter(|n| !is_owned(n)).cloned().collect();
        let kept = wanted.len();
        for n in &desired {
            if is_owned(n) && !wanted[kept..].contains(n) {
                wanted.push(n.clone());
            }
        }
        match replace_owned_locations(toml, name, &desired) {
            Ok(Some(new)) => {
                assert_ne!(wanted, current);
                assert_eq!(server_locations(&new, name).unwrap(), wanted, "{new}");
                assert!(
                    replace_owned_locations(&new, name, &desired)
                        .unwrap()
                        .is_none(),
                    "{new}"
                );
                assert_eq!(
                    without_locations(toml, name),
                    without_locations(&new, name),
                    "{new}"
                );
            }
            Ok(None) => assert_eq!(wanted, current),
            Err(e) => panic!("server_locations read {current:?} but the edit failed: {e}"),
        }
    });
}
