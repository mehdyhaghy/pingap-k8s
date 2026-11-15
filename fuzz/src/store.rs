//! An in-memory etcd holding Pingap entries. Plans are applied step by step, checking
//! every compare and that the config is valid for Pingap after each step.

use std::collections::{BTreeMap, BTreeSet};

use pingap_k8s::etcd::{entry_key, minimal_upstream};
use pingap_k8s::gateway::model::{Expect, Op, Plan, Stored, StoredEntry};
use pingap_k8s::gateway::plan::BUILTIN_PLUGINS;
use pingap_k8s::gateway::render;

pub const PREFIX: &str = "/pingap";

#[derive(Clone, Default)]
pub struct Store {
    revision: i64,
    kv: BTreeMap<String, (String, i64)>,
}

impl Store {
    pub fn put(&mut self, category: &str, name: &str, value: &str) {
        self.revision += 1;
        self.kv.insert(
            entry_key(PREFIX, category, name),
            (value.to_string(), self.revision),
        );
    }

    pub fn has(&self, category: &str, name: &str) -> bool {
        self.kv.contains_key(&entry_key(PREFIX, category, name))
    }

    /// What the endpoint writer does: create the upstreams it manages that are missing.
    pub fn create_upstreams<'a>(&mut self, names: impl IntoIterator<Item = &'a String>) {
        for name in names {
            if !self.has("upstreams", name) {
                let value = minimal_upstream(
                    name,
                    &["10.0.0.1:80".to_string()],
                    pingap_k8s::types::NewUpstream::default(),
                    None,
                );
                self.put("upstreams", name, &value);
            }
        }
    }

    pub fn stored(&self) -> Stored {
        let mut stored = Stored {
            revision: self.revision,
            ..Stored::default()
        };
        for (key, (value, rev)) in &self.kv {
            let rest = key
                .strip_prefix(PREFIX)
                .and_then(|r| r.strip_prefix('/'))
                .unwrap_or_else(|| panic!("key {key} outside {PREFIX}"));
            let (category, file) = rest
                .split_once('/')
                .unwrap_or_else(|| panic!("key {key} has no category"));
            let name = file
                .strip_suffix(".toml")
                .unwrap_or_else(|| panic!("key {key} is not a .toml entry"))
                .to_string();
            assert!(
                !name.contains('/'),
                "key {key}: the entry name has a path separator"
            );
            let entry = StoredEntry {
                key: key.clone(),
                value: value.clone(),
                mod_revision: *rev,
            };
            let map = match category {
                "servers" => &mut stored.servers,
                "locations" => &mut stored.locations,
                "upstreams" => &mut stored.upstreams,
                "certificates" => &mut stored.certificates,
                "plugins" => &mut stored.plugins,
                other => panic!("key {key}: unknown category {other}"),
            };
            map.insert(name, entry);
        }
        stored
    }

    pub fn apply(&mut self, plan: &Plan) {
        for step in &plan.steps {
            self.revision += 1;
            let mut touched = BTreeSet::new();
            for op in &step.ops {
                match op {
                    Op::RequireExists { key } => {
                        assert!(self.kv.contains_key(key), "{}: {key} missing", step.what)
                    }
                    Op::Put { key, expect, .. } | Op::Delete { key, expect } => {
                        assert!(
                            touched.insert(key.clone()),
                            "{}: {key} written twice in one step",
                            step.what
                        );
                        let current = self.kv.get(key).map(|(_, r)| *r);
                        match expect {
                            Expect::Absent => {
                                assert_eq!(current, None, "{}: {key} exists", step.what)
                            }
                            Expect::ModRevision(r) => {
                                assert_eq!(current, Some(*r), "{}: {key} revision", step.what)
                            }
                        }
                    }
                }
            }
            for op in &step.ops {
                match op {
                    Op::Put { key, value, .. } => {
                        self.kv.insert(key.clone(), (value.clone(), self.revision));
                    }
                    Op::Delete { key, .. } => {
                        self.kv.remove(key);
                    }
                    Op::RequireExists { .. } => {}
                }
            }
            self.validate(step.what);
        }
    }

    /// What Pingap checks on every reload: the names servers, locations and plugins refer
    /// to exist, and every entry parses.
    pub fn validate(&self, after: &str) {
        let stored = self.stored();
        for map in [
            &stored.servers,
            &stored.locations,
            &stored.upstreams,
            &stored.certificates,
            &stored.plugins,
        ] {
            for (name, entry) in map {
                assert!(
                    entry.value.parse::<toml::Table>().is_ok(),
                    "after {after}: {name} does not parse:\n{}",
                    entry.value
                );
            }
        }
        for (server, entry) in &stored.servers {
            for name in render::server_locations(&entry.value, server).unwrap() {
                assert!(
                    stored.locations.contains_key(&name),
                    "after {after}: server {server} lists missing location {name}"
                );
            }
        }
        let plugin_exists =
            |name: &str| stored.plugins.contains_key(name) || BUILTIN_PLUGINS.contains(&name);
        for (name, entry) in &stored.locations {
            if let Some(upstream) = render::location_upstream(&entry.value, name).unwrap() {
                assert!(
                    stored.upstreams.contains_key(&upstream),
                    "after {after}: location {name} names missing upstream {upstream}"
                );
            }
            for plugin in render::location_plugins(&entry.value, name) {
                assert!(
                    plugin_exists(&plugin),
                    "after {after}: location {name} names missing plugin {plugin}"
                );
            }
        }
        for (name, entry) in &stored.plugins {
            if let Some(upstream) = render::plugin_upstream(&entry.value, name).unwrap() {
                assert!(
                    stored.upstreams.contains_key(&upstream),
                    "after {after}: plugin {name} names missing upstream {upstream}"
                );
            }
        }
    }
}
