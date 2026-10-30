//! The ordered etcd steps that turn the stored Pingap entries into the desired ones.
//!
//! Pingap validates its whole config on every etcd event and rejects the reload when a
//! location names a missing upstream or plugin, or a server lists a missing location. The
//! steps are ordered so the stored config is valid after each of them:
//!
//! 1. put new or changed `ExternalName` upstreams, certificates and generated plugins;
//!    delete stale owned certificates (nothing refers to a certificate by name), except
//!    those of Secrets a listener still names but that are missing or invalid (fail
//!    static: a bad Secret update does not take TLS away from its hosts);
//! 2. put new or changed locations. A location whose upstream is not stored yet is
//!    deferred: a stored version keeps serving through its old upstream until the new one
//!    is stored, a new location is written without it (Pingap answers 503, as the Gateway
//!    API asks for a backend without endpoints). One that names a missing hand-made
//!    plugin is deferred too (a stored version is kept, a new one waits);
//! 3. set the owned part of every stored server's `locations`: each server lists its live
//!    generated locations, highest weight first; other servers lose every owned name;
//! 4. delete stale owned locations;
//! 5. delete stale owned plugins that no remaining location names;
//! 6. delete stale owned upstreams that no remaining location or plugin names.
//!
//! Every put and delete compares against the revision it was read at (new keys must be
//! absent), so a concurrent change makes the apply fail and the caller plans again.

use std::collections::{BTreeMap, BTreeSet};

use tracing::warn;

use crate::etcd::{SERVICE_UPDATE_FREQUENCY, check_name, entry_key};
use crate::gateway::model::{
    Deferred, Desired, Expect, Op, Plan, Step, Stored, StoredEntry, is_owned,
};
use crate::gateway::{names, render};

/// Plugins Pingap provides without a stored entry.
pub const BUILTIN_PLUGINS: &[&str] = &[
    "pingap:compression",
    "pingap:compressionUpstream",
    "pingap:ping",
    "pingap:stats",
    "pingap:requestId",
    "pingap:acceptEncodingAdjustment",
];

pub struct PlanInput<'a> {
    /// `"/" + <url path>` of the etcd URL, for the keys of new entries.
    pub prefix: &'a str,
    pub desired: &'a Desired,
    pub stored: &'a Stored,
    /// Owned upstreams that must not be deleted (those the endpoint writer manages).
    pub keep_upstreams: &'a BTreeSet<String>,
}

/// Collects the ops of one step; RequireExists compares are kept once per key.
#[derive(Default)]
struct StepOps {
    ops: Vec<Op>,
    required: BTreeSet<String>,
}

impl StepOps {
    fn push(&mut self, op: Op) {
        self.ops.push(op);
    }

    fn require(&mut self, key: String) {
        if self.required.insert(key.clone()) {
            self.ops.push(Op::RequireExists { key });
        }
    }

    fn finish(self, what: &'static str, steps: &mut Vec<Step>) {
        if !self.ops.is_empty() {
            steps.push(Step {
                what,
                ops: self.ops,
            });
        }
    }
}

struct Keys<'a> {
    prefix: &'a str,
}

impl Keys<'_> {
    fn of(
        &self,
        stored: &BTreeMap<String, StoredEntry>,
        category: &'static str,
        name: &str,
    ) -> String {
        stored
            .get(name)
            .map(|e| e.key.clone())
            .unwrap_or_else(|| entry_key(self.prefix, category, name))
    }
}

/// A put of `value` over the stored entry (compare its revision) or a new key.
fn put(
    keys: &Keys<'_>,
    stored: &BTreeMap<String, StoredEntry>,
    category: &'static str,
    name: &str,
    value: String,
) -> Op {
    match stored.get(name) {
        Some(entry) => Op::Put {
            key: entry.key.clone(),
            value,
            expect: Expect::ModRevision(entry.mod_revision),
        },
        None => Op::Put {
            key: keys.of(stored, category, name),
            value,
            expect: Expect::Absent,
        },
    }
}

fn delete(entry: &StoredEntry) -> Op {
    Op::Delete {
        key: entry.key.clone(),
        expect: Expect::ModRevision(entry.mod_revision),
    }
}

pub fn plan(input: &PlanInput<'_>) -> Plan {
    let PlanInput {
        prefix,
        desired,
        stored,
        keep_upstreams,
    } = *input;
    let keys = Keys { prefix };
    let mut out = Plan::default();

    // 1. External upstreams, certificates and generated plugins.
    let mut step = StepOps::default();
    let mut written_upstreams = BTreeSet::new();
    for (name, external) in &desired.external {
        if check_name(name).is_err() {
            warn!(upstream = name, "skipping an upstream with an invalid name");
            continue;
        }
        let rendered =
            match render::render_external_upstream(name, external, SERVICE_UPDATE_FREQUENCY) {
                Ok(rendered) => rendered,
                Err(e) => {
                    warn!(error = %e, "not writing ExternalName upstream");
                    continue;
                }
            };
        if let Some(entry) = stored.upstreams.get(name)
            && render::same_entry(&entry.value, &rendered)
        {
            continue;
        }
        step.push(put(&keys, &stored.upstreams, "upstreams", name, rendered));
        written_upstreams.insert(name.as_str());
    }
    for (name, certificate) in &desired.certificates {
        if check_name(name).is_err() {
            warn!(
                certificate = name,
                "skipping a certificate with an invalid name"
            );
            continue;
        }
        let rendered = render::render_certificate(name, certificate);
        if let Some(entry) = stored.certificates.get(name)
            && render::same_entry(&entry.value, &rendered)
        {
            continue;
        }
        step.push(put(
            &keys,
            &stored.certificates,
            "certificates",
            name,
            rendered,
        ));
    }
    let mut written_plugins = BTreeSet::new();
    for (name, plugin) in &desired.plugins {
        if !is_owned(name) || check_name(name).is_err() {
            warn!(plugin = name, "skipping a plugin with an invalid name");
            continue;
        }
        written_plugins.insert(name.as_str());
        let rendered = render::render_plugin(name, plugin);
        if let Some(entry) = stored.plugins.get(name)
            && render::same_entry(&entry.value, &rendered)
        {
            continue;
        }
        step.push(put(&keys, &stored.plugins, "plugins", name, rendered));
    }
    // Secrets still named by a listener keep their stored certificate, also while they
    // are missing or invalid.
    let named: BTreeSet<String> = desired
        .referenced
        .secrets
        .iter()
        .map(names::certificate_name)
        .collect();
    for (name, entry) in &stored.certificates {
        if is_owned(name) && !desired.certificates.contains_key(name) && !named.contains(name) {
            step.push(delete(entry));
        }
    }
    step.finish("upstreams, certificates and plugins", &mut out.steps);

    // 2. Locations.
    let upstream_exists =
        |u: &str| stored.upstreams.contains_key(u) || written_upstreams.contains(u);
    let plugin_exists = |p: &str| {
        BUILTIN_PLUGINS.contains(&p)
            || stored.plugins.contains_key(p)
            || written_plugins.contains(p)
    };
    // Owned locations stored or written after this plan, with the upstream and plugins
    // they name.
    let mut live: BTreeMap<&str, (Option<String>, Vec<String>)> = BTreeMap::new();
    // Deferred locations whose stored version stays as it is.
    let mut kept_stored: BTreeSet<&str> = BTreeSet::new();
    let mut step = StepOps::default();
    for (name, location) in &desired.locations {
        if !is_owned(name) || check_name(name).is_err() {
            warn!(location = name, "skipping a location with an invalid name");
            continue;
        }
        if let Some(plugin) = location.plugins.iter().find(|p| !plugin_exists(p)) {
            out.deferred
                .insert(name.clone(), Deferred::MissingPlugin(plugin.clone()));
            if let Some(entry) = stored.locations.get(name) {
                live.insert(name, stored_refs(&entry.value, name));
                kept_stored.insert(name.as_str());
            }
            continue;
        }
        let mut location = location.clone();
        if let Some(upstream) = &location.upstream
            && !upstream_exists(upstream)
        {
            out.deferred
                .insert(name.clone(), Deferred::MissingUpstream(upstream.clone()));
            if let Some(entry) = stored.locations.get(name) {
                live.insert(name, stored_refs(&entry.value, name));
                kept_stored.insert(name.as_str());
                continue;
            }
            location.upstream = None;
        }
        let rendered = render::render_location(name, &location);
        live.insert(name, (location.upstream.clone(), location.plugins.clone()));
        if let Some(entry) = stored.locations.get(name)
            && render::same_entry(&entry.value, &rendered)
        {
            continue;
        }
        step.push(put(&keys, &stored.locations, "locations", name, rendered));
        if let Some(upstream) = &location.upstream
            && !written_upstreams.contains(upstream.as_str())
        {
            step.require(keys.of(&stored.upstreams, "upstreams", upstream));
        }
        for plugin in &location.plugins {
            if stored.plugins.contains_key(plugin) && !written_plugins.contains(plugin.as_str()) {
                step.require(keys.of(&stored.plugins, "plugins", plugin));
            }
        }
    }
    step.finish("locations", &mut out.steps);

    // 3. Server location lists.
    let mut step = StepOps::default();
    // Owned names listed by some server once the plan is applied; `None` when a server
    // could not be read.
    let mut listed: Option<BTreeSet<String>> = Some(BTreeSet::new());
    for (server, entry) in &stored.servers {
        let current = match render::server_locations(&entry.value, server) {
            Ok(current) => current,
            Err(e) => {
                warn!(server, error = %e, "can not read the locations of a server");
                listed = None;
                continue;
            }
        };
        let wanted: Vec<String> = desired
            .server_locations
            .get(server)
            .map(|names| {
                names
                    .iter()
                    .filter(|n| live.contains_key(n.as_str()))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        match render::replace_owned_locations(&entry.value, server, &wanted) {
            Ok(Some(updated)) => {
                for name in wanted.iter().filter(|n| !current.contains(n)) {
                    step.require(keys.of(&stored.locations, "locations", name));
                }
                step.push(Op::Put {
                    key: entry.key.clone(),
                    value: updated,
                    expect: Expect::ModRevision(entry.mod_revision),
                });
            }
            Ok(None) => {}
            Err(e) => {
                warn!(server, error = %e, "can not update the locations of a server");
                listed = None;
                continue;
            }
        }
        if let Some(listed) = &mut listed {
            listed.extend(wanted);
        }
    }
    step.finish("servers", &mut out.steps);
    out.programmed = listed.clone().unwrap_or_default();

    // 4. Stale owned locations. Nothing is deleted while a server could not be read: it
    // may still list them.
    let mut step = StepOps::default();
    let mut deleted_locations = BTreeSet::new();
    if let Some(listed) = &listed {
        for (name, entry) in &stored.locations {
            if is_owned(name) && !live.contains_key(name.as_str()) && !listed.contains(name) {
                step.push(delete(entry));
                deleted_locations.insert(name.as_str());
            }
        }
    }
    step.finish("stale locations", &mut out.steps);

    // What the remaining locations name: live ones as written (a deferred one by its kept
    // stored version too), others as stored.
    let mut referenced_upstreams: BTreeSet<String> = BTreeSet::new();
    let mut referenced_plugins: BTreeSet<String> = BTreeSet::new();
    let mut readable = true;
    for (name, entry) in &stored.locations {
        if deleted_locations.contains(name.as_str()) {
            continue;
        }
        if live.contains_key(name.as_str()) && !kept_stored.contains(name.as_str()) {
            continue;
        }
        match toml::from_str::<toml::Table>(&entry.value) {
            Ok(_) => {
                let (upstream, plugins) = stored_refs(&entry.value, name);
                referenced_upstreams.extend(upstream);
                referenced_plugins.extend(plugins);
            }
            Err(_) => readable = false,
        }
    }
    for (upstream, plugins) in live.values() {
        referenced_upstreams.extend(upstream.iter().cloned());
        referenced_plugins.extend(plugins.iter().cloned());
    }
    // Deferred locations will name theirs once they can be written.
    for location in desired.locations.values() {
        referenced_upstreams.extend(location.upstream.iter().cloned());
        referenced_plugins.extend(location.plugins.iter().cloned());
    }

    // 5. Stale owned plugins.
    let mut step = StepOps::default();
    if readable {
        for (name, entry) in &stored.plugins {
            if is_owned(name)
                && !desired.plugins.contains_key(name)
                && !referenced_plugins.contains(name)
            {
                step.push(delete(entry));
            }
        }
    }
    step.finish("stale plugins", &mut out.steps);

    // 6. Stale owned upstreams, unless a remaining location or a plugin names them.
    for (name, entry) in &stored.plugins {
        match render::plugin_upstream(&entry.value, name) {
            Ok(Some(upstream)) => {
                referenced_upstreams.insert(upstream);
            }
            Ok(None) => {}
            Err(_) => readable = false,
        }
    }
    let mut step = StepOps::default();
    if readable {
        for (name, entry) in &stored.upstreams {
            if is_owned(name)
                && !desired.external.contains_key(name)
                && !desired.upstreams.contains_key(name)
                && !keep_upstreams.contains(name)
                && !referenced_upstreams.contains(name)
            {
                step.push(delete(entry));
            }
        }
    } else {
        warn!("a stored location or plugin can not be parsed; not deleting any upstream or plugin");
    }
    step.finish("stale upstreams", &mut out.steps);
    out
}

/// The upstream and plugins a stored location names (empty when it does not parse).
fn stored_refs(toml: &str, name: &str) -> (Option<String>, Vec<String>) {
    let upstream = render::location_upstream(toml, name).ok().flatten();
    let plugins = render::location_plugins(toml, name);
    (upstream, plugins)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::model::{
        CertificateEntry, ExternalUpstream, LocationEntry, PluginEntry, RouteKey, SecretKey,
    };
    use crate::gateway::render::{render_location, server_locations};
    use crate::types::ServiceKey;

    const PREFIX: &str = "/pingap";

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn route(name: &str) -> RouteKey {
        RouteKey::new("shop", name)
    }

    fn loc(owner: &str, upstream: &str) -> LocationEntry {
        LocationEntry {
            owner: route(owner),
            owner_kind: Default::default(),
            server: "http".into(),
            host: Some("shop.example.com".into()),
            path: String::new(),
            match_headers: vec![],
            match_query: vec![],
            upstream: Some(upstream.into()),
            weight: 60000,
            rewrite: None,
            proxy_set_headers: vec![],
            plugins: vec![],
            options: Default::default(),
        }
    }

    fn mock_plugin() -> PluginEntry {
        let mut fields = toml::Table::new();
        fields.insert("status".into(), toml::Value::Integer(500));
        PluginEntry {
            category: "mock".into(),
            fields,
        }
    }

    /// An in-memory etcd: keys -> (value, mod_revision), with a revision counter.
    #[derive(Clone)]
    struct Fake {
        revision: i64,
        kv: BTreeMap<String, (String, i64)>,
    }

    impl Fake {
        fn new() -> Self {
            Fake {
                revision: 1,
                kv: BTreeMap::new(),
            }
        }

        fn put(&mut self, category: &str, name: &str, value: &str) {
            self.revision += 1;
            self.kv.insert(
                entry_key(PREFIX, category, name),
                (value.to_string(), self.revision),
            );
        }

        fn upstream(&mut self, name: &str) {
            self.put(
                "upstreams",
                name,
                &format!("[upstreams.{name}]\naddrs = [\"10.0.0.1:80\"]\n"),
            );
        }

        fn server(&mut self, name: &str, locations: &[&str]) {
            let list: Vec<String> = locations.iter().map(|l| format!("{l:?}")).collect();
            self.put(
                "servers",
                name,
                &format!(
                    "[servers.{name}]\naddr = \"0.0.0.0:80\"\nlocations = [{}]\n",
                    list.join(", ")
                ),
            );
        }

        fn get(&self, category: &str, name: &str) -> Option<&String> {
            self.kv
                .get(&entry_key(PREFIX, category, name))
                .map(|(v, _)| v)
        }

        fn has(&self, category: &str, name: &str) -> bool {
            self.get(category, name).is_some()
        }

        fn stored(&self) -> Stored {
            let mut stored = Stored {
                revision: self.revision,
                ..Stored::default()
            };
            for (key, (value, rev)) in &self.kv {
                let rest = key.strip_prefix("/pingap/").unwrap();
                let (category, file) = rest.split_once('/').unwrap();
                let name = file.strip_suffix(".toml").unwrap().to_string();
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
                    other => panic!("{other}"),
                };
                map.insert(name, entry);
            }
            stored
        }

        /// Applies a plan step by step, checking every compare and that the config is
        /// valid for Pingap after each step.
        fn apply(&mut self, plan: &Plan) {
            for step in &plan.steps {
                self.revision += 1;
                for op in &step.ops {
                    match op {
                        Op::RequireExists { key } => {
                            assert!(self.kv.contains_key(key), "{}: {key} missing", step.what)
                        }
                        Op::Put { key, expect, .. } | Op::Delete { key, expect } => {
                            let current = self.kv.get(key).map(|(_, r)| *r);
                            match expect {
                                Expect::Absent => assert_eq!(current, None, "{key}"),
                                Expect::ModRevision(r) => assert_eq!(current, Some(*r), "{key}"),
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

        fn validate(&self, after: &str) {
            let stored = self.stored();
            for (server, entry) in &stored.servers {
                for name in server_locations(&entry.value, server).unwrap() {
                    assert!(
                        stored.locations.contains_key(&name),
                        "after {after}: server {server} lists missing location {name}"
                    );
                }
            }
            for (name, entry) in &stored.locations {
                if let Some(upstream) = render::location_upstream(&entry.value, name).unwrap() {
                    assert!(
                        stored.upstreams.contains_key(&upstream),
                        "after {after}: location {name} names missing upstream {upstream}"
                    );
                }
            }
        }
    }

    struct Case {
        desired: Desired,
        keep: BTreeSet<String>,
    }

    impl Case {
        fn new() -> Self {
            Case {
                desired: Desired::default(),
                keep: BTreeSet::new(),
            }
        }

        fn location(&mut self, name: &str, entry: LocationEntry) -> &mut Self {
            self.desired
                .server_locations
                .entry(entry.server.clone())
                .or_default()
                .push(name.to_string());
            self.desired.locations.insert(name.to_string(), entry);
            self
        }

        fn plan(&self, fake: &Fake) -> Plan {
            let stored = fake.stored();
            plan(&PlanInput {
                prefix: PREFIX,
                desired: &self.desired,
                stored: &stored,
                keep_upstreams: &self.keep,
            })
        }

        /// Plans, applies, and checks that planning again gives no steps.
        fn converge(&self, fake: &mut Fake) -> Plan {
            let plan = self.plan(fake);
            fake.apply(&plan);
            let again = self.plan(fake);
            assert_eq!(again.steps, vec![], "second plan not empty");
            plan
        }
    }

    fn whats(plan: &Plan) -> Vec<&'static str> {
        plan.steps.iter().map(|s| s.what).collect()
    }

    #[test]
    fn new_route_writes_locations_then_servers() {
        let mut fake = Fake::new();
        fake.upstream("k8s-shop-api-80-aaaa");
        fake.server("http", &["hand"]);
        fake.put(
            "locations",
            "hand",
            "[locations.hand]\nupstream = \"manual\"\n",
        );
        fake.upstream("manual");
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-shop-api-80-aaaa"));

        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["locations", "servers"]);
        assert_eq!(
            plan.steps[0].ops[0],
            Op::Put {
                key: "/pingap/locations/k8s-shop-web-1.toml".into(),
                value: render_location("k8s-shop-web-1", &loc("web", "k8s-shop-api-80-aaaa")),
                expect: Expect::Absent,
            }
        );
        assert!(plan.steps[0].ops.contains(&Op::RequireExists {
            key: "/pingap/upstreams/k8s-shop-api-80-aaaa.toml".into()
        }));
        assert!(plan.steps[1].ops.contains(&Op::RequireExists {
            key: "/pingap/locations/k8s-shop-web-1.toml".into()
        }));
        assert_eq!(
            server_locations(fake.get("servers", "http").unwrap(), "http").unwrap(),
            s(&["hand", "k8s-shop-web-1"])
        );
        assert_eq!(
            plan.programmed,
            BTreeSet::from(["k8s-shop-web-1".to_string()])
        );
        assert!(plan.deferred.is_empty());
    }

    #[test]
    fn external_upstream_and_certificate_come_first() {
        let mut fake = Fake::new();
        fake.server("http", &[]);
        let mut case = Case::new();
        let ext = ExternalUpstream {
            service: ServiceKey {
                namespace: "shop".into(),
                name: "legacy".into(),
            },
            addr: "legacy.example.com:80".into(),
            health_check: None,
            h2c: false,
            tls: None,
        };
        case.desired
            .external
            .insert("k8s-shop-legacy-80-ext-bbbb".into(), ext);
        case.desired.certificates.insert(
            "k8s-shop-tls-cccc".into(),
            CertificateEntry {
                secret: SecretKey {
                    namespace: "shop".into(),
                    name: "tls".into(),
                },
                domains: s(&["shop.example.com"]),
                tls_cert: "cert".into(),
                tls_key: "key".into(),
            },
        );
        case.location("k8s-shop-web-1", loc("web", "k8s-shop-legacy-80-ext-bbbb"));
        let plan = case.converge(&mut fake);
        assert_eq!(
            whats(&plan),
            vec![
                "upstreams, certificates and plugins",
                "locations",
                "servers"
            ]
        );
        assert_eq!(plan.steps[0].ops.len(), 2);
        // The upstream is written in step 1: no RequireExists for it in step 2.
        assert_eq!(plan.steps[1].ops.len(), 1);
        assert!(fake.has("upstreams", "k8s-shop-legacy-80-ext-bbbb"));
        assert!(fake.has("certificates", "k8s-shop-tls-cccc"));

        // Removing them: certificate deleted in step 1, upstream after the location.
        let case = Case::new();
        let plan = case.converge(&mut fake);
        assert_eq!(
            whats(&plan),
            vec![
                "upstreams, certificates and plugins",
                "servers",
                "stale locations",
                "stale upstreams"
            ]
        );
        assert!(!fake.has("certificates", "k8s-shop-tls-cccc"));
        assert!(!fake.has("upstreams", "k8s-shop-legacy-80-ext-bbbb"));
    }

    #[test]
    fn certificate_of_a_broken_secret_is_kept() {
        let secret = SecretKey {
            namespace: "shop".into(),
            name: "tls".into(),
        };
        let name = names::certificate_name(&secret);
        let mut fake = Fake::new();
        fake.server("http", &[]);
        let mut case = Case::new();
        case.desired.certificates.insert(
            name.clone(),
            CertificateEntry {
                secret: secret.clone(),
                domains: s(&["shop.example.com"]),
                tls_cert: "cert".into(),
                tls_key: "key".into(),
            },
        );
        case.converge(&mut fake);
        assert!(fake.has("certificates", &name));

        // The Secret becomes invalid or goes away while a listener names it: kept.
        let mut case = Case::new();
        case.desired.referenced.secrets.insert(secret.clone());
        let plan = case.converge(&mut fake);
        assert_eq!(plan.steps, vec![]);
        assert!(fake.has("certificates", &name));

        // No listener names it any more: deleted.
        Case::new().converge(&mut fake);
        assert!(!fake.has("certificates", &name));
    }

    #[test]
    fn deleted_route_strips_servers_then_deletes() {
        let mut fake = Fake::new();
        fake.upstream("k8s-shop-api-80-aaaa");
        fake.server("http", &[]);
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-shop-api-80-aaaa"));
        case.converge(&mut fake);

        let case = Case::new();
        let plan = case.converge(&mut fake);
        assert_eq!(
            whats(&plan),
            vec!["servers", "stale locations", "stale upstreams"]
        );
        assert!(!fake.has("locations", "k8s-shop-web-1"));
        assert!(!fake.has("upstreams", "k8s-shop-api-80-aaaa"));
        assert!(plan.programmed.is_empty());
    }

    #[test]
    fn backend_switch_writes_new_upstream_before_deleting_old() {
        let mut fake = Fake::new();
        fake.upstream("k8s-old");
        fake.server("http", &[]);
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-old"));
        case.converge(&mut fake);

        fake.upstream("k8s-new");
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-new"));
        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["locations", "stale upstreams"]);
        assert!(fake.has("upstreams", "k8s-new"));
        assert!(!fake.has("upstreams", "k8s-old"));
    }

    #[test]
    fn missing_upstream_keeps_a_stored_location_and_writes_a_new_one_without_it() {
        let mut fake = Fake::new();
        fake.upstream("k8s-old");
        fake.server("http", &[]);
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-old"));
        case.converge(&mut fake);
        let before = fake.get("locations", "k8s-shop-web-1").cloned();

        // The new upstream is not stored yet: the stored location keeps serving through
        // the old one, and a new location answers 503 until the upstream is there.
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-new"));
        case.location("k8s-shop-web-2", loc("web", "k8s-new"));
        let plan = case.plan(&fake);
        fake.apply(&plan);
        assert_eq!(
            plan.deferred,
            BTreeMap::from([
                (
                    "k8s-shop-web-1".to_string(),
                    Deferred::MissingUpstream("k8s-new".into())
                ),
                (
                    "k8s-shop-web-2".to_string(),
                    Deferred::MissingUpstream("k8s-new".into())
                ),
            ])
        );
        assert_eq!(fake.get("locations", "k8s-shop-web-1").cloned(), before);
        assert!(fake.has("upstreams", "k8s-old"));
        let new = fake.get("locations", "k8s-shop-web-2").unwrap();
        assert_eq!(
            render::location_upstream(new, "k8s-shop-web-2").unwrap(),
            None
        );
        assert_eq!(
            plan.programmed,
            set_of(&["k8s-shop-web-1", "k8s-shop-web-2"])
        );

        // Once the endpoint writer stores it, both name it and the old one goes.
        fake.upstream("k8s-new");
        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["locations", "stale upstreams"]);
        assert!(plan.deferred.is_empty());
        assert!(!fake.has("upstreams", "k8s-old"));
    }

    fn set_of(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn generated_plugins_come_first_and_go_last() {
        let mut fake = Fake::new();
        fake.server("http", &[]);
        let mut case = Case::new();
        case.desired
            .plugins
            .insert("k8s-mock-1".into(), mock_plugin());
        let mut entry = loc("web", "unused");
        entry.upstream = None;
        entry.plugins = s(&["k8s-mock-1"]);
        case.location("k8s-shop-web-1", entry);
        let plan = case.converge(&mut fake);
        assert_eq!(
            whats(&plan),
            vec![
                "upstreams, certificates and plugins",
                "locations",
                "servers"
            ]
        );
        assert!(fake.has("plugins", "k8s-mock-1"));

        let plan = Case::new().converge(&mut fake);
        assert_eq!(
            whats(&plan),
            vec!["servers", "stale locations", "stale plugins"]
        );
        assert!(!fake.has("plugins", "k8s-mock-1"));
        assert!(!fake.has("locations", "k8s-shop-web-1"));
    }

    #[test]
    fn missing_plugin_defers() {
        let mut fake = Fake::new();
        fake.upstream("k8s-u");
        fake.server("http", &[]);
        let mut case = Case::new();
        let mut entry = loc("web", "k8s-u");
        entry.plugins = s(&["pingap:requestId", "auth"]);
        case.location("k8s-shop-web-1", entry);
        let plan = case.converge(&mut fake);
        assert_eq!(plan.steps, vec![]);
        assert_eq!(
            plan.deferred.get("k8s-shop-web-1"),
            Some(&Deferred::MissingPlugin("auth".into()))
        );
        fake.put(
            "plugins",
            "auth",
            "[plugins.auth]\ncategory = \"basic_auth\"\n",
        );
        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["locations", "servers"]);
    }

    #[test]
    fn server_without_generated_locations_loses_owned_names() {
        let mut fake = Fake::new();
        fake.upstream("k8s-u");
        fake.server("http", &[]);
        fake.server("admin", &["k8s-shop-web-1"]);
        let mut case = Case::new();
        case.location("k8s-shop-web-1", loc("web", "k8s-u"));
        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["locations", "servers"]);
        assert_eq!(
            server_locations(fake.get("servers", "admin").unwrap(), "admin").unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            server_locations(fake.get("servers", "http").unwrap(), "http").unwrap(),
            s(&["k8s-shop-web-1"])
        );
    }

    #[test]
    fn upstream_named_by_hand_made_location_or_plugin_stays() {
        let mut fake = Fake::new();
        fake.upstream("k8s-by-location");
        fake.upstream("k8s-by-plugin");
        fake.upstream("k8s-kept");
        fake.upstream("k8s-orphan");
        fake.upstream("hand-made");
        fake.put(
            "locations",
            "manual",
            "[locations.manual]\nupstream = \"k8s-by-location\"\n",
        );
        fake.put(
            "plugins",
            "split",
            "[plugins.split]\ncategory = \"traffic_splitting\"\nupstream = \"k8s-by-plugin\"\n",
        );
        let mut case = Case::new();
        case.keep.insert("k8s-kept".into());
        let plan = case.converge(&mut fake);
        assert_eq!(whats(&plan), vec!["stale upstreams"]);
        assert_eq!(plan.steps[0].ops.len(), 1);
        assert!(!fake.has("upstreams", "k8s-orphan"));
        for name in ["k8s-by-location", "k8s-by-plugin", "k8s-kept", "hand-made"] {
            assert!(fake.has("upstreams", name), "{name}");
        }
    }

    #[test]
    fn unparsable_location_blocks_upstream_gc() {
        let mut fake = Fake::new();
        fake.upstream("k8s-orphan");
        fake.put("locations", "broken", "[locations.broken\n");
        let plan = Case::new().plan(&fake);
        assert_eq!(plan.steps, vec![]);
    }

    #[test]
    fn many_routes_converge_in_one_plan() {
        let mut fake = Fake::new();
        fake.server("http", &["a"]);
        fake.server("https", &[]);
        fake.put("locations", "a", "[locations.a]\nupstream = \"x\"\n");
        fake.upstream("x");
        let mut case = Case::new();
        for i in 0..20 {
            let upstream = format!("k8s-u{}", i % 3);
            fake.upstream(&upstream);
            let mut entry = loc(&format!("i{i}"), &upstream);
            if i % 2 == 1 {
                entry.server = "https".into();
            }
            case.location(&format!("k8s-l{i}"), entry);
        }
        let plan = case.converge(&mut fake);
        assert_eq!(plan.programmed.len(), 20);
        // One RequireExists per upstream, not per location.
        let requires = plan.steps[0]
            .ops
            .iter()
            .filter(|op| matches!(op, Op::RequireExists { .. }))
            .count();
        assert_eq!(requires, 3);
        assert_eq!(
            server_locations(fake.get("servers", "http").unwrap(), "http")
                .unwrap()
                .first()
                .map(String::as_str),
            Some("a")
        );
    }
}
