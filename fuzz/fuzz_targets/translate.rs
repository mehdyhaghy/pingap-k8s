//! Gateway API translation, rendering and planning on generated worlds.
//!
//! A world is translated against Pingap servers stored in an in-memory etcd, its plan is
//! applied step by step (the config must stay valid after each step), then a world with
//! some objects deleted goes through the same. Checks: no panic; the translation does
//! not depend on the order of its input lists; every generated name is owned (`k8s-`)
//! and a valid entry name; rendered entries parse and read back; and a second plan
//! after an apply has nothing left to do.
#![no_main]

use std::collections::BTreeSet;

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use pingap_k8s::gateway::model::{Desired, PluginEntry, RouteKind, ServerInfo, Stored, is_owned};
use pingap_k8s::gateway::plan::{BUILTIN_PLUGINS, PlanInput, plan};
use pingap_k8s::gateway::render;
use pingap_k8s::gateway::translate::{referenced, translate};
use pingap_k8s_fuzz::store::{PREFIX, Store};
use pingap_k8s_fuzz::valid_name;
use pingap_k8s_fuzz::world::{self, World};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(servers) = world::servers(&mut u) else {
        return;
    };
    let Ok(first) = world::world(&mut u) else {
        return;
    };
    let Ok(second) = first.subset(&mut u) else {
        return;
    };

    let mut store = Store::default();
    store.put(
        "upstreams",
        "manual-up",
        "[upstreams.manual-up]\naddrs = [\"10.0.0.9:80\"]\n",
    );
    store.put(
        "locations",
        "manual",
        "[locations.manual]\nupstream = \"manual-up\"\n",
    );
    for (name, toml) in &servers {
        store.put("servers", name, toml);
    }
    for plugin in &first.plugins {
        store.put(
            "plugins",
            plugin,
            &format!("[plugins.{plugin}]\ncategory = \"ping\"\npath = \"/ping\"\n"),
        );
    }
    store.validate("setup");

    pass(&first, &mut store);
    pass(&second, &mut store);
});

/// One controller pass: translate, plan, apply, and check that it converged.
fn pass(world: &World, store: &mut Store) {
    let stored = store.stored();
    let servers: Vec<ServerInfo> = stored
        .servers
        .iter()
        .filter_map(|(name, entry)| render::server_info(&entry.value, name))
        .collect();
    let plugins: BTreeSet<String> = stored
        .plugins
        .keys()
        .cloned()
        .chain(BUILTIN_PLUGINS.iter().map(|p| p.to_string()))
        .collect();

    let input = world.input(&servers, &plugins);
    let desired = translate(&input);
    let refs = referenced(&input);
    assert!(
        desired.referenced.services.is_subset(&refs.services),
        "translate refers to Services that referenced() does not list"
    );

    let reversed = world.reversed();
    let other = translate(&reversed.input(&servers, &plugins));
    assert_same(&desired, &other);

    check_desired(&desired, &plugins);

    // The endpoint writer creates the generated upstreams before the plan deletes old
    // ones (the controller waits for it).
    store.create_upstreams(desired.upstreams.keys());
    let keep: BTreeSet<String> = desired.upstreams.keys().cloned().collect();
    let first = plan(&PlanInput {
        prefix: PREFIX,
        desired: &desired,
        stored: &store.stored(),
        keep_upstreams: &keep,
    });
    store.apply(&first);

    let stored = store.stored();
    check_stored(&desired, &stored, &first.deferred.keys().cloned().collect());
    let again = plan(&PlanInput {
        prefix: PREFIX,
        desired: &desired,
        stored: &stored,
        keep_upstreams: &keep,
    });
    assert!(
        again.steps.is_empty(),
        "a second plan still has steps: {:#?}",
        again.steps
    );
}

fn assert_same(a: &Desired, b: &Desired) {
    assert_eq!(a.locations, b.locations, "locations depend on input order");
    assert_eq!(
        a.server_locations, b.server_locations,
        "server locations depend on input order"
    );
    assert_eq!(a.upstreams, b.upstreams, "upstreams depend on input order");
    assert_eq!(
        a.external, b.external,
        "external upstreams depend on input order"
    );
    assert_eq!(
        a.certificates, b.certificates,
        "certificates depend on input order"
    );
    assert_eq!(a.plugins, b.plugins, "plugins depend on input order");
    assert_eq!(a.status, b.status, "statuses depend on input order");
    assert_eq!(
        a.referenced, b.referenced,
        "references depend on input order"
    );
}

fn owned(kind: &str, name: &str) {
    assert!(is_owned(name), "{kind} {name:?} is not owned");
    assert!(
        valid_name(name),
        "{kind} {name:?} is not a valid entry name"
    );
}

fn check_desired(desired: &Desired, plugins: &BTreeSet<String>) {
    for (name, location) in &desired.locations {
        owned("location", name);
        let rendered = render::render_location(name, location);
        assert!(
            render::same_entry(&rendered, &rendered),
            "location {name} does not parse:\n{rendered}"
        );
        assert_eq!(
            render::location_upstream(&rendered, name).unwrap(),
            location.upstream,
            "{rendered}"
        );
        assert_eq!(
            render::location_plugins(&rendered, name),
            location.plugins,
            "{rendered}"
        );
        if location.owner_kind == RouteKind::Http {
            assert_eq!(
                render::location_owner(&rendered, name),
                Some(location.owner.clone()),
                "{rendered}"
            );
        }
        if let Some(upstream) = &location.upstream {
            assert!(
                desired.upstreams.contains_key(upstream) || desired.external.contains_key(upstream),
                "location {name} names upstream {upstream} that is not generated"
            );
        }
        for plugin in &location.plugins {
            assert!(
                desired.plugins.contains_key(plugin) || plugins.contains(plugin),
                "location {name} names plugin {plugin} that is neither generated nor stored"
            );
        }
        let listed = desired
            .server_locations
            .get(&location.server)
            .is_some_and(|names| names.contains(name));
        assert!(
            listed,
            "location {name} is not listed in its server {}",
            location.server
        );
    }
    for (server, names) in &desired.server_locations {
        let unique: BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "server {server} lists a location twice"
        );
        let mut last = u16::MAX;
        for name in names {
            let location = desired
                .locations
                .get(name)
                .unwrap_or_else(|| panic!("server {server} lists missing location {name}"));
            assert_eq!(
                &location.server, server,
                "location {name} is listed in another server"
            );
            assert!(
                location.weight <= last,
                "server {server}: locations are not by weight"
            );
            last = location.weight;
        }
    }
    for name in desired.upstreams.keys() {
        owned("upstream", name);
    }
    for (name, upstream) in &desired.external {
        owned("upstream", name);
        assert!(
            !desired.upstreams.contains_key(name),
            "upstream {name} is generated twice"
        );
        if let Ok(rendered) = render::render_external_upstream(name, upstream, "5s") {
            assert!(rendered.parse::<toml::Table>().is_ok(), "{rendered}");
        }
    }
    for (name, certificate) in &desired.certificates {
        owned("certificate", name);
        let rendered = render::render_certificate(name, certificate);
        assert!(rendered.parse::<toml::Table>().is_ok(), "{rendered}");
    }
    for (name, plugin) in &desired.plugins {
        owned("plugin", name);
        let PluginEntry { category, .. } = plugin;
        let rendered = render::render_plugin(name, plugin);
        let table: toml::Table = rendered
            .parse()
            .unwrap_or_else(|e| panic!("{e}:\n{rendered}"));
        assert_eq!(
            table["plugins"][name]["category"].as_str(),
            Some(category.as_str()),
            "{rendered}"
        );
    }
}

/// After the apply: every live location is stored and listed by its server in the
/// desired order, after the hand-made names.
fn check_stored(desired: &Desired, stored: &Stored, deferred: &BTreeSet<String>) {
    for (server, entry) in &stored.servers {
        let listed = render::server_locations(&entry.value, server).unwrap();
        assert!(
            listed.first().is_some_and(|n| n == "manual"),
            "server {server} lost its hand-made location: {listed:?}"
        );
        let owned: Vec<&String> = listed.iter().filter(|n| is_owned(n)).collect();
        let wanted: Vec<&String> = desired
            .server_locations
            .get(server)
            .map(|names| {
                names
                    .iter()
                    .filter(|n| !deferred.contains(*n) || stored.locations.contains_key(*n))
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(owned, wanted, "server {server}: owned locations differ");
    }
    for name in stored.locations.keys().filter(|n| is_owned(n)) {
        assert!(
            desired.locations.contains_key(name),
            "stale location {name} kept"
        );
    }
}
