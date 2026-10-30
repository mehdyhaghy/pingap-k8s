//! The Gateway writer (read, plan, apply, watch) against a real etcd. Skipped unless
//! `PINGAP_K8S_TEST_ETCD` is set to an endpoint such as `http://127.0.0.1:23790` (see
//! scripts/test-etcd.sh).

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, EventType, WatchOptions};
use pingap_k8s::etcd::{EtcdWriter, entry_key, parse_etcd_url, read_addrs};
use pingap_k8s::gateway::model::{
    ApplyOutcome, CertificateEntry, Desired, ExternalUpstream, LocationEntry, PingapStore, Plan,
    RouteKey, SecretKey,
};
use pingap_k8s::gateway::plan::{PlanInput, plan};
use pingap_k8s::gateway::render::{location_upstream, server_locations};
use pingap_k8s::gateway::writer::{GatewayWriter, StoreConflict, TxnLimits, parse_entry_key};
use pingap_k8s::types::{AddrsWriter, ServiceKey};

/// An endpoint writer and a Gateway writer under a prefix of their own, or `None` when
/// no test etcd is configured.
async fn writers(test: &str, limits: TxnLimits) -> Option<(EtcdWriter, GatewayWriter)> {
    let Ok(endpoint) = std::env::var("PINGAP_K8S_TEST_ETCD") else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping {test}");
        return None;
    };
    let host = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let target = parse_etcd_url(&format!("etcd://{host}/pingap-k8s-test-{test}-{nanos}")).unwrap();
    let etcd = EtcdWriter::connect(&target).await.unwrap();
    let gateway = GatewayWriter::new(&etcd, limits);
    Some((etcd, gateway))
}

async fn put(client: &Client, prefix: &str, category: &str, name: &str, value: &str) {
    client
        .kv_client()
        .put(entry_key(prefix, category, name), value, None)
        .await
        .unwrap();
}

async fn get(client: &Client, prefix: &str, category: &str, name: &str) -> Option<String> {
    let resp = client
        .kv_client()
        .get(entry_key(prefix, category, name), None)
        .await
        .unwrap();
    resp.kvs()
        .first()
        .map(|kv| kv.value_str().unwrap().to_string())
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn location(owner: &str, path: &str, upstream: &str) -> LocationEntry {
    LocationEntry {
        owner: RouteKey::new("shop", owner),
        server: "http".into(),
        host: Some("shop.example.com".into()),
        path: path.into(),
        match_headers: vec![],
        match_query: vec![],
        upstream: Some(upstream.into()),
        weight: 60000 + path.len() as u16 * 2,
        rewrite: None,
        proxy_set_headers: vec![],
        plugins: vec![],
        options: Default::default(),
    }
}

fn desired(locations: &[(&str, LocationEntry)]) -> Desired {
    let mut desired = Desired::default();
    for (name, entry) in locations {
        desired
            .server_locations
            .entry(entry.server.clone())
            .or_default()
            .push(name.to_string());
        desired.locations.insert(name.to_string(), entry.clone());
    }
    desired
}

async fn plan_for(w: &GatewayWriter, desired: &Desired) -> Plan {
    let stored = w.read_stored().await.unwrap();
    plan(&PlanInput {
        prefix: w.prefix(),
        desired,
        stored: &stored,
        keep_upstreams: &BTreeSet::new(),
    })
}

/// Plans and applies until nothing is left to do (at most a few rounds).
async fn converge(w: &GatewayWriter, desired: &Desired) -> ApplyOutcome {
    let plan = plan_for(w, desired).await;
    let outcome = w.apply(&plan).await.unwrap();
    let again = plan_for(w, desired).await;
    assert_eq!(again.steps, vec![], "plan after apply is not empty");
    assert_eq!(w.apply(&again).await.unwrap(), ApplyOutcome::Unchanged);
    outcome
}

const SERVER: &str = r#"# hand-made server
[servers.http]
addr = "0.0.0.0:80"
locations = [
    "hand", # first
]
"#;

/// Hand-made entries every test starts with: a server, a location and its upstream.
async fn seed(client: &Client, prefix: &str) {
    put(client, prefix, "servers", "http", SERVER).await;
    put(
        client,
        prefix,
        "locations",
        "hand",
        "[locations.hand]\nupstream = \"manual\"\npath = \"/manual\"\n",
    )
    .await;
    put(
        client,
        prefix,
        "upstreams",
        "manual",
        "[upstreams.manual]\naddrs = [\"10.9.9.9:80\"]\n",
    )
    .await;
}

#[tokio::test]
async fn apply_creates_and_removes_entries() {
    let Some((etcd, w)) = writers("gateway-apply", TxnLimits::default()).await else {
        return;
    };
    let client = etcd.client();
    let prefix = w.prefix().to_string();
    seed(&client, &prefix).await;
    // A pods upstream, as the endpoint writer creates it.
    etcd.write_addrs("k8s-shop-api-80-aaaa", &s(&["10.0.0.1:80"]))
        .await
        .unwrap();

    let mut want = desired(&[
        (
            "k8s-shop-web-1",
            location("web", "/", "k8s-shop-api-80-aaaa"),
        ),
        (
            "k8s-shop-web-2",
            location("web", "=/legacy", "k8s-shop-legacy-80-ext-bbbb"),
        ),
    ]);
    want.external.insert(
        "k8s-shop-legacy-80-ext-bbbb".into(),
        ExternalUpstream {
            service: ServiceKey {
                namespace: "shop".into(),
                name: "legacy".into(),
            },
            addr: "legacy.example.com:80".into(),
            health_check: None,
        },
    );
    want.certificates.insert(
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
    let outcome = converge(&w, &want).await;
    let ApplyOutcome::Applied { puts, deletes, .. } = outcome else {
        panic!("expected Applied, got {outcome:?}");
    };
    assert_eq!((puts, deletes), (5, 0));
    let server = get(&client, &prefix, "servers", "http").await.unwrap();
    assert_eq!(
        server_locations(&server, "http").unwrap(),
        s(&["hand", "k8s-shop-web-1", "k8s-shop-web-2"])
    );
    assert!(server.contains("# hand-made server") && server.contains("# first"));
    assert!(
        get(&client, &prefix, "certificates", "k8s-shop-tls-cccc")
            .await
            .is_some()
    );

    // The route goes away: owned entries are removed, hand-made ones stay.
    let outcome = converge(&w, &Desired::default()).await;
    assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
    let server = get(&client, &prefix, "servers", "http").await.unwrap();
    assert_eq!(server, SERVER);
    let stored = w.read_stored().await.unwrap();
    assert_eq!(stored.locations.keys().collect::<Vec<_>>(), vec!["hand"]);
    assert_eq!(stored.upstreams.keys().collect::<Vec<_>>(), vec!["manual"]);
    assert!(stored.certificates.is_empty());
}

#[tokio::test]
async fn concurrent_change_fails_the_apply_and_replanning_recovers() {
    let Some((etcd, w)) = writers("gateway-cas", TxnLimits::default()).await else {
        return;
    };
    let client = etcd.client();
    let prefix = w.prefix().to_string();
    seed(&client, &prefix).await;
    etcd.write_addrs("k8s-u", &s(&["10.0.0.1:80"]))
        .await
        .unwrap();
    let want = desired(&[("k8s-shop-web-1", location("web", "/", "k8s-u"))]);

    let plan = plan_for(&w, &want).await;
    // Someone saves the server in the admin between the read and the apply.
    let edited = SERVER.replace("\"hand\", # first\n", "\"hand\", # first\n    \"hand\",\n");
    put(&client, &prefix, "servers", "http", &edited).await;
    let err = w.apply(&plan).await.unwrap_err();
    let conflict = err
        .downcast_ref::<StoreConflict>()
        .unwrap_or_else(|| panic!("expected StoreConflict, got {err:#}"));
    assert_eq!(conflict.step, "servers");
    // The step before the conflict is applied; the server is as the admin left it.
    assert!(
        get(&client, &prefix, "locations", "k8s-shop-web-1")
            .await
            .is_some()
    );
    assert_eq!(
        get(&client, &prefix, "servers", "http").await.unwrap(),
        edited
    );

    // Read and plan again: the admin's edit is kept.
    converge(&w, &want).await;
    let server = get(&client, &prefix, "servers", "http").await.unwrap();
    assert_eq!(
        server_locations(&server, "http").unwrap(),
        s(&["hand", "hand", "k8s-shop-web-1"])
    );
}

/// Pingap's reload checks that matter for ordering: every listed location exists, every
/// location's upstream exists, no upstream has empty addrs.
fn validate(state: &BTreeMap<String, String>, prefix: &str, revision: i64) {
    let mut entries: BTreeMap<&str, BTreeMap<&str, &str>> = BTreeMap::new();
    for (key, value) in state {
        if let Some((category, name)) = parse_entry_key(prefix, key) {
            entries
                .entry(category)
                .or_default()
                .insert(name, value.as_str());
        }
    }
    let empty = BTreeMap::new();
    let servers = entries.get("servers").unwrap_or(&empty);
    let locations = entries.get("locations").unwrap_or(&empty);
    let upstreams = entries.get("upstreams").unwrap_or(&empty);
    for (server, value) in servers {
        for name in server_locations(value, server).unwrap() {
            assert!(
                locations.contains_key(name.as_str()),
                "revision {revision}: server {server} lists missing location {name}"
            );
        }
    }
    for (name, value) in locations {
        if let Some(upstream) = location_upstream(value, name).unwrap() {
            assert!(
                upstreams.contains_key(upstream.as_str()),
                "revision {revision}: location {name} names missing upstream {upstream}"
            );
        }
    }
    for (name, value) in upstreams {
        let addrs = read_addrs(value, name).unwrap().unwrap_or_default();
        assert!(
            !addrs.is_empty(),
            "revision {revision}: upstream {name} has no addrs"
        );
    }
}

#[tokio::test]
async fn every_intermediate_revision_is_valid() {
    // Two ops per Txn: many revisions, each of which Pingap reloads.
    let limits = TxnLimits {
        max_ops: 2,
        max_bytes: 1 << 20,
    };
    let Some((etcd, w)) = writers("gateway-revisions", limits).await else {
        return;
    };
    let client = etcd.client();
    let prefix = w.prefix().to_string();
    seed(&client, &prefix).await;
    for upstream in ["k8s-old-a", "k8s-old-b"] {
        etcd.write_addrs(upstream, &s(&["10.0.0.1:80"]))
            .await
            .unwrap();
    }
    let paths = ["/", "/a", "/b", "/c", "/d", "/e"];
    let before: Vec<(String, LocationEntry)> = paths
        .iter()
        .enumerate()
        .map(|(i, path)| {
            let upstream = if i % 2 == 0 { "k8s-old-a" } else { "k8s-old-b" };
            (format!("k8s-shop-web-{i}"), location("web", path, upstream))
        })
        .collect();
    let refs: Vec<(&str, LocationEntry)> = before
        .iter()
        .map(|(n, e)| (n.as_str(), e.clone()))
        .collect();
    converge(&w, &desired(&refs)).await;

    // Switch every backend, drop half of the locations and add new ones.
    for upstream in ["k8s-new-a", "k8s-new-b"] {
        etcd.write_addrs(upstream, &s(&["10.0.0.2:80"]))
            .await
            .unwrap();
    }
    let mut after: Vec<(String, LocationEntry)> = paths
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 2 == 0)
        .map(|(i, path)| {
            (
                format!("k8s-shop-web-{i}"),
                location("web", path, "k8s-new-a"),
            )
        })
        .collect();
    for (i, path) in ["/x", "/y", "/z"].iter().enumerate() {
        after.push((
            format!("k8s-shop-api-{i}"),
            location("api", path, "k8s-new-b"),
        ));
    }
    let refs: Vec<(&str, LocationEntry)> =
        after.iter().map(|(n, e)| (n.as_str(), e.clone())).collect();
    let want = desired(&refs);

    let stored = w.read_stored().await.unwrap();
    let mut state: BTreeMap<String, String> = [
        &stored.servers,
        &stored.locations,
        &stored.upstreams,
        &stored.certificates,
        &stored.plugins,
    ]
    .into_iter()
    .flat_map(|m| m.values().map(|e| (e.key.clone(), e.value.clone())))
    .collect();
    validate(&state, &prefix, stored.revision);
    let mut events = client
        .watch_client()
        .watch(
            prefix.as_str(),
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(stored.revision + 1),
            ),
        )
        .await
        .unwrap();

    let plan = plan(&PlanInput {
        prefix: &prefix,
        desired: &want,
        stored: &stored,
        keep_upstreams: &BTreeSet::new(),
    });
    let outcome = w.apply(&plan).await.unwrap();
    let ApplyOutcome::Applied { revision: last, .. } = outcome else {
        panic!("expected Applied, got {outcome:?}");
    };

    let mut revisions = 0;
    let mut seen = stored.revision;
    while seen < last {
        let resp = tokio::time::timeout(Duration::from_secs(5), events.message())
            .await
            .expect("watch stalled")
            .unwrap()
            .expect("watch ended");
        // Group events by revision: one Txn is one revision.
        let mut current = None;
        for event in resp.events() {
            let kv = event.kv().unwrap();
            let revision = kv.mod_revision();
            if current.is_some_and(|c| c != revision) {
                validate(&state, &prefix, current.unwrap());
                revisions += 1;
            }
            current = Some(revision);
            let key = kv.key_str().unwrap().to_string();
            match event.event_type() {
                EventType::Put => {
                    state.insert(key, kv.value_str().unwrap().to_string());
                }
                EventType::Delete => {
                    state.remove(&key);
                }
            }
        }
        if let Some(revision) = current {
            validate(&state, &prefix, revision);
            revisions += 1;
            seen = revision;
        }
    }
    assert!(revisions >= 5, "only {revisions} revisions");
    assert!(!state.keys().any(|k| k.contains("k8s-old-")));
    assert!(!state.keys().any(|k| k.ends_with("/k8s-shop-web-1.toml")));
    let again = plan_for(&w, &want).await;
    assert_eq!(again.steps, vec![]);
}

#[tokio::test]
async fn watch_changes_ignores_addrs_only_updates() {
    let Some((etcd, w)) = writers("gateway-watch", TxnLimits::default()).await else {
        return;
    };
    let client = etcd.client();
    let prefix = w.prefix().to_string();
    etcd.write_addrs("k8s-u", &s(&["10.0.0.1:80"]))
        .await
        .unwrap();
    let mut rx = w.watch_changes().await.unwrap();

    let quiet = |rx: &mut tokio::sync::mpsc::Receiver<()>| {
        let got = rx.try_recv();
        assert!(got.is_err(), "unexpected signal");
    };
    async fn signal(rx: &mut tokio::sync::mpsc::Receiver<()>) {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("no signal within 5s")
            .expect("watch channel closed");
    }

    // Phase 1 rewriting addrs: no signal.
    etcd.write_addrs("k8s-u", &s(&["10.0.0.2:80"]))
        .await
        .unwrap();
    etcd.write_addrs("k8s-u", &s(&["10.0.0.2:80", "10.0.0.3:80"]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    quiet(&mut rx);

    // An upstream created, an upstream edited beyond addrs, a location, a server, a
    // plugin, a certificate, an upstream deleted: each signals.
    etcd.write_addrs("k8s-v", &s(&["10.0.0.4:80"]))
        .await
        .unwrap();
    signal(&mut rx).await;
    put(
        &client,
        &prefix,
        "upstreams",
        "k8s-u",
        "[upstreams.k8s-u]\naddrs = [\"10.0.0.2:80\"]\nhealth_check = \"tcp://x\"\n",
    )
    .await;
    signal(&mut rx).await;
    for category in ["locations", "servers", "plugins", "certificates"] {
        put(&client, &prefix, category, "x", "").await;
        signal(&mut rx).await;
    }
    client
        .kv_client()
        .delete(entry_key(&prefix, "upstreams", "k8s-v"), None)
        .await
        .unwrap();
    signal(&mut rx).await;

    // Keys that are not entries: no signal.
    client
        .kv_client()
        .put(format!("{prefix}/basic.toml"), "x", None)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    quiet(&mut rx);
}
