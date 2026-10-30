//! `EtcdWriter` against a real etcd. Skipped unless `PINGAP_K8S_TEST_ETCD` is set to an
//! endpoint such as `http://127.0.0.1:23790` (see scripts/test-etcd.sh).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::Client;
use pingap_k8s::etcd::{EtcdError, EtcdWriter, MAX_CAS_RETRIES, parse_etcd_url};
use pingap_k8s::types::{AddrsWriter, UpstreamTls, WriteOutcome};

/// A writer under a prefix of its own, or `None` when no test etcd is configured.
async fn writer(test: &str) -> Option<EtcdWriter> {
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
    Some(EtcdWriter::connect(&target).await.unwrap())
}

async fn get(client: &Client, key: &str) -> Option<(String, i64)> {
    let resp = client.kv_client().get(key, None).await.unwrap();
    resp.kvs()
        .first()
        .map(|kv| (kv.value_str().unwrap().to_string(), kv.mod_revision()))
}

async fn put(client: &Client, key: &str, value: &str) -> i64 {
    let resp = client.kv_client().put(key, value, None).await.unwrap();
    resp.header().unwrap().revision()
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

const ENTRY: &str = r#"# edited in the admin
[upstreams.api]
addrs = [
    "10.0.0.9:80",
]
health_check = "http://api/ping" # keep me
connection_timeout = "10s"
"#;

#[tokio::test]
async fn only_addrs_change() {
    let Some(w) = writer("only-addrs").await else {
        return;
    };
    let key = w.upstream_key("api");
    assert!(key.ends_with("/upstreams/api.toml"));
    put(&w.client(), &key, ENTRY).await;

    let outcome = w
        .write_addrs("api", &s(&["10.0.0.1:80", "[2001:db8::1]:80 3"]))
        .await
        .unwrap();
    let WriteOutcome::Updated { revision } = outcome else {
        panic!("expected Updated, got {outcome:?}")
    };
    let (value, mod_revision) = get(&w.client(), &key).await.unwrap();
    assert_eq!(mod_revision, revision);
    assert_eq!(
        value,
        ENTRY.replace(
            "    \"10.0.0.9:80\",\n",
            "    \"10.0.0.1:80\",\n    \"[2001:db8::1]:80 3\",\n"
        )
    );
}

#[tokio::test]
async fn unchanged_list_not_written() {
    let Some(w) = writer("unchanged").await else {
        return;
    };
    let key = w.upstream_key("api");
    let before = put(&w.client(), &key, ENTRY).await;
    let outcome = w.write_addrs("api", &s(&["10.0.0.9:80"])).await.unwrap();
    assert_eq!(outcome, WriteOutcome::Unchanged);
    let (_, mod_revision) = get(&w.client(), &key).await.unwrap();
    assert_eq!(mod_revision, before);
}

#[tokio::test]
async fn missing_key_gets_minimal_upstream() {
    let Some(w) = writer("missing").await else {
        return;
    };
    let key = w.upstream_key("new-api");
    let outcome = w
        .write_addrs("new-api", &s(&["10.0.0.1:80", "10.0.0.2:80"]))
        .await
        .unwrap();
    let WriteOutcome::Created { revision } = outcome else {
        panic!("expected Created, got {outcome:?}")
    };
    let (value, mod_revision) = get(&w.client(), &key).await.unwrap();
    assert_eq!(mod_revision, revision);
    assert_eq!(
        value,
        "[upstreams.new-api]\naddrs = [\"10.0.0.1:80\", \"10.0.0.2:80\"]\n"
    );
    // A second write of the same list is a no-op.
    let again = w
        .write_addrs("new-api", &s(&["10.0.0.1:80", "10.0.0.2:80"]))
        .await
        .unwrap();
    assert_eq!(again, WriteOutcome::Unchanged);
}

#[tokio::test]
async fn created_entry_gets_health_check_once() {
    let Some(w) = writer("health-check").await else {
        return;
    };
    let key = w.upstream_key("k8s-api");
    let hc = "http://api/healthz?check_frequency=5s";
    w.write_upstream("k8s-api", &s(&["10.0.0.1:80"]), Some(hc), None, None)
        .await
        .unwrap();
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert_eq!(
        value,
        format!("[upstreams.k8s-api]\naddrs = [\"10.0.0.1:80\"]\nhealth_check = \"{hc}\"\n")
    );
    // An existing entry only gets its addrs replaced, whatever health check is asked for.
    let edited = value.replace("check_frequency=5s", "check_frequency=9s");
    put(&w.client(), &key, &edited).await;
    w.write_upstream("k8s-api", &s(&["10.0.0.2:80"]), Some(hc), None, None)
        .await
        .unwrap();
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(value.contains("check_frequency=9s"), "{value}");
    assert!(value.contains("10.0.0.2:80"), "{value}");
}

#[tokio::test]
async fn h2c_upstreams_get_alpn_once() {
    let Some(w) = writer("alpn").await else {
        return;
    };
    // Created with it.
    let key = w.upstream_key("k8s-grpc");
    w.write_upstream("k8s-grpc", &s(&["10.0.0.1:9000"]), None, None, Some("H2"))
        .await
        .unwrap();
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert_eq!(
        value,
        "[upstreams.k8s-grpc]\naddrs = [\"10.0.0.1:9000\"]\nalpn = \"H2\"\n"
    );
    // Same addrs and alpn: nothing to write.
    let outcome = w
        .write_upstream("k8s-grpc", &s(&["10.0.0.1:9000"]), None, None, Some("H2"))
        .await
        .unwrap();
    assert_eq!(outcome, WriteOutcome::Unchanged);

    // An existing entry without alpn gets it, even when the addrs are already right.
    let key = w.upstream_key("api");
    put(
        &w.client(),
        &key,
        "[upstreams.api]\naddrs = [\"10.0.0.1:80\"] # mine\n",
    )
    .await;
    let outcome = w
        .write_upstream("api", &s(&["10.0.0.1:80"]), None, None, Some("H2"))
        .await
        .unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Updated { .. }),
        "{outcome:?}"
    );
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert_eq!(
        value,
        "[upstreams.api]\naddrs = [\"10.0.0.1:80\"] # mine\nalpn = \"H2\"\n"
    );
    // A hand-set alpn is kept.
    put(
        &w.client(),
        &key,
        "[upstreams.api]\naddrs = [\"10.0.0.1:80\"]\nalpn = \"H1\"\n",
    )
    .await;
    let outcome = w
        .write_upstream("api", &s(&["10.0.0.2:80"]), None, None, Some("H2"))
        .await
        .unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Updated { .. }),
        "{outcome:?}"
    );
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(value.contains("alpn = \"H1\""), "{value}");
    assert!(value.contains("10.0.0.2:80"), "{value}");
}

#[tokio::test]
async fn created_entry_gets_tls_fields() {
    let Some(w) = writer("tls").await else {
        return;
    };
    let key = w.upstream_key("k8s-api-tls");
    let tls = UpstreamTls {
        sni: "api.example.com".into(),
        ca: Some("CA PEM".into()),
        client_cert: Some(("CERT PEM".into(), "KEY PEM".into())),
    };
    w.write_upstream(
        "k8s-api-tls",
        &s(&["10.0.0.1:8443"]),
        None,
        Some(&tls),
        None,
    )
    .await
    .unwrap();
    let (value, _) = get(&w.client(), &key).await.unwrap();
    let table: toml::Table = toml::from_str(&value).unwrap();
    let entry = &table["upstreams"]["k8s-api-tls"];
    assert_eq!(entry["sni"].as_str(), Some("api.example.com"));
    assert_eq!(entry["verify_cert"].as_bool(), Some(true));
    assert_eq!(entry["ca"].as_str(), Some("CA PEM"));
    assert_eq!(entry["client_cert"].as_str(), Some("CERT PEM"));
    assert_eq!(entry["client_key"].as_str(), Some("KEY PEM"));
    // Later writes replace the addrs only.
    w.write_upstream(
        "k8s-api-tls",
        &s(&["10.0.0.2:8443"]),
        None,
        Some(&tls),
        None,
    )
    .await
    .unwrap();
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(
        value.contains("10.0.0.2:8443") && value.contains("sni"),
        "{value}"
    );
}

#[tokio::test]
async fn malformed_toml_not_written() {
    let Some(w) = writer("malformed").await else {
        return;
    };
    let key = w.upstream_key("api");
    let bad = "[upstreams.api\naddrs = [\"10.0.0.9:80\"\n";
    let before = put(&w.client(), &key, bad).await;
    let err = w
        .write_addrs("api", &s(&["10.0.0.1:80"]))
        .await
        .unwrap_err();
    assert!(
        matches!(err.downcast_ref::<EtcdError>(), Some(EtcdError::Toml(_))),
        "{err:#}"
    );
    assert_eq!(
        get(&w.client(), &key).await.unwrap(),
        (bad.to_string(), before)
    );

    // An existing key without the [upstreams.api] table is not written either.
    let other = "[upstreams.other]\naddrs = [\"1.1.1.1:1\"]\n";
    let before = put(&w.client(), &key, other).await;
    let err = w
        .write_addrs("api", &s(&["10.0.0.1:80"]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<EtcdError>(),
            Some(EtcdError::MissingTable(_))
        ),
        "{err:#}"
    );
    assert_eq!(
        get(&w.client(), &key).await.unwrap(),
        (other.to_string(), before)
    );
}

#[tokio::test]
async fn empty_addrs_never_written() {
    let Some(w) = writer("empty").await else {
        return;
    };
    // Neither an existing entry nor a missing key gets an empty list.
    let key = w.upstream_key("api");
    let before = put(&w.client(), &key, ENTRY).await;
    let err = w.write_addrs("api", &[]).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<EtcdError>(),
            Some(EtcdError::EmptyAddrs(_))
        ),
        "{err:#}"
    );
    assert_eq!(
        get(&w.client(), &key).await.unwrap(),
        (ENTRY.to_string(), before)
    );
    assert!(w.write_addrs("missing", &[]).await.is_err());
    assert_eq!(get(&w.client(), &w.upstream_key("missing")).await, None);
}

#[tokio::test]
async fn cas_conflict_retry_keeps_concurrent_edit() {
    let Some(mut w) = writer("cas").await else {
        return;
    };
    let key = w.upstream_key("api");
    put(&w.client(), &key, ENTRY).await;

    // Between the controller's GET and its Txn, an admin save changes another field.
    let client = w.client();
    let hook_key = key.clone();
    let calls = Arc::new(AtomicU32::new(0));
    let hook_calls = calls.clone();
    w.set_pre_commit_hook(move |attempt| {
        let client = client.clone();
        let key = hook_key.clone();
        hook_calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                let edited = ENTRY.replace("10s", "30s");
                client.kv_client().put(key, edited, None).await.unwrap();
            }
        }
    });

    let outcome = w.write_addrs("api", &s(&["10.0.0.1:80"])).await.unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Updated { .. }),
        "{outcome:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2, "one conflict, one retry");
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(value.contains("connection_timeout = \"30s\""), "{value}");
    assert!(value.contains("\"10.0.0.1:80\""), "{value}");
    assert!(!value.contains("10.0.0.9"), "{value}");
}

#[tokio::test]
async fn cas_conflict_gives_up_after_retries() {
    let Some(mut w) = writer("cas-give-up").await else {
        return;
    };
    let key = w.upstream_key("api");
    put(&w.client(), &key, ENTRY).await;

    // Someone writes the key before every Txn: the controller never wins.
    let client = w.client();
    let hook_key = key.clone();
    w.set_pre_commit_hook(move |attempt| {
        let client = client.clone();
        let key = hook_key.clone();
        async move {
            let edited = ENTRY.replace("10s", &format!("{}s", 100 + attempt));
            client.kv_client().put(key, edited, None).await.unwrap();
        }
    });
    let err = w
        .write_addrs("api", &s(&["10.0.0.1:80"]))
        .await
        .unwrap_err();
    match err.downcast_ref::<EtcdError>() {
        Some(EtcdError::Conflict { attempts, .. }) => assert_eq!(*attempts, MAX_CAS_RETRIES + 1),
        other => panic!("expected Conflict, got {other:?} ({err:#})"),
    }
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(value.contains("10.0.0.9:80"), "{value}");
    assert!(
        value.contains(&format!("{}s", 100 + MAX_CAS_RETRIES)),
        "{value}"
    );
}

#[tokio::test]
async fn create_conflict_retries_as_update() {
    let Some(mut w) = writer("create-race").await else {
        return;
    };
    let key = w.upstream_key("api");
    // The key is missing at the GET, then the admin creates it before the Txn.
    let client = w.client();
    let hook_key = key.clone();
    w.set_pre_commit_hook(move |attempt| {
        let client = client.clone();
        let key = hook_key.clone();
        async move {
            if attempt == 0 {
                client.kv_client().put(key, ENTRY, None).await.unwrap();
            }
        }
    });
    let outcome = w.write_addrs("api", &s(&["10.0.0.1:80"])).await.unwrap();
    assert!(
        matches!(outcome, WriteOutcome::Updated { .. }),
        "{outcome:?}"
    );
    let (value, _) = get(&w.client(), &key).await.unwrap();
    assert!(
        value.contains("health_check = \"http://api/ping\" # keep me"),
        "{value}"
    );
    assert!(value.contains("\"10.0.0.1:80\""), "{value}");
}
