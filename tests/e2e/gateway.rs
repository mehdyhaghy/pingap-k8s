//! The end-to-end tests 19–27 of the Gateway API provider, one test function each.
//!
//! The chart creates the GatewayClass `pingap` (tests/e2e/values.yaml): listeners map onto
//! the seed's Pingap servers `gw-http` (plain HTTP, port 8082) and `gw-https` (TLS by SNI,
//! port 8443), backends and listener Secrets live in `e2e-app`. The chart's Pingap
//! Service exposes port 80 only, so requests go from the client pod to the Pingap pod IP
//! with curl's `--resolve` (the Host header then carries the listener port, as for a
//! client that connects to the listener directly).
//!
//! Every test creates its own uniquely named objects (Gateways, HTTPRoutes, Secrets,
//! Services, a Namespace) with hostnames of its own (`*.t<NN>.example.com`), so the
//! listeners of different tests never conflict. The objects are deleted before the test
//! (leftovers of an aborted run) and after it, also when an assertion fails
//! ([`guarded`]). Each test prints its observations on lines starting with `[e2e_NN]`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use k8s_openapi::serde_json::{self, Value, json};

use crate::controller;
use crate::harness::{Env, wait_for};
use crate::k8s;
use crate::kubectl::Kubectl;
use crate::spec::{guarded, host_cmd};

const CLASS: &str = "pingap";
const CONTROLLER_NAME: &str = "pingap.io/gateway-controller";
const GROUP: &str = "gateway.networking.k8s.io";
const API_VERSION: &str = "gateway.networking.k8s.io/v1";
/// The seed's `gw-http` server.
const HTTP_PORT: u16 = 8082;
/// The seed's `gw-https` server (`global_certificates`).
const HTTPS_PORT: u16 = 8443;
/// No Pingap server listens here.
const UNMAPPED_PORT: u16 = 9999;
const HTTP_SERVER: &str = "gw-http";
/// The address of the `[gateway]` table in tests/e2e/values.yaml.
const ADDRESS: &str = "pingap-k8s-pingap.pingap-k8s.svc";
const STATUS_WAIT: Duration = Duration::from_secs(60);
const TRAFFIC_WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(500);
const SPLIT_REQUESTS: usize = 40;

const GATEWAYS: &str = "gateways.gateway.networking.k8s.io";
const ROUTES: &str = "httproutes.gateway.networking.k8s.io";
const GRANTS: &str = "referencegrants.gateway.networking.k8s.io";
const CLASSES: &str = "gatewayclasses.gateway.networking.k8s.io";

// ---------------------------------------------------------------------------------------
// Objects: apply, get, delete
// ---------------------------------------------------------------------------------------

/// An object a test creates, for the cleanup.
#[derive(Clone, Debug)]
struct Obj {
    /// Resource name for kubectl, e.g. [`GATEWAYS`] or "secret".
    kind: &'static str,
    namespace: Option<String>,
    name: String,
}

fn obj(kind: &'static str, namespace: &str, name: &str) -> Obj {
    Obj {
        kind,
        namespace: Some(namespace.into()),
        name: name.into(),
    }
}

fn cluster_obj(kind: &'static str, name: &str) -> Obj {
    Obj {
        kind,
        namespace: None,
        name: name.into(),
    }
}

/// `kubectl apply` of one object or a `List` (JSON).
async fn apply(env: &Env, doc: &Value) -> anyhow::Result<()> {
    let body = serde_json::to_vec(doc)?;
    env.kubectl
        .run_stdin(&["apply", "-f", "-"], Some(&body))
        .await?;
    Ok(())
}

fn list(items: Vec<Value>) -> Value {
    json!({ "apiVersion": "v1", "kind": "List", "items": items })
}

async fn get_obj(
    kubectl: &Kubectl,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> anyhow::Result<Option<Value>> {
    let mut args = vec!["get", kind, name, "-o", "json"];
    if let Some(ns) = namespace {
        args.extend(["-n", ns]);
    }
    let out = kubectl.output(&args, None).await?;
    if !out.success {
        if out.stderr.contains("NotFound") {
            return Ok(None);
        }
        anyhow::bail!("kubectl get {kind} {name}: {}", out.stderr.trim());
    }
    Ok(Some(serde_json::from_str(&out.stdout)?))
}

/// Deletes the objects, last first, and waits until each one is gone.
async fn delete_all(env: &Env, objs: &[Obj]) -> anyhow::Result<()> {
    for o in objs.iter().rev() {
        let mut args = vec![
            "delete",
            o.kind,
            o.name.as_str(),
            "--ignore-not-found",
            "--wait=true",
            "--timeout=180s",
        ];
        if let Some(ns) = &o.namespace {
            args.extend(["-n", ns.as_str()]);
        }
        env.kubectl.run(&args).await?;
    }
    Ok(())
}

/// Deletes leftovers of an aborted run, runs `body`, then deletes the objects again,
/// whether or not `body` panicked.
async fn with_objects<B>(env: &Env, objs: &[Obj], body: B)
where
    B: std::future::Future<Output = ()>,
{
    delete_all(env, objs).await.expect("delete leftovers");
    guarded(body, delete_all(env, objs)).await;
}

/// The test can start: the controller has a leader and the class is accepted.
async fn ready(env: &Env) {
    controller::wait_leader(env, Duration::from_secs(60))
        .await
        .expect("a controller leader");
    wait_obj(
        env,
        "GatewayClass pingap accepted",
        CLASSES,
        None,
        CLASS,
        |c| cond_is(&c["status"]["conditions"], "Accepted", "True", None),
    )
    .await
    .unwrap();
}

// ---------------------------------------------------------------------------------------
// Object builders
// ---------------------------------------------------------------------------------------

fn gateway(ns: &str, name: &str, listeners: Vec<Value>) -> Value {
    gateway_of(ns, name, CLASS, listeners)
}

fn gateway_of(ns: &str, name: &str, class: &str, listeners: Vec<Value>) -> Value {
    json!({
        "apiVersion": API_VERSION,
        "kind": "Gateway",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "gatewayClassName": class, "listeners": listeners },
    })
}

fn http_listener(name: &str, host: &str) -> Value {
    json!({ "name": name, "port": HTTP_PORT, "protocol": "HTTP", "hostname": host })
}

fn https_listener(name: &str, host: &str, secret: &str) -> Value {
    json!({
        "name": name, "port": HTTPS_PORT, "protocol": "HTTPS", "hostname": host,
        "tls": { "mode": "Terminate", "certificateRefs": [{ "kind": "Secret", "name": secret }] },
    })
}

fn route(ns: &str, name: &str, parents: Value, hostnames: &[&str], rules: Value) -> Value {
    json!({
        "apiVersion": API_VERSION,
        "kind": "HTTPRoute",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "parentRefs": parents, "hostnames": hostnames, "rules": rules },
    })
}

fn backend(name: &str, weight: Option<u32>) -> Value {
    let mut b = json!({ "name": name, "port": 80 });
    if let Some(w) = weight {
        b["weight"] = json!(w);
    }
    b
}

fn prefix(path: &str) -> Value {
    json!({ "path": { "type": "PathPrefix", "value": path } })
}

fn exact(path: &str) -> Value {
    json!({ "path": { "type": "Exact", "value": path } })
}

/// A ResponseHeaderModifier that names the rule in `X-Rule`.
fn tag(rule: &str) -> Value {
    json!({ "type": "ResponseHeaderModifier",
            "responseHeaderModifier": { "set": [{ "name": "X-Rule", "value": rule }] } })
}

// ---------------------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------------------

fn cond<'a>(conds: &'a Value, type_: &str) -> Option<&'a Value> {
    conds.as_array()?.iter().find(|c| c["type"] == type_)
}

/// Condition `type_` has `status` (and `reason`, when given).
fn cond_is(conds: &Value, type_: &str, status: &str, reason: Option<&str>) -> bool {
    cond(conds, type_)
        .is_some_and(|c| c["status"] == status && reason.is_none_or(|r| c["reason"] == r))
}

/// Every condition carries the object's current generation.
fn current(obj: &Value, conds: &Value) -> bool {
    let generation = &obj["metadata"]["generation"];
    conds
        .as_array()
        .is_some_and(|a| !a.is_empty() && a.iter().all(|c| &c["observedGeneration"] == generation))
}

/// "Accepted=True/Accepted, ..." for the logs.
fn show(conds: &Value) -> String {
    conds
        .as_array()
        .map(|a| {
            a.iter()
                .map(|c| {
                    format!(
                        "{}={}/{}",
                        c["type"].as_str().unwrap_or("?"),
                        c["status"].as_str().unwrap_or("?"),
                        c["reason"].as_str().unwrap_or("?")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "-".into())
}

/// Polls the object until `check` holds; returns it. On timeout the error carries the last
/// status seen.
async fn wait_obj(
    env: &Env,
    what: &str,
    kind: &str,
    namespace: Option<&str>,
    name: &str,
    check: impl Fn(&Value) -> bool,
) -> anyhow::Result<Value> {
    wait_for(what, STATUS_WAIT, POLL, || async {
        match get_obj(&env.kubectl, kind, namespace, name).await? {
            Some(v) if check(&v) => Ok(Some(v)),
            Some(v) => anyhow::bail!("last status: {}", v["status"]),
            None => anyhow::bail!("{kind} {name} does not exist"),
        }
    })
    .await
}

fn listener<'a>(gw: &'a Value, name: &str) -> Option<&'a Value> {
    gw["status"]["listeners"]
        .as_array()?
        .iter()
        .find(|l| l["name"] == name)
}

fn listener_cond_is(gw: &Value, name: &str, type_: &str, status: &str, reason: &str) -> bool {
    listener(gw, name).is_some_and(|l| cond_is(&l["conditions"], type_, status, Some(reason)))
}

fn attached(gw: &Value, name: &str) -> Option<i64> {
    listener(gw, name)?["attachedRoutes"].as_i64()
}

/// The `status.parents` entry our controller wrote for `gateway`.
fn our_parent<'a>(route: &'a Value, gateway: &str) -> Option<&'a Value> {
    route["status"]["parents"]
        .as_array()?
        .iter()
        .find(|p| p["controllerName"] == CONTROLLER_NAME && p["parentRef"]["name"] == gateway)
}

/// Waits until our parent entry for `gateway` is current and has every
/// `(type, status, reason)`.
async fn wait_route(
    env: &Env,
    ns: &str,
    name: &str,
    gateway: &str,
    want: &[(&str, &str, &str)],
) -> Value {
    let what = format!("HTTPRoute {ns}/{name} on {gateway}: {want:?}");
    wait_obj(env, &what, ROUTES, Some(ns), name, |r| {
        our_parent(r, gateway).is_some_and(|p| {
            current(r, &p["conditions"])
                && want
                    .iter()
                    .all(|(t, s, reason)| cond_is(&p["conditions"], t, s, Some(reason)))
        })
    })
    .await
    .unwrap()
}

async fn wait_gateway(
    env: &Env,
    ns: &str,
    name: &str,
    what: &str,
    check: impl Fn(&Value) -> bool,
) -> Value {
    wait_obj(env, what, GATEWAYS, Some(ns), name, |g| {
        current(g, &g["status"]["conditions"]) && check(g)
    })
    .await
    .unwrap()
}

// ---------------------------------------------------------------------------------------
// Traffic through the Gateway listeners
// ---------------------------------------------------------------------------------------

/// One HTTP response as curl -i printed it. `status` is 0 without a response.
#[derive(Debug, Clone, Default)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }

    fn short(&self) -> String {
        let body: String = self.body.chars().take(120).collect();
        format!(
            "{} X-Rule={:?} Location={:?} body={body:?}",
            self.status,
            self.header("X-Rule"),
            self.header("Location")
        )
    }
}

/// curl ran with `-i -w '\n%{http_code}'`: status line and headers, a blank line, the
/// body, then the code on the last line.
fn parse_response(stdout: &str) -> Option<Resp> {
    let (rest, code) = stdout.rsplit_once('\n')?;
    let status = code.trim().parse().ok()?;
    let (head, body) = rest.split_once("\r\n\r\n").unwrap_or((rest, ""));
    let headers = head
        .lines()
        .skip(1)
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    Some(Resp {
        status,
        headers,
        body: body.to_string(),
    })
}

/// Requests from the client pod to a Pingap pod's Gateway listeners.
struct Front {
    kubectl: Kubectl,
    namespace: String,
    pod: String,
    /// The Pingap pod IP, bracketed when IPv6.
    ip: String,
}

fn scheme(port: u16) -> &'static str {
    if port == HTTPS_PORT { "https" } else { "http" }
}

impl Front {
    async fn new(env: &Env) -> anyhow::Result<Front> {
        let release = controller::release(env);
        let pods = k8s::pods(&env.client, &env.namespace, &k8s::pingap_selector(&release)).await?;
        let ip = pods
            .iter()
            .find(|p| p.ready && !p.terminating)
            .and_then(|p| p.ip.clone())
            .ok_or_else(|| anyhow::anyhow!("no ready Pingap pod: {pods:?}"))?;
        Ok(Front {
            kubectl: env.kubectl.clone(),
            namespace: env.pingap.client_namespace.clone(),
            pod: env.pingap.client_pod.clone(),
            ip: if ip.contains(':') {
                format!("[{ip}]")
            } else {
                ip
            },
        })
    }

    fn resolve(&self, host: &str, port: u16) -> String {
        format!("{host}:{port}:{}", self.ip)
    }

    /// GET `<scheme>://<host>:<port><path>` (https on [`HTTPS_PORT`], certificate not
    /// verified) with extra headers ("Name: value").
    async fn get(
        &self,
        port: u16,
        host: &str,
        path: &str,
        headers: &[&str],
    ) -> anyhow::Result<Resp> {
        let mut cmd: Vec<String> = [
            "curl",
            "-s",
            "-i",
            "-k",
            "-m",
            "5",
            "-w",
            "\n%{http_code}",
            "--resolve",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        cmd.push(self.resolve(host, port));
        for h in headers {
            cmd.push("-H".into());
            cmd.push(h.to_string());
        }
        cmd.push(format!("{}://{host}:{port}{path}", scheme(port)));
        let out = self
            .kubectl
            .exec(&self.namespace, &self.pod, None, &cmd)
            .await?;
        parse_response(&out.stdout).ok_or_else(|| {
            anyhow::anyhow!("unexpected curl output {:?} ({:?})", out.stdout, out.stderr)
        })
    }

    /// Polls a GET until `check` holds.
    async fn wait(
        &self,
        what: &str,
        port: u16,
        host: &str,
        path: &str,
        headers: &[&str],
        check: impl Fn(&Resp) -> bool,
    ) -> Resp {
        wait_for(what, TRAFFIC_WAIT, POLL, || async {
            let r = self.get(port, host, path, headers).await?;
            if check(&r) {
                Ok(Some(r))
            } else {
                anyhow::bail!("last response: {}", r.short())
            }
        })
        .await
        .unwrap()
    }

    /// `n` sequential GETs in one exec: `(status, first 200 bytes of the body without
    /// spaces and newlines)` each.
    async fn sample(
        &self,
        port: u16,
        host: &str,
        path: &str,
        n: usize,
    ) -> anyhow::Result<Vec<(u16, String)>> {
        let script = format!(
            "i=0; while [ $i -lt {n} ]; do \
               code=$(curl -s -k -m 5 --resolve '{resolve}' -o /tmp/e2e-gw-body -w '%{{http_code}}' '{scheme}://{host}:{port}{path}'); \
               b=$(head -c 200 /tmp/e2e-gw-body | tr -d '\\n '); echo \"$code ${{b:--}}\"; i=$((i+1)); \
             done",
            resolve = self.resolve(host, port),
            scheme = scheme(port),
        );
        let out = self
            .kubectl
            .exec(&self.namespace, &self.pod, None, &["sh", "-c", &script])
            .await?;
        if !out.success {
            anyhow::bail!("sample loop failed: {}", out.stderr.trim());
        }
        Ok(out
            .stdout
            .lines()
            .filter_map(|l| {
                let (code, body) = l.trim().split_once(' ')?;
                Some((code.parse().ok()?, body.to_string()))
            })
            .collect())
    }

    /// Subject and serial number of the certificate served for `host` (SNI), `None`
    /// without a TLS handshake.
    async fn certificate(&self, host: &str) -> anyhow::Result<Option<(String, String)>> {
        let cmd = [
            "curl".to_string(),
            "-s".into(),
            "-k".into(),
            "-m".into(),
            "5".into(),
            "-o".into(),
            "/dev/null".into(),
            "-w".into(),
            "%{certs}".into(),
            "--resolve".into(),
            self.resolve(host, HTTPS_PORT),
            format!("https://{host}:{HTTPS_PORT}/"),
        ];
        let out = self
            .kubectl
            .exec(&self.namespace, &self.pod, None, &cmd)
            .await?;
        let field = |name: &str| {
            out.stdout
                .lines()
                .find_map(|l| l.strip_prefix(name).map(|v| v.trim().to_string()))
        };
        Ok(match (field("Subject:"), field("Serial Number:")) {
            (Some(subject), Some(serial)) => Some((subject, serial)),
            _ => None,
        })
    }
}

/// Counts the responses of a sample by backend: agnhost (echo) answers its pod name on
/// `/hostname` and `NOW: <time>` on other paths, go-httpbin answers `/hostname` with
/// `{"hostname":"go-httpbin"}`.
fn by_backend(sample: &[(u16, String)]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for (code, body) in sample {
        let kind = match (code, body) {
            (200, b) if b.starts_with("echo-") || b.starts_with("NOW:") => "echo".to_string(),
            (200, b) if b.contains("go-httpbin") => "httpbin".to_string(),
            (200, _) => "other-200".to_string(),
            (code, _) => code.to_string(),
        };
        *out.entry(kind).or_default() += 1;
    }
    out
}

/// All values of request header `name` in a go-httpbin `/anything` answer.
fn httpbin_values(answer: &Value, name: &str) -> Vec<String> {
    answer["headers"]
        .as_object()
        .and_then(|h| h.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)))
        .map(|(_, v)| match v {
            Value::Array(a) => a
                .iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect(),
            Value::String(s) => vec![s.clone()],
            _ => Vec::new(),
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------------------
// etcd
// ---------------------------------------------------------------------------------------

/// The generated locations of HTTPRoute `ns/name` (found by their `remark`).
async fn route_locations(env: &Env, ns: &str, name: &str) -> anyhow::Result<Vec<String>> {
    let remark = format!("remark = \"pingap-k8s httproute {ns}/{name}\"");
    let dir = format!("{}/locations/", env.etcd.prefix());
    Ok(env
        .etcd
        .get_prefix(&format!("{dir}k8s-"))
        .await?
        .into_iter()
        .filter(|kv| kv.value.lines().any(|l| l.trim() == remark))
        .filter_map(|kv| {
            kv.key
                .strip_prefix(&dir)
                .and_then(|k| k.strip_suffix(".toml"))
                .map(String::from)
        })
        .collect())
}

/// `servers.<name>.locations` as stored.
async fn server_locations(env: &Env, server: &str) -> anyhow::Result<Vec<String>> {
    let key = format!("{}/servers/{server}.toml", env.etcd.prefix());
    let kv = env
        .etcd
        .get(&key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("{key} missing"))?;
    let table: toml::Table = toml::from_str(&kv.value)?;
    Ok(table["servers"][server]
        .get("locations")
        .and_then(|l| l.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default())
}

/// The keys of the generated certificates of Secret `ns/name`.
async fn secret_certificates(env: &Env, ns: &str, name: &str) -> anyhow::Result<Vec<String>> {
    let remark = format!("remark = \"pingap-k8s secret {ns}/{name}\"");
    Ok(env
        .etcd
        .get_prefix(&format!("{}/certificates/k8s-", env.etcd.prefix()))
        .await?
        .into_iter()
        .filter(|kv| kv.value.lines().any(|l| l.trim() == remark))
        .map(|kv| kv.key)
        .collect())
}

// ---------------------------------------------------------------------------------------
// TLS Secrets
// ---------------------------------------------------------------------------------------

/// Creates or replaces `kubernetes.io/tls` Secret `ns/name` with a new self-signed
/// certificate for `host` (openssl on the test host; the key never leaves the test host
/// and the cluster). Every call makes a new key and serial number.
async fn apply_tls_secret(env: &Env, ns: &str, name: &str, host: &str) -> anyhow::Result<()> {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_nanos();
    let dir: PathBuf =
        std::env::temp_dir().join(format!("pk8s-e2e-{name}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let cert = dir.join("tls.crt").display().to_string();
    let key = dir.join("tls.key").display().to_string();
    let result = async {
        host_cmd(
            "openssl",
            [
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-keyout",
                key.as_str(),
                "-out",
                cert.as_str(),
                "-subj",
                format!("/CN={host}").as_str(),
                "-addext",
                format!("subjectAltName=DNS:{host}").as_str(),
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        )
        .await?;
        let doc = env
            .kubectl
            .run(&[
                "create",
                "secret",
                "tls",
                name,
                "-n",
                ns,
                "--cert",
                cert.as_str(),
                "--key",
                key.as_str(),
                "--dry-run=client",
                "-o",
                "json",
            ])
            .await?;
        env.kubectl
            .run_stdin(&["apply", "-f", "-"], Some(doc.as_bytes()))
            .await?;
        anyhow::Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&dir);
    result
}

// ---------------------------------------------------------------------------------------
// 19. GatewayClass status
// ---------------------------------------------------------------------------------------

/// The chart's GatewayClass `pingap` is accepted for the current generation, the CRDs
/// are of the supported version, and `supportedFeatures` lists the features the
/// provider implements (sorted, as the API requires), among them HTTPRoute and
/// ReferenceGrant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_19_gateway_class_status() {
    const T: &str = "e2e_19";
    let Some(env) = Env::setup("e2e_19_gateway_class_status").await else {
        return;
    };
    controller::wait_leader(&env, Duration::from_secs(60))
        .await
        .unwrap();
    let class = wait_obj(
        &env,
        "GatewayClass pingap status",
        CLASSES,
        None,
        CLASS,
        |c| {
            let conds = &c["status"]["conditions"];
            current(c, conds)
                && cond_is(conds, "Accepted", "True", Some("Accepted"))
                && cond_is(conds, "SupportedVersion", "True", Some("SupportedVersion"))
        },
    )
    .await
    .unwrap();
    assert_eq!(class["spec"]["controllerName"], CONTROLLER_NAME);
    println!("[{T}] conditions: {}", show(&class["status"]["conditions"]));
    let features: Vec<&str> = class["status"]["supportedFeatures"]
        .as_array()
        .expect("supportedFeatures")
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    println!("[{T}] supportedFeatures: {features:?}");
    for want in ["Gateway", "HTTPRoute", "ReferenceGrant"] {
        assert!(features.contains(&want), "{want} missing from {features:?}");
    }
    let mut sorted = features.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        features, sorted,
        "supportedFeatures must be sorted and unique"
    );
}

// ---------------------------------------------------------------------------------------
// 20. Gateway status: conditions, addresses, listeners, attachedRoutes, conflicts
// ---------------------------------------------------------------------------------------

/// Gateway `t20` has an HTTP listener on 8082, an HTTPS listener on 8443 and two invalid
/// ones (plain HTTP on 9999 and HTTPS on 8082: no Pingap server of that kind listens
/// there). Route `t20-a` attaches to listener `http` by sectionName, `t20-b` to the whole
/// Gateway. The Gateway is accepted (reason ListenersNotValid, as some listeners are
/// invalid) and programmed, its address is the configured one, the valid listeners are
/// accepted and programmed with the right attachedRoutes, the invalid ones get
/// Accepted=False/PortUnavailable. A newer Gateway with the same port and hostname is
/// merged onto the same server: both listeners are programmed and not conflicted.
/// Without the invalid listeners the Gateway is accepted with reason Accepted for the
/// new generation.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_20_gateway_status() {
    const T: &str = "e2e_20";
    let Some(env) = Env::setup("e2e_20_gateway_status").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t20.example.com";
    let objs = [
        obj(GATEWAYS, &ns, "t20"),
        obj(GATEWAYS, &ns, "t20-dup"),
        obj(ROUTES, &ns, "t20-a"),
        obj(ROUTES, &ns, "t20-b"),
        obj("secret", &ns, "t20-tls"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        apply_tls_secret(&env, &ns, "t20-tls", host).await.unwrap();
        let valid = vec![http_listener("http", host), https_listener("https", host, "t20-tls")];
        let mut listeners = valid.clone();
        listeners.push(json!({ "name": "bad-port", "port": UNMAPPED_PORT, "protocol": "HTTP", "hostname": host }));
        listeners.push(json!({
            "name": "bad-tls", "port": HTTP_PORT, "protocol": "HTTPS", "hostname": "tls.t20.example.com",
            "tls": { "mode": "Terminate", "certificateRefs": [{ "name": "t20-tls" }] },
        }));
        let rules = json!([{ "backendRefs": [backend("echo", None)] }]);
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t20", listeners),
                route(&ns, "t20-a", json!([{ "name": "t20", "sectionName": "http" }]), &[host], rules.clone()),
                route(&ns, "t20-b", json!([{ "name": "t20" }]), &[host], rules.clone()),
            ]),
        )
        .await
        .unwrap();

        let gw = wait_gateway(&env, &ns, "t20", "Gateway t20 status with attached routes", |g| {
            let c = &g["status"]["conditions"];
            cond_is(c, "Accepted", "True", Some("ListenersNotValid"))
                && cond_is(c, "Programmed", "True", Some("Programmed"))
                && attached(g, "http") == Some(2)
                && attached(g, "https") == Some(1)
        })
        .await;
        println!("[{T}] Gateway: {}", show(&gw["status"]["conditions"]));
        for l in gw["status"]["listeners"].as_array().unwrap() {
            println!(
                "[{T}] listener {}: attachedRoutes {} {}",
                l["name"], l["attachedRoutes"], show(&l["conditions"])
            );
        }
        assert_eq!(
            gw["status"]["addresses"],
            json!([{ "type": "Hostname", "value": ADDRESS }])
        );
        assert_eq!(gw["status"]["listeners"].as_array().unwrap().len(), 4);
        for name in ["http", "https"] {
            for (t, s, r) in [
                ("Accepted", "True", "Accepted"),
                ("Programmed", "True", "Programmed"),
                ("ResolvedRefs", "True", "ResolvedRefs"),
                ("Conflicted", "False", "NoConflicts"),
            ] {
                assert!(listener_cond_is(&gw, name, t, s, r), "listener {name}: {t}={s}/{r}");
            }
            // gw-http sets enabled_h2 (tests/e2e/values.yaml), so its listeners also
            // take GRPCRoutes; gw-https does not.
            let kinds = if name == "http" {
                json!([{ "group": GROUP, "kind": "HTTPRoute" }, { "group": GROUP, "kind": "GRPCRoute" }])
            } else {
                json!([{ "group": GROUP, "kind": "HTTPRoute" }])
            };
            assert_eq!(listener(&gw, name).unwrap()["supportedKinds"], kinds);
        }
        for name in ["bad-port", "bad-tls"] {
            assert!(
                listener_cond_is(&gw, name, "Accepted", "False", "PortUnavailable"),
                "listener {name}: {}",
                show(&listener(&gw, name).unwrap()["conditions"])
            );
            assert!(
                listener(&gw, name).is_some_and(|l| cond_is(&l["conditions"], "Programmed", "False", None)),
                "listener {name} programmed"
            );
        }
        for r in ["t20-a", "t20-b"] {
            wait_route(&env, &ns, r, "t20", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        }

        // A newer Gateway with the same port and hostname: merged onto the same server,
        // both listeners programmed, neither conflicted.
        apply(&env, &gateway(&ns, "t20-dup", vec![http_listener("http", host)]))
            .await
            .unwrap();
        let dup = wait_gateway(&env, &ns, "t20-dup", "Gateway t20-dup merged", |g| {
            listener_cond_is(g, "http", "Conflicted", "False", "NoConflicts")
                && listener_cond_is(g, "http", "Programmed", "True", "Programmed")
        })
        .await;
        println!(
            "[{T}] t20-dup: {}; listener http: {}",
            show(&dup["status"]["conditions"]),
            show(&listener(&dup, "http").unwrap()["conditions"])
        );
        assert!(cond_is(&dup["status"]["conditions"], "Accepted", "True", Some("Accepted")));
        let gw = get_obj(&env.kubectl, GATEWAYS, Some(&ns), "t20").await.unwrap().unwrap();
        assert!(listener_cond_is(&gw, "http", "Conflicted", "False", "NoConflicts"));
        assert!(listener_cond_is(&gw, "http", "Programmed", "True", "Programmed"));
        delete_all(&env, &[obj(GATEWAYS, &ns, "t20-dup")]).await.unwrap();

        // Only the valid listeners: accepted with reason Accepted, for generation 2.
        apply(&env, &gateway(&ns, "t20", valid)).await.unwrap();
        let gw = wait_gateway(&env, &ns, "t20", "Gateway t20 fully valid", |g| {
            cond_is(&g["status"]["conditions"], "Accepted", "True", Some("Accepted"))
                && g["status"]["listeners"].as_array().is_some_and(|l| l.len() == 2)
        })
        .await;
        assert_eq!(gw["metadata"]["generation"], 2);
        assert_eq!((attached(&gw, "http"), attached(&gw, "https")), (Some(2), Some(1)));
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 21. Routing: hosts, paths, headers, query, precedence, deletion
// ---------------------------------------------------------------------------------------

/// Gateway `t21` listens for `*.t21.example.com`. Route `t21-a` (host
/// `a.t21.example.com`) has rules for PathPrefix /foo, PathPrefix /foo/bar, Exact /foo,
/// PathPrefix /foo with a header, PathPrefix /q with a query parameter; route `t21-b`
/// (host `b.t21.example.com`) one catch-all rule. Every rule names itself in the
/// response header `X-Rule` (backend echo). The rule that answers must follow the
/// Gateway API: segment-aware prefixes, Exact before prefix, the longer prefix first, a
/// header match before a plain one; other hosts get Pingap's 404. Deleting `t21-a`
/// removes its generated locations from etcd and from server gw-http, and its host
/// gets 404 again, while `t21-b` keeps working.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_21_httproute_matching() {
    const T: &str = "e2e_21";
    let Some(env) = Env::setup("e2e_21_httproute_matching").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let (a, b) = ("a.t21.example.com", "b.t21.example.com");
    let objs = [
        obj(GATEWAYS, &ns, "t21"),
        obj(ROUTES, &ns, "t21-a"),
        obj(ROUTES, &ns, "t21-b"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let echo = json!([backend("echo", None)]);
        let rules_a = json!([
            { "matches": [prefix("/foo")], "filters": [tag("prefix-foo")], "backendRefs": echo },
            { "matches": [prefix("/foo/bar")], "filters": [tag("prefix-foo-bar")], "backendRefs": echo },
            { "matches": [exact("/foo")], "filters": [tag("exact-foo")], "backendRefs": echo },
            { "matches": [{ "path": { "type": "PathPrefix", "value": "/foo" },
                            "headers": [{ "name": "X-Variant", "value": "blue" }] }],
              "filters": [tag("header")], "backendRefs": echo },
            { "matches": [{ "path": { "type": "PathPrefix", "value": "/q" },
                            "queryParams": [{ "name": "v", "value": "1" }] }],
              "filters": [tag("query")], "backendRefs": echo },
        ]);
        let rules_b = json!([{ "filters": [tag("b-root")], "backendRefs": echo }]);
        let parent = json!([{ "name": "t21" }]);
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t21", vec![http_listener("http", "*.t21.example.com")]),
                route(&ns, "t21-a", parent.clone(), &[a], rules_a),
                route(&ns, "t21-b", parent.clone(), &[b], rules_b),
            ]),
        )
        .await
        .unwrap();
        for r in ["t21-a", "t21-b"] {
            wait_route(&env, &ns, r, "t21", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        }
        let front = Front::new(&env).await.unwrap();
        front
            .wait("route t21-a served", HTTP_PORT, a, "/foo/x", &[], |r| {
                r.header("X-Rule") == Some("prefix-foo")
            })
            .await;
        front
            .wait("route t21-b served", HTTP_PORT, b, "/anything", &[], |r| {
                r.header("X-Rule") == Some("b-root")
            })
            .await;

        // (host, path, headers, expected X-Rule; None = Pingap's 404).
        let cases: &[(&str, &str, &[&str], Option<&str>)] = &[
            (a, "/foo", &[], Some("exact-foo")),
            (a, "/foo/", &[], Some("prefix-foo")),
            (a, "/foo/x", &[], Some("prefix-foo")),
            (a, "/foo/bar", &[], Some("prefix-foo-bar")),
            (a, "/foo/bar/baz", &[], Some("prefix-foo-bar")),
            (a, "/foo/barbaz", &[], Some("prefix-foo")),
            (a, "/foobar", &[], None),
            (a, "/fo", &[], None),
            (a, "/", &[], None),
            (a, "/foo/x", &["X-Variant: blue"], Some("header")),
            (a, "/foo/x", &["X-Variant: red"], Some("prefix-foo")),
            // Exact path ranks before header matches.
            (a, "/foo", &["X-Variant: blue"], Some("exact-foo")),
            (a, "/q?v=1", &[], Some("query")),
            (a, "/q/x?a=b&v=1", &[], Some("query")),
            (a, "/q?v=2", &[], None),
            (a, "/q", &[], None),
            (b, "/foo", &[], Some("b-root")),
            (b, "/", &[], Some("b-root")),
            ("c.t21.example.com", "/foo", &[], None),
            ("t21.example.com", "/foo", &[], None),
        ];
        let mut wrong = Vec::new();
        for (host, path, headers, want) in cases {
            let r = front.get(HTTP_PORT, host, path, headers).await.unwrap();
            let got = r.header("X-Rule");
            let ok = match want {
                Some(rule) => got == Some(*rule) && r.status == 200,
                None => got.is_none() && r.status == 404,
            };
            println!("[{T}] {host}{path} {headers:?}: {} {got:?}", r.status);
            if !ok {
                wrong.push(format!("{host}{path} {headers:?}: want {want:?}, got {}", r.short()));
            }
        }
        assert!(wrong.is_empty(), "wrong routing:\n{}", wrong.join("\n"));

        // Deleting route t21-a removes its locations.
        let locations = route_locations(&env, &ns, "t21-a").await.unwrap();
        println!("[{T}] t21-a locations: {locations:?}");
        assert_eq!(locations.len(), 5, "one location per match: {locations:?}");
        let listed = server_locations(&env, HTTP_SERVER).await.unwrap();
        for l in &locations {
            assert!(listed.contains(l), "{l} not in servers.{HTTP_SERVER}.locations {listed:?}");
        }
        delete_all(&env, &[obj(ROUTES, &ns, "t21-a")]).await.unwrap();
        wait_for("t21-a locations gone", STATUS_WAIT, POLL, || async {
            let left = route_locations(&env, &ns, "t21-a").await?;
            let listed = server_locations(&env, HTTP_SERVER).await?;
            let still: Vec<&String> = locations.iter().filter(|l| listed.contains(l)).collect();
            Ok((left.is_empty() && still.is_empty()).then_some(()))
        })
        .await
        .unwrap();
        front
            .wait("a.t21 back to 404", HTTP_PORT, a, "/foo/x", &[], |r| {
                r.status == 404 && r.header("X-Rule").is_none()
            })
            .await;
        let r = front.get(HTTP_PORT, b, "/foo", &[]).await.unwrap();
        assert_eq!((r.status, r.header("X-Rule")), (200, Some("b-root")), "{}", r.short());
        let gw = wait_gateway(&env, &ns, "t21", "attachedRoutes 1", |g| attached(g, "http") == Some(1)).await;
        assert_eq!(attached(&gw, "http"), Some(1));
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 22. Weighted backendRefs
// ---------------------------------------------------------------------------------------

/// Route `t22` splits `/hostname` 1:1 between echo (answers its pod name) and httpbin
/// (answers `{"hostname":"go-httpbin"}`): of 40 requests each side gets 25–75 %. With
/// httpbin at weight 0, it gets none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_22_weighted_backends() {
    const T: &str = "e2e_22";
    let Some(env) = Env::setup("e2e_22_weighted_backends").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t22.example.com";
    let objs = [obj(GATEWAYS, &ns, "t22"), obj(ROUTES, &ns, "t22")];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let split = |echo: u32, httpbin: u32| {
            route(
                &ns,
                "t22",
                json!([{ "name": "t22" }]),
                &[host],
                json!([{ "backendRefs": [backend("echo", Some(echo)), backend("httpbin", Some(httpbin))] }]),
            )
        };
        apply(&env, &list(vec![gateway(&ns, "t22", vec![http_listener("http", host)]), split(1, 1)]))
            .await
            .unwrap();
        wait_route(&env, &ns, "t22", "t22", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        let front = Front::new(&env).await.unwrap();
        front
            .wait("route t22 served", HTTP_PORT, host, "/hostname", &[], |r| r.status == 200)
            .await;
        let sample = front.sample(HTTP_PORT, host, "/hostname", SPLIT_REQUESTS).await.unwrap();
        let counts = by_backend(&sample);
        println!("[{T}] 1:1 over {SPLIT_REQUESTS}: {counts:?}");
        let (lo, hi) = (SPLIT_REQUESTS / 4, SPLIT_REQUESTS * 3 / 4);
        for side in ["echo", "httpbin"] {
            let n = counts.get(side).copied().unwrap_or(0);
            assert!((lo..=hi).contains(&n), "{side}: {n} of {SPLIT_REQUESTS} ({counts:?})");
        }
        assert_eq!(counts.values().sum::<usize>(), SPLIT_REQUESTS);

        // Weight 0: httpbin gets nothing.
        apply(&env, &split(1, 0)).await.unwrap();
        wait_route(&env, &ns, "t22", "t22", &[("Accepted", "True", "Accepted")]).await;
        wait_for("only echo answers", TRAFFIC_WAIT, POLL, || async {
            let s = front.sample(HTTP_PORT, host, "/hostname", 10).await?;
            Ok((by_backend(&s).get("echo") == Some(&10)).then_some(()))
        })
        .await
        .unwrap();
        let sample = front.sample(HTTP_PORT, host, "/hostname", SPLIT_REQUESTS).await.unwrap();
        let counts = by_backend(&sample);
        println!("[{T}] 1:0 over {SPLIT_REQUESTS}: {counts:?}");
        assert_eq!(counts.get("echo"), Some(&SPLIT_REQUESTS), "{counts:?}");
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 23. Invalid backends
// ---------------------------------------------------------------------------------------

/// Route `t23` sends `/` to the missing Service `t23-backend` and `/mixed` 1:1 to echo
/// and that Service: the route is accepted with ResolvedRefs=False/BackendNotFound, `/`
/// answers 500 and `/mixed` both 200 and 500. Once Service `t23-backend` (selecting the
/// echo pods) exists, ResolvedRefs is True and both answer 200 only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_23_invalid_backend() {
    const T: &str = "e2e_23";
    let Some(env) = Env::setup("e2e_23_invalid_backend").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t23.example.com";
    let objs = [
        obj(GATEWAYS, &ns, "t23"),
        obj(ROUTES, &ns, "t23"),
        obj("service", &ns, "t23-backend"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t23", vec![http_listener("http", host)]),
                route(
                    &ns,
                    "t23",
                    json!([{ "name": "t23" }]),
                    &[host],
                    json!([
                        { "matches": [prefix("/mixed")],
                          "backendRefs": [backend("echo", Some(1)), backend("t23-backend", Some(1))] },
                        { "backendRefs": [backend("t23-backend", None)] },
                    ]),
                ),
            ]),
        )
        .await
        .unwrap();
        let r = wait_route(&env, &ns, "t23", "t23", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "False", "BackendNotFound")]).await;
        println!("[{T}] route: {}", show(&our_parent(&r, "t23").unwrap()["conditions"]));
        let front = Front::new(&env).await.unwrap();
        front
            .wait("500 for the missing backend", HTTP_PORT, host, "/hostname", &[], |r| r.status == 500)
            .await;
        // The echo upstream of /mixed may be written after the 500 of the catch-all rule.
        front
            .wait("/mixed reaches echo", HTTP_PORT, host, "/mixed/hostname", &[], |r| r.status == 200)
            .await;
        let sample = front.sample(HTTP_PORT, host, "/mixed/hostname", SPLIT_REQUESTS).await.unwrap();
        let counts = by_backend(&sample);
        println!("[{T}] /mixed with the Service missing: {counts:?}");
        let errors = counts.get("500").copied().unwrap_or(0);
        let echo = counts.get("echo").copied().unwrap_or(0);
        assert!(errors > 0 && echo > 0, "want both 500 and echo: {counts:?}");
        assert_eq!(errors + echo, SPLIT_REQUESTS, "{counts:?}");

        apply(
            &env,
            &json!({
                "apiVersion": "v1", "kind": "Service",
                "metadata": { "name": "t23-backend", "namespace": ns },
                "spec": { "selector": { "app": "echo" },
                          "ports": [{ "name": "http", "port": 80, "targetPort": "http" }] },
            }),
        )
        .await
        .unwrap();
        wait_route(&env, &ns, "t23", "t23", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        front
            .wait("200 once the Service exists", HTTP_PORT, host, "/hostname", &[], |r| r.status == 200)
            .await;
        wait_for("/mixed without errors", TRAFFIC_WAIT, POLL, || async {
            let s = front.sample(HTTP_PORT, host, "/mixed/hostname", 10).await?;
            Ok((by_backend(&s).get("echo") == Some(&10)).then_some(()))
        })
        .await
        .unwrap();
        let sample = front.sample(HTTP_PORT, host, "/hostname", 20).await.unwrap();
        let counts = by_backend(&sample);
        println!("[{T}] / with the Service: {counts:?}");
        assert_eq!(counts.get("echo"), Some(&20), "{counts:?}");
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 24. Filters
// ---------------------------------------------------------------------------------------

/// Route `t24` (backend httpbin) exercises every supported filter:
/// RequestHeaderModifier (set replaces the client's value, add appends, remove drops),
/// ResponseHeaderModifier (on go-httpbin's `/response-headers`), RequestRedirect (path,
/// scheme, hostname, port, prefix replacement and the status codes 301/302/303/307/308)
/// and URLRewrite (prefix and full path, hostname).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_24_filters() {
    const T: &str = "e2e_24";
    let Some(env) = Env::setup("e2e_24_filters").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t24.example.com";
    let objs = [obj(GATEWAYS, &ns, "t24"), obj(ROUTES, &ns, "t24")];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let httpbin = json!([backend("httpbin", None)]);
        let redirect = |path: Value, r: Value| json!({ "matches": [path], "filters": [{ "type": "RequestRedirect", "requestRedirect": r }] });
        let rules = json!([
            { "matches": [prefix("/anything/req")],
              "filters": [{ "type": "RequestHeaderModifier", "requestHeaderModifier": {
                  "set": [{ "name": "X-Set", "value": "s1" }],
                  "add": [{ "name": "X-Add", "value": "a1" }],
                  "remove": ["X-Remove"] } }],
              "backendRefs": httpbin },
            { "matches": [prefix("/response-headers")],
              "filters": [{ "type": "ResponseHeaderModifier", "responseHeaderModifier": {
                  "set": [{ "name": "X-Resp", "value": "r1" }],
                  "add": [{ "name": "X-Resp-Add", "value": "a1" }],
                  "remove": ["X-Drop"] } }],
              "backendRefs": httpbin },
            redirect(exact("/redir/path"), json!({ "path": { "type": "ReplaceFullPath", "replaceFullPath": "/new" }, "statusCode": 301 })),
            redirect(exact("/redir/scheme"), json!({ "scheme": "https" })),
            redirect(exact("/redir/host"), json!({ "hostname": "other.example.com", "statusCode": 308 })),
            redirect(exact("/redir/port"), json!({ "port": 9090, "statusCode": 307 })),
            redirect(prefix("/redir/prefix"), json!({ "path": { "type": "ReplacePrefixMatch", "replacePrefixMatch": "/moved" }, "statusCode": 303 })),
            { "matches": [prefix("/rw")],
              "filters": [{ "type": "URLRewrite", "urlRewrite": {
                  "hostname": "rewritten.example.com",
                  "path": { "type": "ReplacePrefixMatch", "replacePrefixMatch": "/anything/rewritten" } } }],
              "backendRefs": httpbin },
            { "matches": [prefix("/full")],
              "filters": [{ "type": "URLRewrite", "urlRewrite": {
                  "path": { "type": "ReplaceFullPath", "replaceFullPath": "/anything/full" } } }],
              "backendRefs": httpbin },
        ]);
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t24", vec![http_listener("http", host)]),
                route(&ns, "t24", json!([{ "name": "t24" }]), &[host], rules),
            ]),
        )
        .await
        .unwrap();
        let r = wait_route(&env, &ns, "t24", "t24", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        let parent = our_parent(&r, "t24").unwrap();
        assert!(cond(&parent["conditions"], "PartiallyInvalid").is_none(), "{}", show(&parent["conditions"]));
        let front = Front::new(&env).await.unwrap();

        // RequestHeaderModifier.
        let r = front
            .wait("route t24 served", HTTP_PORT, host, "/anything/req", &["X-Set: client", "X-Add: c0", "X-Remove: gone", "X-Keep: k"], |r| r.status == 200)
            .await;
        let answer = r.json();
        println!("[{T}] request headers at httpbin: {}", answer["headers"]);
        assert_eq!(httpbin_values(&answer, "X-Set"), vec!["s1"]);
        let mut added = httpbin_values(&answer, "X-Add");
        added.sort();
        assert_eq!(added, vec!["a1", "c0"]);
        assert!(httpbin_values(&answer, "X-Remove").is_empty(), "X-Remove kept: {}", answer["headers"]);
        assert_eq!(httpbin_values(&answer, "X-Keep"), vec!["k"]);

        // ResponseHeaderModifier: go-httpbin sets the response headers named in the query.
        let r = front
            .get(HTTP_PORT, host, "/response-headers?X-Drop=1&X-Resp=backend&X-Other=2", &[])
            .await
            .unwrap();
        println!("[{T}] response headers: {:?}", r.headers);
        assert_eq!(r.status, 200, "{}", r.short());
        assert_eq!(r.header("X-Resp"), Some("r1"));
        assert_eq!(r.header("X-Resp-Add"), Some("a1"));
        assert_eq!(r.header("X-Other"), Some("2"));
        assert_eq!(r.header("X-Drop"), None);

        // RequestRedirect: (path, status, Location). The client connects to the listener
        // port, so the request authority is t24.example.com:8082.
        let redirects: &[(&str, u16, &str)] = &[
            ("/redir/path?q=1", 301, "http://t24.example.com:8082/new?q=1"),
            ("/redir/scheme", 302, "https://t24.example.com/redir/scheme"),
            ("/redir/host", 308, "http://other.example.com:8082/redir/host"),
            ("/redir/port", 307, "http://t24.example.com:9090/redir/port"),
            ("/redir/prefix/a/b", 303, "http://t24.example.com:8082/moved/a/b"),
            ("/redir/prefix", 303, "http://t24.example.com:8082/moved"),
        ];
        let mut wrong = Vec::new();
        for (path, status, location) in redirects {
            let r = front.get(HTTP_PORT, host, path, &[]).await.unwrap();
            println!("[{T}] {path}: {} {:?}", r.status, r.header("Location"));
            if r.status != *status || r.header("Location") != Some(*location) {
                wrong.push(format!("{path}: want {status} {location}, got {}", r.short()));
            }
        }
        assert!(wrong.is_empty(), "wrong redirects:\n{}", wrong.join("\n"));

        // URLRewrite.
        let r = front.get(HTTP_PORT, host, "/rw/x?y=1", &[]).await.unwrap();
        let answer = r.json();
        println!("[{T}] /rw/x?y=1 at httpbin: url {} Host {:?}", answer["url"], httpbin_values(&answer, "Host"));
        assert_eq!(r.status, 200, "{}", r.short());
        assert_eq!(answer["url"], "http://rewritten.example.com/anything/rewritten/x?y=1");
        let r = front.get(HTTP_PORT, host, "/rw", &[]).await.unwrap();
        assert_eq!(r.json()["url"], "http://rewritten.example.com/anything/rewritten", "{}", r.short());
        let r = front.get(HTTP_PORT, host, "/full/deep/path?z=2", &[]).await.unwrap();
        let answer = r.json();
        println!("[{T}] /full/deep/path?z=2 at httpbin: url {}", answer["url"]);
        assert_eq!(r.status, 200, "{}", r.short());
        let url = answer["url"].as_str().unwrap_or_default();
        assert!(url.contains("/anything/full"), "url {url}");
        assert!(!url.contains("deep"), "url {url}");
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 25. HTTPS listeners
// ---------------------------------------------------------------------------------------

/// Gateway `t25` has an HTTPS listener for `t25.example.com` with Secret `t25-tls` (a
/// self-signed certificate): Pingap serves it by SNI (subject CN=t25.example.com) and the
/// route answers. Replacing the Secret's certificate changes the served one. Gateway
/// `t25-missing` names a Secret that does not exist: its listener gets
/// ResolvedRefs=False/InvalidCertificateRef and is not programmed; once the Secret
/// exists the listener resolves and HTTPS works for its host. Deleting the Gateways and
/// Secrets removes the generated certificates from etcd.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_25_https_listener() {
    const T: &str = "e2e_25";
    let Some(env) = Env::setup("e2e_25_https_listener").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let (host, host2) = ("t25.example.com", "missing.t25.example.com");
    let objs = [
        obj(GATEWAYS, &ns, "t25"),
        obj(GATEWAYS, &ns, "t25-missing"),
        obj(ROUTES, &ns, "t25"),
        obj("secret", &ns, "t25-tls"),
        obj("secret", &ns, "t25-late"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        apply_tls_secret(&env, &ns, "t25-tls", host).await.unwrap();
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t25", vec![https_listener("https", host, "t25-tls")]),
                gateway(
                    &ns,
                    "t25-missing",
                    vec![https_listener("https", host2, "t25-late")],
                ),
                route(
                    &ns,
                    "t25",
                    json!([{ "name": "t25" }, { "name": "t25-missing" }]),
                    &[host, host2],
                    json!([{ "backendRefs": [backend("echo", None)] }]),
                ),
            ]),
        )
        .await
        .unwrap();
        let gw = wait_gateway(&env, &ns, "t25", "Gateway t25 programmed", |g| {
            listener_cond_is(g, "https", "Programmed", "True", "Programmed")
                && listener_cond_is(g, "https", "ResolvedRefs", "True", "ResolvedRefs")
        })
        .await;
        println!(
            "[{T}] t25 listener https: {}",
            show(&listener(&gw, "https").unwrap()["conditions"])
        );
        let front = Front::new(&env).await.unwrap();
        let r = front
            .wait(
                "HTTPS through t25",
                HTTPS_PORT,
                host,
                "/hostname",
                &[],
                |r| r.status == 200,
            )
            .await;
        assert!(r.body.starts_with("echo-"), "{}", r.short());
        let (subject, serial1) = front
            .certificate(host)
            .await
            .unwrap()
            .expect("a certificate");
        println!("[{T}] served for {host}: {subject} serial {serial1}");
        assert!(
            subject.contains(&format!("CN = {host}")) || subject.contains(&format!("CN={host}")),
            "{subject}"
        );
        let certs = secret_certificates(&env, &ns, "t25-tls").await.unwrap();
        assert_eq!(certs.len(), 1, "{certs:?}");

        // Rotation: a new certificate in the same Secret.
        apply_tls_secret(&env, &ns, "t25-tls", host).await.unwrap();
        let serial2 = wait_for("the new certificate served", TRAFFIC_WAIT, POLL, || async {
            Ok(front
                .certificate(host)
                .await?
                .map(|(_, s)| s)
                .filter(|s| *s != serial1))
        })
        .await
        .unwrap();
        println!("[{T}] after the rotation: serial {serial2}");
        let r = front.get(HTTPS_PORT, host, "/hostname", &[]).await.unwrap();
        assert_eq!(r.status, 200, "{}", r.short());

        // A missing Secret.
        let gw = wait_gateway(
            &env,
            &ns,
            "t25-missing",
            "listener with a missing Secret",
            |g| listener_cond_is(g, "https", "ResolvedRefs", "False", "InvalidCertificateRef"),
        )
        .await;
        println!(
            "[{T}] t25-missing listener https: {}",
            show(&listener(&gw, "https").unwrap()["conditions"])
        );
        assert!(listener(&gw, "https").is_some_and(|l| cond_is(
            &l["conditions"],
            "Programmed",
            "False",
            None
        )));
        let served = front.certificate(host2).await.unwrap();
        println!("[{T}] served for {host2} without its Secret: {served:?}");
        assert!(
            served.as_ref().is_none_or(|(s, _)| !s.contains(host2)),
            "a certificate for {host2} without a Secret: {served:?}"
        );
        apply_tls_secret(&env, &ns, "t25-late", host2)
            .await
            .unwrap();
        wait_gateway(&env, &ns, "t25-missing", "listener resolved", |g| {
            listener_cond_is(g, "https", "ResolvedRefs", "True", "ResolvedRefs")
                && listener_cond_is(g, "https", "Programmed", "True", "Programmed")
        })
        .await;
        front
            .wait(
                "HTTPS through t25-missing",
                HTTPS_PORT,
                host2,
                "/hostname",
                &[],
                |r| r.status == 200,
            )
            .await;
        let (subject, _) = front
            .certificate(host2)
            .await
            .unwrap()
            .expect("a certificate");
        assert!(subject.contains(host2), "{subject}");

        // Cleanup removes the certificates.
        delete_all(&env, &objs).await.unwrap();
        wait_for(
            "generated certificates removed",
            STATUS_WAIT,
            POLL,
            || async {
                let a = secret_certificates(&env, &ns, "t25-tls").await?;
                let b = secret_certificates(&env, &ns, "t25-late").await?;
                Ok((a.is_empty() && b.is_empty()).then_some(()))
            },
        )
        .await
        .unwrap();
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 26. Cross-namespace routes and ReferenceGrant
// ---------------------------------------------------------------------------------------

/// Route `t26` lives in namespace `e2e-gw-t26` and sends traffic to Service echo in
/// e2e-app through Gateway `t26` in e2e-app. With the listener's default allowedRoutes
/// (Same namespace) the route is not accepted (NotAllowedByListeners). With `from: All`
/// it is accepted, but the backend in another namespace is RefNotPermitted (500) until a
/// ReferenceGrant in e2e-app allows it (200); deleting the grant takes it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_26_cross_namespace_reference_grant() {
    const T: &str = "e2e_26";
    let Some(env) = Env::setup("e2e_26_cross_namespace_reference_grant").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let route_ns = "e2e-gw-t26";
    let host = "t26.example.com";
    let objs = [
        cluster_obj("namespace", route_ns),
        obj(GATEWAYS, &ns, "t26"),
        obj(GRANTS, &ns, "t26"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let gw = |from: Option<&str>| {
            let mut l = http_listener("http", host);
            if let Some(from) = from {
                l["allowedRoutes"] = json!({ "namespaces": { "from": from } });
            }
            gateway(&ns, "t26", vec![l])
        };
        apply(
            &env,
            &list(vec![
                json!({ "apiVersion": "v1", "kind": "Namespace", "metadata": { "name": route_ns } }),
                gw(None),
            ]),
        )
        .await
        .unwrap();
        apply(
            &env,
            &route(
                route_ns,
                "t26",
                json!([{ "name": "t26", "namespace": ns }]),
                &[host],
                json!([{ "backendRefs": [{ "name": "echo", "namespace": ns, "port": 80 }] }]),
            ),
        )
        .await
        .unwrap();
        let r = wait_route(&env, route_ns, "t26", "t26", &[("Accepted", "False", "NotAllowedByListeners")]).await;
        println!("[{T}] Same namespace only: {}", show(&our_parent(&r, "t26").unwrap()["conditions"]));
        let front = Front::new(&env).await.unwrap();
        let resp = front.get(HTTP_PORT, host, "/hostname", &[]).await.unwrap();
        assert_eq!(resp.status, 404, "{}", resp.short());

        apply(&env, &gw(Some("All"))).await.unwrap();
        let r = wait_route(&env, route_ns, "t26", "t26", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "False", "RefNotPermitted")]).await;
        println!("[{T}] from All, no grant: {}", show(&our_parent(&r, "t26").unwrap()["conditions"]));
        front
            .wait("500 without a ReferenceGrant", HTTP_PORT, host, "/hostname", &[], |r| r.status == 500)
            .await;
        wait_gateway(&env, &ns, "t26", "route attached", |g| attached(g, "http") == Some(1)).await;

        apply(
            &env,
            &json!({
                "apiVersion": "gateway.networking.k8s.io/v1beta1", "kind": "ReferenceGrant",
                "metadata": { "name": "t26", "namespace": ns },
                "spec": {
                    "from": [{ "group": GROUP, "kind": "HTTPRoute", "namespace": route_ns }],
                    "to": [{ "group": "", "kind": "Service", "name": "echo" }],
                },
            }),
        )
        .await
        .unwrap();
        wait_route(&env, route_ns, "t26", "t26", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        let resp = front
            .wait("200 with the ReferenceGrant", HTTP_PORT, host, "/hostname", &[], |r| r.status == 200)
            .await;
        assert!(resp.body.starts_with("echo-"), "{}", resp.short());

        delete_all(&env, &[obj(GRANTS, &ns, "t26")]).await.unwrap();
        wait_route(&env, route_ns, "t26", "t26", &[("ResolvedRefs", "False", "RefNotPermitted")]).await;
        front
            .wait("500 again without the grant", HTTP_PORT, host, "/hostname", &[], |r| r.status == 500)
            .await;
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 27. Status ownership
// ---------------------------------------------------------------------------------------

const FOREIGN_CONTROLLER: &str = "example.com/e2e-other-controller";

/// Route `t27` has two parents: Gateway `t27` (ours) and Gateway `t27-foreign` of class
/// `t27-other`, whose controller is someone else's (and not running). Our controller
/// writes only its own `status.parents` entry; an entry of the other controller, added
/// to the status by hand, survives our later updates (a spec change, so a new
/// observedGeneration). The foreign GatewayClass and Gateway get no status from us.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_27_status_ownership() {
    const T: &str = "e2e_27";
    let Some(env) = Env::setup("e2e_27_status_ownership").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t27.example.com";
    let objs = [
        cluster_obj(CLASSES, "t27-other"),
        obj(GATEWAYS, &ns, "t27"),
        obj(GATEWAYS, &ns, "t27-foreign"),
        obj(ROUTES, &ns, "t27"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let rules = |path: &str| json!([{ "matches": [prefix(path)], "backendRefs": [backend("echo", None)] }]);
        let parents = json!([{ "name": "t27" }, { "name": "t27-foreign" }]);
        apply(
            &env,
            &list(vec![
                json!({
                    "apiVersion": API_VERSION, "kind": "GatewayClass",
                    "metadata": { "name": "t27-other" },
                    "spec": { "controllerName": FOREIGN_CONTROLLER },
                }),
                gateway(&ns, "t27", vec![http_listener("http", host)]),
                gateway_of(&ns, "t27-foreign", "t27-other", vec![http_listener("http", host)]),
                route(&ns, "t27", parents.clone(), &[host], rules("/one")),
            ]),
        )
        .await
        .unwrap();
        let r = wait_route(&env, &ns, "t27", "t27", &[("Accepted", "True", "Accepted")]).await;
        let entries = r["status"]["parents"].as_array().unwrap();
        println!("[{T}] parents after our first write: {}", r["status"]["parents"]);
        assert_eq!(entries.len(), 1, "we wrote an entry for the foreign parent: {entries:?}");

        // The other controller's entry, as it would write it.
        let foreign = json!({
            "controllerName": FOREIGN_CONTROLLER,
            "parentRef": { "group": GROUP, "kind": "Gateway", "name": "t27-foreign" },
            "conditions": [{
                "type": "Accepted", "status": "True", "reason": "Accepted", "message": "by the other controller",
                "observedGeneration": 1, "lastTransitionTime": "2026-01-01T00:00:00Z",
            }],
        });
        let patch = serde_json::to_string(&json!([{ "op": "add", "path": "/status/parents/-", "value": foreign }])).unwrap();
        env.kubectl
            .run(&["patch", ROUTES, "t27", "-n", ns.as_str(), "--subresource=status", "--type=json", "-p", patch.as_str()])
            .await
            .unwrap();

        // A spec change: we write a new status for generation 2.
        apply(&env, &route(&ns, "t27", parents.clone(), &[host], rules("/two"))).await.unwrap();
        let r = wait_route(&env, &ns, "t27", "t27", &[("Accepted", "True", "Accepted")]).await;
        assert_eq!(r["metadata"]["generation"], 2);
        println!("[{T}] parents after our update: {}", r["status"]["parents"]);
        let kept = r["status"]["parents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["controllerName"] == FOREIGN_CONTROLLER)
            .cloned();
        assert_eq!(kept.as_ref(), Some(&foreign), "the foreign entry was not kept as written");
        assert_eq!(r["status"]["parents"].as_array().unwrap().len(), 2);
        let front = Front::new(&env).await.unwrap();
        front
            .wait("the new rule served", HTTP_PORT, host, "/two", &[], |r| r.status == 200)
            .await;

        // Nothing of ours on the foreign class and Gateway.
        let class = get_obj(&env.kubectl, CLASSES, None, "t27-other").await.unwrap().unwrap();
        println!("[{T}] foreign class: {}", show(&class["status"]["conditions"]));
        assert!(
            !cond_is(&class["status"]["conditions"], "Accepted", "True", None),
            "foreign class accepted: {}",
            class["status"]
        );
        let gw = get_obj(&env.kubectl, GATEWAYS, Some(&ns), "t27-foreign").await.unwrap().unwrap();
        println!("[{T}] foreign Gateway: {}", show(&gw["status"]["conditions"]));
        assert!(
            !cond_is(&gw["status"]["conditions"], "Accepted", "True", None)
                && gw["status"]["listeners"].as_array().is_none_or(|l| l.is_empty()),
            "foreign Gateway got a status: {}",
            gw["status"]
        );
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_curl_include_output() {
        let r = parse_response(
            "HTTP/1.1 301 Moved Permanently\r\nlocation: http://a/new?q=1\r\nX-Rule: x\r\n\r\nbody\n301",
        )
        .unwrap();
        assert_eq!(r.status, 301);
        assert_eq!(r.header("Location"), Some("http://a/new?q=1"));
        assert_eq!(r.header("x-rule"), Some("x"));
        assert_eq!(r.body, "body");
        let r = parse_response("\n000").unwrap();
        assert_eq!(r.status, 0);
        assert!(r.headers.is_empty());
    }

    #[test]
    fn counts_backends() {
        let s = vec![
            (200, "echo-abc".to_string()),
            (200, "{\"hostname\":\"go-httpbin\"}".to_string()),
            (500, "-".to_string()),
        ];
        let c = by_backend(&s);
        assert_eq!((c["echo"], c["httpbin"], c["500"]), (1, 1, 1));
    }
}
