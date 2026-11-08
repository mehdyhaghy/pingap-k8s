//! The end-to-end tests 19–32, 34 and 35 of the Gateway API provider, one test function each.
//!
//! The chart creates the GatewayClass `pingap` (tests/e2e/values.yaml): listeners map onto
//! the seed's Pingap servers `gw-http` (plain HTTP and h2c, port 8082), `gw-https` (TLS
//! by SNI, port 8443) and `gw-https-h2` (TLS with HTTP/2, port 8444; both TLS servers with
//! `strict_sni`), backends and listener Secrets live in `e2e-app`. The server groups
//! `a` and `b` (port 80 each, own Service) are only for test 30: the other Gateways ask
//! for the shared address ([`gateway`]), so they never take a group. The chart's Pingap
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
/// The seed's `gw-https-h2` server (`global_certificates`, `enabled_h2`: ALPN h2).
const HTTPS_H2_PORT: u16 = 8444;
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
const GRPC_ROUTES: &str = "grpcroutes.gateway.networking.k8s.io";
const LISTENER_SETS: &str = "listenersets.gateway.networking.k8s.io";
const BACKEND_TLS: &str = "backendtlspolicies.gateway.networking.k8s.io";

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

/// A Gateway of our class on the shared servers: it asks for the shared address, so it
/// never takes one of the server groups of tests/e2e/values.yaml.
fn gateway(ns: &str, name: &str, listeners: Vec<Value>) -> Value {
    let mut gw = gateway_of(ns, name, CLASS, listeners);
    gw["spec"]["addresses"] = json!([{ "type": "Hostname", "value": ADDRESS }]);
    gw
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
    wait_route_of(env, ROUTES, ns, name, gateway, want).await
}

/// [`wait_route`] for a route of resource `kind` (e.g. [`GRPC_ROUTES`]); `gateway` is the
/// name of the parent (a Gateway or a ListenerSet).
async fn wait_route_of(
    env: &Env,
    kind: &str,
    ns: &str,
    name: &str,
    gateway: &str,
    want: &[(&str, &str, &str)],
) -> Value {
    let what = format!("{kind} {ns}/{name} on {gateway}: {want:?}");
    wait_obj(env, &what, kind, Some(ns), name, |r| {
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
    if port == HTTPS_PORT || port == HTTPS_H2_PORT {
        "https"
    } else {
        "http"
    }
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
    apply_tls_secret_for(env, ns, name, &[host]).await
}

/// [`apply_tls_secret`] for a certificate with the DNS names `hosts` (the first is the CN).
async fn apply_tls_secret_for(
    env: &Env,
    ns: &str,
    name: &str,
    hosts: &[&str],
) -> anyhow::Result<()> {
    let host = hosts[0];
    let san = hosts
        .iter()
        .map(|h| format!("DNS:{h}"))
        .collect::<Vec<_>>()
        .join(",");
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
                format!("subjectAltName={san}").as_str(),
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

// ---------------------------------------------------------------------------------------
// 28. GRPCRoute
// ---------------------------------------------------------------------------------------

/// podinfo's gRPC port (Service port `grpc`, appProtocol `kubernetes.io/h2c`).
const PODINFO_GRPC_PORT: u16 = 9999;

/// A unary gRPC call over h2c (prior knowledge) to `$2`, resolved by `$1`: an empty
/// request message. Prints `<http status> <http version>`, then the `grpc-status`
/// header or trailer if any, then `BODY` and the response with everything but
/// `[A-Za-z0-9.-]` blanked.
const GRPC_CALL_SCRIPT: &str = r#"d=$(mktemp -d); head -c 5 /dev/zero > "$d/req"
curl -s -m 5 --http2-prior-knowledge --resolve "$1" -H 'content-type: application/grpc' \
  -H 'te: trailers' --data-binary "@$d/req" -D "$d/h" -o "$d/b" -w '%{http_code} %{http_version}' "$2"
echo; grep -i '^grpc-status:' "$d/h" | tr -d '\r'; echo "BODY $(tr -c 'A-Za-z0-9.-' ' ' < "$d/b" 2>/dev/null)"
rm -rf "$d""#;

/// One call of [`Front::grpc`].
#[derive(Debug, Clone, Default)]
struct GrpcResp {
    status: u16,
    http_version: String,
    grpc_status: Option<String>,
    body: String,
}

fn parse_grpc(stdout: &str) -> Option<GrpcResp> {
    let mut lines = stdout.lines();
    let mut first = lines.next()?.split_whitespace();
    let mut r = GrpcResp {
        status: first.next()?.parse().ok()?,
        http_version: first.next().unwrap_or_default().to_string(),
        ..GrpcResp::default()
    };
    for line in lines {
        if let Some(v) = line.strip_prefix("BODY") {
            r.body = v.split_whitespace().collect::<Vec<_>>().join(" ");
        } else if let Some((k, v)) = line.split_once(':')
            && k.eq_ignore_ascii_case("grpc-status")
        {
            r.grpc_status = Some(v.trim().to_string());
        }
    }
    Some(r)
}

impl Front {
    /// A unary gRPC call of `/<service>/<method>` on listener port [`HTTP_PORT`]
    /// ([`GRPC_CALL_SCRIPT`]).
    async fn grpc(&self, host: &str, path: &str) -> anyhow::Result<GrpcResp> {
        let resolve = self.resolve(host, HTTP_PORT);
        let url = format!("http://{host}:{HTTP_PORT}{path}");
        let out = self
            .kubectl
            .exec(
                &self.namespace,
                &self.pod,
                None,
                &["sh", "-c", GRPC_CALL_SCRIPT, "sh", &resolve, &url],
            )
            .await?;
        parse_grpc(&out.stdout)
            .ok_or_else(|| anyhow::anyhow!("unexpected output {:?} ({:?})", out.stdout, out.stderr))
    }
}

/// GRPCRoute `t28` on Gateway `t28` (listener on `gw-http`, which speaks h2c) sends the
/// method `info.InfoService/Info` (Exact service and method) and the whole service
/// `grpc.health.v1.Health` (service only) to podinfo's h2c port. The route is accepted
/// with its references resolved and counted on the listener. Unary calls over h2c
/// through Pingap reach podinfo (InfoResponse with the pod name, grpc-status 0); a method
/// no rule matches gets Pingap's 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_28_grpc_route() {
    const T: &str = "e2e_28";
    let Some(env) = Env::setup("e2e_28_grpc_route").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t28.example.com";
    let objs = [obj(GATEWAYS, &ns, "t28"), obj(GRPC_ROUTES, &ns, "t28")];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let podinfo = json!([{ "name": "podinfo", "port": PODINFO_GRPC_PORT }]);
        apply(
            &env,
            &list(vec![
                gateway(&ns, "t28", vec![http_listener("http", host)]),
                json!({
                    "apiVersion": API_VERSION, "kind": "GRPCRoute",
                    "metadata": { "name": "t28", "namespace": ns },
                    "spec": {
                        "parentRefs": [{ "name": "t28" }],
                        "hostnames": [host],
                        "rules": [
                            { "matches": [{ "method": { "type": "Exact", "service": "info.InfoService", "method": "Info" } }],
                              "backendRefs": podinfo },
                            { "matches": [{ "method": { "service": "grpc.health.v1.Health" } }],
                              "backendRefs": podinfo },
                        ],
                    },
                }),
            ]),
        )
        .await
        .unwrap();
        let r = wait_route_of(&env, GRPC_ROUTES, &ns, "t28", "t28", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        println!("[{T}] route: {}", show(&our_parent(&r, "t28").unwrap()["conditions"]));
        let gw = wait_gateway(&env, &ns, "t28", "GRPCRoute attached", |g| attached(g, "http") == Some(1)).await;
        assert!(listener_cond_is(&gw, "http", "Programmed", "True", "Programmed"));

        let front = Front::new(&env).await.unwrap();
        let info = wait_for("info.InfoService/Info through t28", TRAFFIC_WAIT, POLL, || async {
            let r = front.grpc(host, "/info.InfoService/Info").await?;
            if r.status == 200 && r.body.contains("podinfo-") {
                Ok(Some(r))
            } else {
                anyhow::bail!("last call: {r:?}")
            }
        })
        .await
        .unwrap();
        println!("[{T}] Info: {info:?}");
        assert_eq!(info.http_version, "2", "{info:?}");
        assert_eq!(info.grpc_status.as_deref(), Some("0"), "{info:?}");

        let health = front.grpc(host, "/grpc.health.v1.Health/Check").await.unwrap();
        println!("[{T}] Health/Check: {health:?}");
        assert_eq!((health.status, health.http_version.as_str()), (200, "2"), "{health:?}");
        assert!(health.grpc_status.is_some(), "no grpc-status from podinfo: {health:?}");

        let other = front.grpc(host, "/info.InfoService/Other").await.unwrap();
        println!("[{T}] info.InfoService/Other: {other:?}");
        assert_eq!(other.status, 404, "a method no rule matches: {other:?}");
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 29. HTTPRoute backend over h2c
// ---------------------------------------------------------------------------------------

/// The Gateway API conformance echo server (as in the v1.7.0 conformance manifests): HTTP
/// on 3000, h2c only on 3001; its JSON answer has the request's `proto`.
const ECHO_BASIC_IMAGE: &str = "registry.k8s.io/gateway-api/conformance/echo-basic:v0.1.0";

/// Deployment `name` (1 replica, label `app: <name>`) of one container.
fn deployment(ns: &str, name: &str, container: Value, volumes: Value) -> Value {
    json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": { "name": name, "namespace": ns },
        "spec": {
            "replicas": 1,
            "selector": { "matchLabels": { "app": name } },
            "template": {
                "metadata": { "labels": { "app": name } },
                "spec": { "terminationGracePeriodSeconds": 1, "containers": [container], "volumes": volumes },
            },
        },
    })
}

fn service(ns: &str, name: &str, ports: Value) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Service",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "selector": { "app": name }, "ports": ports },
    })
}

/// Service `t29` (echo-basic) has port `h2c` (8081 -> 3001, appProtocol
/// `kubernetes.io/h2c`) and port `http` (8080 -> 3000). Route `t29` sends `/h2c` to the
/// first and `/h1` to the second. A plain HTTP/1.1 client gets an answer through both:
/// echo-basic saw HTTP/2.0 on the h2c port (which takes nothing else) and HTTP/1.1 on
/// the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_29_h2c_backend() {
    const T: &str = "e2e_29";
    let Some(env) = Env::setup("e2e_29_h2c_backend").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t29.example.com";
    let objs = [
        obj("deployment", &ns, "t29"),
        obj("service", &ns, "t29"),
        obj(GATEWAYS, &ns, "t29"),
        obj(ROUTES, &ns, "t29"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let container = json!({
            "name": "echo", "image": ECHO_BASIC_IMAGE,
            "env": [
                { "name": "POD_NAME", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } },
                { "name": "NAMESPACE", "valueFrom": { "fieldRef": { "fieldPath": "metadata.namespace" } } },
            ],
            "ports": [{ "containerPort": 3000 }, { "containerPort": 3001 }],
            "readinessProbe": { "httpGet": { "path": "/", "port": 3000 }, "periodSeconds": 1 },
            "resources": { "requests": { "cpu": "10m", "memory": "16Mi" } },
        });
        apply(
            &env,
            &list(vec![
                deployment(&ns, "t29", container, json!([])),
                service(&ns, "t29", json!([
                    { "name": "h2c", "port": 8081, "targetPort": 3001, "appProtocol": "kubernetes.io/h2c" },
                    { "name": "http", "port": 8080, "targetPort": 3000 },
                ])),
                gateway(&ns, "t29", vec![http_listener("http", host)]),
                route(&ns, "t29", json!([{ "name": "t29" }]), &[host], json!([
                    { "matches": [prefix("/h2c")], "backendRefs": [{ "name": "t29", "port": 8081 }] },
                    { "matches": [prefix("/h1")], "backendRefs": [{ "name": "t29", "port": 8080 }] },
                ])),
            ]),
        )
        .await
        .unwrap();
        wait_route(&env, &ns, "t29", "t29", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        let front = Front::new(&env).await.unwrap();
        let r = front
            .wait("h2c backend answers", HTTP_PORT, host, "/h2c/x", &[], |r| r.status == 200)
            .await;
        let answer = r.json();
        println!("[{T}] /h2c/x: proto {} pod {} path {}", answer["proto"], answer["pod"], answer["path"]);
        assert_eq!(answer["proto"], "HTTP/2.0", "{}", r.short());
        assert!(answer["pod"].as_str().is_some_and(|p| p.starts_with("t29-")), "{}", r.short());
        let r = front
            .wait("HTTP/1.1 backend answers", HTTP_PORT, host, "/h1/x", &[], |r| r.status == 200)
            .await;
        println!("[{T}] /h1/x: proto {}", r.json()["proto"]);
        assert_eq!(r.json()["proto"], "HTTP/1.1", "{}", r.short());
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 30. Server groups
// ---------------------------------------------------------------------------------------

/// The server groups of tests/e2e/values.yaml: (Gateway, group address, Pingap server).
const GROUPS: [(&str, &str, &str); 2] = [
    ("t30-a", "pingap-k8s-pingap-gw-a.pingap-k8s.svc", "gw-a-80"),
    ("t30-b", "pingap-k8s-pingap-gw-b.pingap-k8s.svc", "gw-b-80"),
];

/// Gateways `t30-a` and `t30-b` ask for the addresses of the server groups `a` and `b`
/// and listen on port 80 without a hostname; route `t30-a` (no hostname, no match:
/// everything) goes to echo, `t30-b` to httpbin. Each Gateway is programmed with its
/// group's address, its route's locations are on its group's server only, and a request
/// to each group's Service reaches that Gateway's backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_30_server_groups() {
    const T: &str = "e2e_30";
    let Some(env) = Env::setup("e2e_30_server_groups").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let objs = [
        obj(GATEWAYS, &ns, "t30-a"),
        obj(GATEWAYS, &ns, "t30-b"),
        obj(ROUTES, &ns, "t30-a"),
        obj(ROUTES, &ns, "t30-b"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let mut items = Vec::new();
        for ((name, address, _), backend_name) in GROUPS.iter().zip(["echo", "httpbin"]) {
            let mut gw = gateway_of(
                &ns,
                name,
                CLASS,
                vec![json!({ "name": "http", "port": 80, "protocol": "HTTP" })],
            );
            gw["spec"]["addresses"] = json!([{ "type": "Hostname", "value": address }]);
            items.push(gw);
            items.push(route(
                &ns,
                name,
                json!([{ "name": name }]),
                &[],
                json!([{ "backendRefs": [backend(backend_name, None)] }]),
            ));
        }
        apply(&env, &list(items)).await.unwrap();

        for (name, address, _) in GROUPS {
            let gw = wait_gateway(
                &env,
                &ns,
                name,
                &format!("Gateway {name} programmed on its group"),
                |g| {
                    cond_is(
                        &g["status"]["conditions"],
                        "Programmed",
                        "True",
                        Some("Programmed"),
                    ) && listener_cond_is(g, "http", "Programmed", "True", "Programmed")
                        && attached(g, "http") == Some(1)
                },
            )
            .await;
            println!(
                "[{T}] {name}: {} addresses {}",
                show(&gw["status"]["conditions"]),
                gw["status"]["addresses"]
            );
            assert_eq!(
                gw["status"]["addresses"],
                json!([{ "type": "Hostname", "value": address }])
            );
            wait_route(
                &env,
                &ns,
                name,
                name,
                &[
                    ("Accepted", "True", "Accepted"),
                    ("ResolvedRefs", "True", "ResolvedRefs"),
                ],
            )
            .await;
        }

        // Each route's locations are on its group's server, not on the other one.
        let la = route_locations(&env, &ns, "t30-a").await.unwrap();
        let lb = route_locations(&env, &ns, "t30-b").await.unwrap();
        assert!(!la.is_empty() && !lb.is_empty(), "{la:?} {lb:?}");
        wait_for(
            "locations on the group servers",
            STATUS_WAIT,
            POLL,
            || async {
                let sa = server_locations(&env, GROUPS[0].2).await?;
                let sb = server_locations(&env, GROUPS[1].2).await?;
                let ok = la.iter().all(|l| sa.contains(l) && !sb.contains(l))
                    && lb.iter().all(|l| sb.contains(l) && !sa.contains(l));
                if ok {
                    println!(
                        "[{T}] servers: {} {sa:?}, {} {sb:?}",
                        GROUPS[0].2, GROUPS[1].2
                    );
                    Ok(Some(()))
                } else {
                    anyhow::bail!("{}: {sa:?}, {}: {sb:?}", GROUPS[0].2, GROUPS[1].2)
                }
            },
        )
        .await
        .unwrap();
        let shared = server_locations(&env, HTTP_SERVER).await.unwrap();
        assert!(
            la.iter().chain(&lb).all(|l| !shared.contains(l)),
            "group routes on {HTTP_SERVER}: {shared:?}"
        );

        // Through each group's Service.
        for ((_, address, _), want) in GROUPS.iter().zip(["echo-", "go-httpbin"]) {
            let url = format!("http://{address}/hostname");
            let r = wait_for(
                &format!("{url} answered by {want}"),
                TRAFFIC_WAIT,
                POLL,
                || async {
                    let r = env.pingap.get_url(&url).await?;
                    if r.status == 200 && r.body.contains(want) {
                        Ok(Some(r))
                    } else {
                        anyhow::bail!(
                            "last response: {} {:?}",
                            r.status,
                            r.body.chars().take(120).collect::<String>()
                        )
                    }
                },
            )
            .await
            .unwrap();
            println!(
                "[{T}] {url}: {} {}",
                r.status,
                r.body.trim().replace('\n', "")
            );
        }
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 31. ListenerSet
// ---------------------------------------------------------------------------------------

/// ListenerSet `t31-ls` adds listener `extra` (`ls.t31.example.com`, port 8082) to
/// Gateway `t31`; route `t31` attaches to the ListenerSet. While the Gateway has no
/// `allowedListeners` the ListenerSet is not accepted (NotAllowed) and its host gets 404.
/// With `allowedListeners` from Same, the ListenerSet and its listener are accepted and
/// programmed, the route is accepted with the ListenerSet as parent and served on the
/// ListenerSet's host, and not on the Gateway's own listener host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_31_listener_set() {
    const T: &str = "e2e_31";
    let Some(env) = Env::setup("e2e_31_listener_set").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let (own, extra) = ("t31.example.com", "ls.t31.example.com");
    let objs = [
        obj(GATEWAYS, &ns, "t31"),
        obj(LISTENER_SETS, &ns, "t31-ls"),
        obj(ROUTES, &ns, "t31"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let gw = |allowed: bool| {
            let mut g = gateway(&ns, "t31", vec![http_listener("http", own)]);
            if allowed {
                g["spec"]["allowedListeners"] = json!({ "namespaces": { "from": "Same" } });
            }
            g
        };
        apply(
            &env,
            &list(vec![
                gw(false),
                json!({
                    "apiVersion": API_VERSION, "kind": "ListenerSet",
                    "metadata": { "name": "t31-ls", "namespace": ns },
                    "spec": {
                        "parentRef": { "name": "t31" },
                        "listeners": [{ "name": "extra", "port": HTTP_PORT, "protocol": "HTTP", "hostname": extra }],
                    },
                }),
                route(&ns, "t31", json!([{ "group": GROUP, "kind": "ListenerSet", "name": "t31-ls" }]), &[], json!([{ "backendRefs": [backend("echo", None)] }])),
            ]),
        )
        .await
        .unwrap();
        let ls = wait_obj(&env, "ListenerSet t31-ls not allowed", LISTENER_SETS, Some(&ns), "t31-ls", |l| {
            cond_is(&l["status"]["conditions"], "Accepted", "False", Some("NotAllowed"))
        })
        .await
        .unwrap();
        println!("[{T}] without allowedListeners: {}", show(&ls["status"]["conditions"]));
        let front = Front::new(&env).await.unwrap();
        let r = front.get(HTTP_PORT, extra, "/hostname", &[]).await.unwrap();
        assert_eq!(r.status, 404, "{}", r.short());

        apply(&env, &gw(true)).await.unwrap();
        let ls = wait_obj(&env, "ListenerSet t31-ls accepted", LISTENER_SETS, Some(&ns), "t31-ls", |l| {
            let c = &l["status"]["conditions"];
            current(l, c)
                && cond_is(c, "Accepted", "True", Some("Accepted"))
                && cond_is(c, "Programmed", "True", Some("Programmed"))
                && listener(l, "extra").is_some_and(|x| {
                    cond_is(&x["conditions"], "Programmed", "True", Some("Programmed"))
                        && x["attachedRoutes"] == 1
                })
        })
        .await
        .unwrap();
        println!(
            "[{T}] with allowedListeners: {}; listener extra: {}",
            show(&ls["status"]["conditions"]),
            show(&listener(&ls, "extra").unwrap()["conditions"])
        );
        let r = wait_route(&env, &ns, "t31", "t31-ls", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        assert_eq!(our_parent(&r, "t31-ls").unwrap()["parentRef"]["kind"], "ListenerSet");
        let r = front
            .wait("route served on the ListenerSet host", HTTP_PORT, extra, "/hostname", &[], |r| r.status == 200)
            .await;
        assert!(r.body.starts_with("echo-"), "{}", r.short());
        let r = front.get(HTTP_PORT, own, "/hostname", &[]).await.unwrap();
        println!("[{T}] {extra}: 200 echo; {own}: {}", r.status);
        assert_eq!(r.status, 404, "route on the Gateway's own listener: {}", r.short());
        let g = get_obj(&env.kubectl, GATEWAYS, Some(&ns), "t31").await.unwrap().unwrap();
        assert_eq!(attached(&g, "http"), Some(0), "{}", g["status"]);
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 32. BackendTLSPolicy
// ---------------------------------------------------------------------------------------

/// A test CA and a server certificate for `host` signed by it: (CA PEM, certificate PEM,
/// key PEM).
fn backend_certs(host: &str) -> (String, String, String) {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair, KeyUsagePurpose,
    };
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.distinguished_name
        .push(DnType::CommonName, "pingap-k8s e2e backend CA");
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate().unwrap()).unwrap();
    let mut leaf = CertificateParams::new(vec![host.to_string()]).unwrap();
    leaf.distinguished_name.push(DnType::CommonName, host);
    leaf.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().unwrap();
    let cert = leaf.signed_by(&key, &ca).unwrap();
    (ca.pem(), cert.pem(), key.serialize_pem())
}

/// The `status.ancestors` entry our controller wrote for Gateway `gateway`.
fn our_ancestor<'a>(policy: &'a Value, gateway: &str) -> Option<&'a Value> {
    policy["status"]["ancestors"]
        .as_array()?
        .iter()
        .find(|a| a["controllerName"] == CONTROLLER_NAME && a["ancestorRef"]["name"] == gateway)
}

/// Deployment `t32-backend` (agnhost netexec) serves HTTPS only, with a certificate for
/// `backend.t32.example.com` from a test CA (rcgen) whose certificate is in ConfigMap
/// `t32-ca` (`ca.crt`). Route `t32` sends everything to it. Without a policy Pingap talks
/// plain HTTP to it (400 from the backend). BackendTLSPolicy `t32` (that hostname, that
/// CA) makes the request succeed over TLS, and the policy's `status.ancestors` has an
/// accepted entry for Gateway `t32`. With a hostname the certificate does not match, the
/// verification fails: 5xx.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_32_backend_tls_policy() {
    const T: &str = "e2e_32";
    let Some(env) = Env::setup("e2e_32_backend_tls_policy").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let host = "t32.example.com";
    let backend_host = "backend.t32.example.com";
    let objs = [
        obj("secret", &ns, "t32-backend-tls"),
        obj("configmap", &ns, "t32-ca"),
        obj("deployment", &ns, "t32-backend"),
        obj("service", &ns, "t32-backend"),
        obj(GATEWAYS, &ns, "t32"),
        obj(ROUTES, &ns, "t32"),
        obj(BACKEND_TLS, &ns, "t32"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        let (ca, cert, key) = backend_certs(backend_host);
        let container = json!({
            "name": "https", "image": "registry.k8s.io/e2e-test-images/agnhost:2.66.1",
            "args": ["netexec", "--http-port=8443", "--udp-port=-1",
                     "--tls-cert-file=/tls/tls.crt", "--tls-private-key-file=/tls/tls.key"],
            "ports": [{ "containerPort": 8443 }],
            "readinessProbe": { "httpGet": { "path": "/hostname", "port": 8443, "scheme": "HTTPS" }, "periodSeconds": 1 },
            "volumeMounts": [{ "name": "tls", "mountPath": "/tls", "readOnly": true }],
            "resources": { "requests": { "cpu": "10m", "memory": "16Mi" } },
        });
        apply(
            &env,
            &list(vec![
                json!({
                    "apiVersion": "v1", "kind": "Secret", "type": "kubernetes.io/tls",
                    "metadata": { "name": "t32-backend-tls", "namespace": ns },
                    "stringData": { "tls.crt": cert, "tls.key": key },
                }),
                json!({
                    "apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": { "name": "t32-ca", "namespace": ns },
                    "data": { "ca.crt": ca },
                }),
                deployment(&ns, "t32-backend", container, json!([{ "name": "tls", "secret": { "secretName": "t32-backend-tls" } }])),
                service(&ns, "t32-backend", json!([{ "name": "https", "port": 443, "targetPort": 8443 }])),
                gateway(&ns, "t32", vec![http_listener("http", host)]),
                route(&ns, "t32", json!([{ "name": "t32" }]), &[host], json!([{ "backendRefs": [{ "name": "t32-backend", "port": 443 }] }])),
            ]),
        )
        .await
        .unwrap();
        wait_route(&env, &ns, "t32", "t32", &[("Accepted", "True", "Accepted"), ("ResolvedRefs", "True", "ResolvedRefs")]).await;
        let front = Front::new(&env).await.unwrap();
        let r = front
            .wait("plain HTTP to the TLS backend", HTTP_PORT, host, "/hostname", &[], |r| r.status == 400)
            .await;
        println!("[{T}] without a policy: {}", r.short());

        let policy = |hostname: &str| {
            json!({
                "apiVersion": API_VERSION, "kind": "BackendTLSPolicy",
                "metadata": { "name": "t32", "namespace": ns },
                "spec": {
                    "targetRefs": [{ "group": "", "kind": "Service", "name": "t32-backend" }],
                    "validation": {
                        "caCertificateRefs": [{ "group": "", "kind": "ConfigMap", "name": "t32-ca" }],
                        "hostname": hostname,
                    },
                },
            })
        };
        apply(&env, &policy(backend_host)).await.unwrap();
        let p = wait_obj(&env, "BackendTLSPolicy t32 accepted for Gateway t32", BACKEND_TLS, Some(&ns), "t32", |p| {
            our_ancestor(p, "t32").is_some_and(|a| {
                current(p, &a["conditions"])
                    && cond_is(&a["conditions"], "Accepted", "True", Some("Accepted"))
                    && cond_is(&a["conditions"], "ResolvedRefs", "True", Some("ResolvedRefs"))
            })
        })
        .await
        .unwrap();
        let ancestor = our_ancestor(&p, "t32").unwrap();
        println!("[{T}] ancestor {}: {}", ancestor["ancestorRef"], show(&ancestor["conditions"]));
        assert_eq!(ancestor["ancestorRef"]["kind"], "Gateway");
        let r = front
            .wait("request over TLS with the policy", HTTP_PORT, host, "/hostname", &[], |r| r.status == 200)
            .await;
        println!("[{T}] with the policy: {}", r.short());
        assert!(r.body.starts_with("t32-backend-"), "{}", r.short());

        // A hostname the backend certificate does not match.
        apply(&env, &policy("wrong.t32.example.com")).await.unwrap();
        let r = front
            .wait("5xx with the wrong hostname", HTTP_PORT, host, "/hostname", &[], |r| (500..600).contains(&r.status))
            .await;
        println!("[{T}] wrong hostname: {}", r.short());
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 34. Misdirected requests on a reused HTTP/2 connection
// ---------------------------------------------------------------------------------------

/// What one request of [`reuse_requests`] got back.
#[derive(Debug, Clone, PartialEq)]
struct ReuseResult {
    status: u16,
    http_version: String,
    /// New connections curl opened for this request: 0 when it reused one.
    connects: u32,
}

/// The `curl` arguments for one request of a [`reuse_requests`] run.
fn reuse_args(front: &Front, port: u16, url_host: &str, host: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = ["-s", "-k", "--http2", "-m", "5", "-o", "/dev/null", "-w"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    args.push("%{http_code} %{http_version} %{num_connects}\n".into());
    args.push("--resolve".into());
    args.push(front.resolve(url_host, port));
    if let Some(host) = host {
        args.push("-H".into());
        args.push(format!("Host: {host}:{port}"));
    }
    args.push(format!("https://{url_host}:{port}/hostname"));
    args
}

/// Runs `requests` (URL host, `Host` header) in one curl process, separated by
/// `--next`: curl keeps its connections across them, so requests to the same URL host
/// share one HTTP/2 connection, opened with that URL host as the TLS server name (SNI),
/// whatever `Host` (`:authority`) they send.
async fn reuse_requests(
    front: &Front,
    port: u16,
    requests: &[(&str, Option<&str>)],
) -> anyhow::Result<Vec<ReuseResult>> {
    let mut cmd = vec!["curl".to_string()];
    for (i, (url_host, host)) in requests.iter().enumerate() {
        if i > 0 {
            cmd.push("--next".into());
        }
        cmd.extend(reuse_args(front, port, url_host, *host));
    }
    let out = front
        .kubectl
        .exec(&front.namespace, &front.pod, None, &cmd)
        .await?;
    let results: Vec<ReuseResult> = out
        .stdout
        .lines()
        .filter_map(|l| {
            let mut parts = l.split_whitespace();
            Some(ReuseResult {
                status: parts.next()?.parse().ok()?,
                http_version: parts.next()?.to_string(),
                connects: parts.next()?.parse().ok()?,
            })
        })
        .collect();
    if results.len() != requests.len() {
        anyhow::bail!(
            "expected {} results, got {:?} ({:?})",
            requests.len(),
            out.stdout,
            out.stderr
        );
    }
    Ok(results)
}

/// Gateway `t34` on the HTTP/2 TLS server (`gw-https-h2`, `strict_sni`) has listeners
/// `a.t34.example.com`, `b.t34.example.com` and `*.w.t34.example.com`, all with one
/// Secret whose certificate names all three, so a client may reuse one connection for
/// all of them (HTTP/2 connection coalescing). On a connection opened for
/// `a.t34.example.com`, a request with `Host: b.t34.example.com` belongs to another
/// listener and gets 421 Misdirected Request; on one opened for `x.w.t34.example.com`,
/// `y.w.t34.example.com` is the same (wildcard) listener and is served, and
/// `a.t34.example.com` gets 421. The connection stays usable after a 421. Every request
/// after the first of a connection reuses it (curl's `num_connects` is 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_34_misdirected_request_on_reused_connection() {
    const T: &str = "e2e_34";
    let Some(env) = Env::setup("e2e_34_misdirected_request_on_reused_connection").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let (a, b, wild) = (
        "a.t34.example.com",
        "b.t34.example.com",
        "*.w.t34.example.com",
    );
    let (x, y) = ("x.w.t34.example.com", "y.w.t34.example.com");
    let objs = [
        obj(GATEWAYS, &ns, "t34"),
        obj(ROUTES, &ns, "t34"),
        obj("secret", &ns, "t34-tls"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        apply_tls_secret_for(&env, &ns, "t34-tls", &[a, b, wild])
            .await
            .unwrap();
        let listener = |name: &str, host: &str| {
            json!({
                "name": name, "port": HTTPS_H2_PORT, "protocol": "HTTPS", "hostname": host,
                "tls": { "mode": "Terminate", "certificateRefs": [{ "kind": "Secret", "name": "t34-tls" }] },
            })
        };
        apply(
            &env,
            &list(vec![
                gateway(
                    &ns,
                    "t34",
                    vec![listener("a", a), listener("b", b), listener("wild", wild)],
                ),
                route(
                    &ns,
                    "t34",
                    json!([{ "name": "t34" }]),
                    &[a, b, wild],
                    json!([{ "backendRefs": [backend("echo", None)] }]),
                ),
            ]),
        )
        .await
        .unwrap();
        wait_gateway(&env, &ns, "t34", "Gateway t34 programmed", |g| {
            ["a", "b", "wild"]
                .iter()
                .all(|l| listener_cond_is(g, l, "Programmed", "True", "Programmed"))
        })
        .await;
        let front = Front::new(&env).await.unwrap();
        for host in [a, b, x] {
            front
                .wait(
                    &format!("HTTPS through t34 for {host}"),
                    HTTPS_H2_PORT,
                    host,
                    "/hostname",
                    &[],
                    |r| r.status == 200,
                )
                .await;
        }

        let requests = [
            (a, None),
            (a, Some(b)),
            (a, Some(a)),
            (x, None),
            (x, Some(y)),
            (x, Some(a)),
            (x, Some(x)),
        ];
        let results = reuse_requests(&front, HTTPS_H2_PORT, &requests)
            .await
            .unwrap();
        for ((url_host, host), r) in requests.iter().zip(&results) {
            println!(
                "[{T}] SNI {url_host}, Host {}: {} over HTTP/{}, new connections {}",
                host.unwrap_or(url_host),
                r.status,
                r.http_version,
                r.connects
            );
        }
        let expected = [200, 421, 200, 200, 200, 421, 200];
        let statuses: Vec<u16> = results.iter().map(|r| r.status).collect();
        assert_eq!(statuses, expected, "{results:?}");
        assert!(
            results.iter().all(|r| r.http_version == "2"),
            "not HTTP/2: {results:?}"
        );
        // One connection per TLS server name, reused by every later request to it.
        let connects: Vec<u32> = results.iter().map(|r| r.connects).collect();
        assert_eq!(connects, [1, 0, 0, 1, 0, 0, 0], "{results:?}");
    })
    .await;
}

// ---------------------------------------------------------------------------------------
// 35. Misdirected WebSocket upgrades and HTTP/1.1 keep-alive reuse
// ---------------------------------------------------------------------------------------

/// A WebSocket upgrade over HTTP/1.1 on a new TLS connection opened with the server name
/// (SNI) `url_host`, sending `Host: <host>`: the status of the answer (101 when it
/// switched protocols). curl waits for more after a 101 until its timeout, so its exit
/// status is not looked at.
async fn ws_upgrade(front: &Front, port: u16, url_host: &str, host: &str) -> anyhow::Result<u16> {
    let cmd: Vec<String> = [
        "curl",
        "-s",
        "-k",
        "--http1.1",
        "-m",
        "3",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "--resolve",
        &front.resolve(url_host, port),
        "-H",
        &format!("Host: {host}:{port}"),
        "-H",
        "Connection: Upgrade",
        "-H",
        "Upgrade: websocket",
        "-H",
        "Sec-WebSocket-Version: 13",
        "-H",
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
        &format!("https://{url_host}:{port}/ws"),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let out = front
        .kubectl
        .exec(&front.namespace, &front.pod, None, &cmd)
        .await?;
    out.stdout
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("unexpected curl output {:?} ({:?})", out.stdout, out.stderr))
}

/// Gateway `t35` on the HTTP/1.1 TLS server (`gw-https`, `strict_sni`) has listeners
/// `a.t35.example.com`, `b.t35.example.com` and `*.w.t35.example.com` with one Secret
/// for all three; `/ws` goes to the WebSocket port of the echo pods. A WebSocket is an
/// HTTP/1.1 upgrade on a connection of its own: with the `Host` of the TLS server name's
/// listener it switches protocols (101), with the `Host` of another listener it gets 421
/// and is never upgraded, and a name of the same wildcard listener is upgraded. A plain
/// request with another listener's `Host` on a reused HTTP/1.1 keep-alive connection
/// also gets 421.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_35_misdirected_websocket_and_keepalive() {
    const T: &str = "e2e_35";
    let Some(env) = Env::setup("e2e_35_misdirected_websocket_and_keepalive").await else {
        return;
    };
    let ns = env.app_namespace.clone();
    let (a, b, wild) = (
        "a.t35.example.com",
        "b.t35.example.com",
        "*.w.t35.example.com",
    );
    let (x, y) = ("x.w.t35.example.com", "y.w.t35.example.com");
    let objs = [
        obj(GATEWAYS, &ns, "t35"),
        obj(ROUTES, &ns, "t35"),
        obj("secret", &ns, "t35-tls"),
    ];
    ready(&env).await;
    with_objects(&env, &objs, async {
        apply_tls_secret_for(&env, &ns, "t35-tls", &[a, b, wild])
            .await
            .unwrap();
        apply(
            &env,
            &list(vec![
                gateway(
                    &ns,
                    "t35",
                    vec![
                        https_listener("a", a, "t35-tls"),
                        https_listener("b", b, "t35-tls"),
                        https_listener("wild", wild, "t35-tls"),
                    ],
                ),
                route(
                    &ns,
                    "t35",
                    json!([{ "name": "t35" }]),
                    &[a, b, wild],
                    json!([
                        { "matches": [prefix("/ws")],
                          "backendRefs": [{ "name": "echo", "port": 8090 }] },
                        { "backendRefs": [backend("echo", None)] },
                    ]),
                ),
            ]),
        )
        .await
        .unwrap();
        wait_gateway(&env, &ns, "t35", "Gateway t35 programmed", |g| {
            ["a", "b", "wild"]
                .iter()
                .all(|l| listener_cond_is(g, l, "Programmed", "True", "Programmed"))
        })
        .await;
        let front = Front::new(&env).await.unwrap();
        for host in [a, b, x] {
            front
                .wait(
                    &format!("HTTPS through t35 for {host}"),
                    HTTPS_PORT,
                    host,
                    "/hostname",
                    &[],
                    |r| r.status == 200,
                )
                .await;
        }

        // The `/ws` backend is a new upstream: 503 until its endpoints are written.
        wait_for(
            "WebSocket upgrade through t35",
            TRAFFIC_WAIT,
            POLL,
            || async {
                match ws_upgrade(&front, HTTPS_PORT, a, a).await? {
                    101 => Ok(Some(())),
                    status => anyhow::bail!("last status: {status}"),
                }
            },
        )
        .await
        .unwrap();

        // WebSocket upgrades: (TLS server name, Host, expected status).
        let cases = [
            (a, a, 101),
            (a, b, 421),
            (b, b, 101),
            (b, a, 421),
            (x, x, 101),
            (x, y, 101),
            (x, a, 421),
            (a, x, 421),
        ];
        let mut got = Vec::new();
        for (sni, host, _) in cases {
            let status = ws_upgrade(&front, HTTPS_PORT, sni, host).await.unwrap();
            println!("[{T}] WebSocket upgrade, SNI {sni}, Host {host}: {status}");
            got.push(status);
        }
        let expected: Vec<u16> = cases.iter().map(|c| c.2).collect();
        assert_eq!(got, expected);

        // HTTP/1.1 keep-alive: one connection opened for `a`, then another listener's Host.
        let mut cmd = vec!["curl".to_string()];
        for (i, host) in [None, Some(b)].iter().enumerate() {
            if i > 0 {
                cmd.push("--next".into());
            }
            let mut args = reuse_args(&front, HTTPS_PORT, a, *host);
            // reuse_args asks for HTTP/2; gw-https speaks HTTP/1.1 only.
            for arg in args.iter_mut() {
                if arg == "--http2" {
                    *arg = "--http1.1".into();
                }
            }
            cmd.extend(args);
        }
        let out = front
            .kubectl
            .exec(&front.namespace, &front.pod, None, &cmd)
            .await
            .unwrap();
        let lines: Vec<&str> = out.stdout.lines().collect();
        println!("[{T}] keep-alive, SNI {a}, Host {a} then {b}: {lines:?}");
        assert_eq!(lines, ["200 1.1 1", "421 1.1 0"], "{:?}", out.stderr);
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

    #[test]
    fn parses_grpc_call_output() {
        let r = parse_grpc("200 2\ngrpc-status: 0\nBODY  podinfo-abc   x \n").unwrap();
        assert_eq!((r.status, r.http_version.as_str()), (200, "2"));
        assert_eq!(r.grpc_status.as_deref(), Some("0"));
        assert_eq!(r.body, "podinfo-abc x");
        let r = parse_grpc("404 2\nBODY \n").unwrap();
        assert_eq!((r.status, r.grpc_status), (404, None));
    }

    #[test]
    fn backend_certs_are_signed_by_the_ca() {
        let (ca, cert, key) = backend_certs("backend.example.com");
        assert!(ca.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(cert.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(key.contains("PRIVATE KEY"));
        assert_ne!(ca, cert);
    }
}
