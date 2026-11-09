//! `kubectl pingap`: a kubectl plugin for pingap-k8s.
//!
//! It shells out to `kubectl` for everything that talks to the cluster
//! (listing pods, `port-forward`, reading the admin Secret), so it uses the
//! caller's kubeconfig, context, credentials and RBAC exactly as kubectl
//! does, and needs no extra client features. The controller's `/status`
//! listens on 127.0.0.1 inside the pod and is reached only through
//! `kubectl port-forward` (RBAC: `pods/portforward`).

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use clap::{Parser, Subcommand};
use serde_json::Value;

use kubectl_pingap::{STATUS_VERSION, StatusDoc, UpstreamStatus};

/// Pod annotation the chart sets when `status.enabled` is true.
const STATUS_PORT_ANNOTATION: &str = "pingap-k8s/status-port";
const DEFAULT_SELECTOR: &str =
    "app.kubernetes.io/name=pingap-k8s,app.kubernetes.io/component=controller";
/// Labels of the chart's quickstart Pingap Services. All three are matched so a
/// Service of another chart that only shares `component=proxy` is never used.
const PROXY_SELECTOR: &str = "app.kubernetes.io/part-of=pingap-k8s,app.kubernetes.io/name=pingap,app.kubernetes.io/component=proxy";
const FORWARD_TIMEOUT: Duration = Duration::from_secs(20);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const DISABLED_HINT: &str = "the status endpoint is disabled: enable status.enabled in the chart \
     (helm upgrade <release> <chart> --reuse-values --set status.enabled=true)";

/// Inspect pingap-k8s from kubectl.
#[derive(Debug, Parser)]
#[command(name = "kubectl pingap", bin_name = "kubectl pingap", version)]
struct Cli {
    /// Namespace of the controller (default: the kubectl context's namespace).
    #[arg(short = 'n', long, global = true)]
    namespace: Option<String>,
    /// kubeconfig context to use.
    #[arg(long, global = true)]
    context: Option<String>,
    /// Path to the kubeconfig file.
    #[arg(long, global = true)]
    kubeconfig: Option<String>,
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Show every owned upstream as seen by the leader.
    Status {
        #[command(flatten)]
        target: StatusTarget,
    },
    /// Explain one upstream: warnings, watch state, and why it has no endpoints.
    Explain {
        upstream: String,
        #[command(flatten)]
        target: StatusTarget,
    },
    /// Port-forward the quickstart Pingap admin UI and print how to log in.
    Admin {
        /// Local port (default: the Service port).
        #[arg(long)]
        local_port: Option<u16>,
        /// Also print the admin password (read from its Secret).
        #[arg(long)]
        show_password: bool,
    },
}

#[derive(Debug, clap::Args)]
struct StatusTarget {
    /// Label selector of the controller pods.
    #[arg(short = 'l', long, default_value = DEFAULT_SELECTOR)]
    selector: String,
    /// Status port in the pod (default: from the pod annotation the chart sets).
    #[arg(long)]
    port: Option<u16>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let kubectl = Kubectl {
        bin: std::env::var("KUBECTL").unwrap_or_else(|_| "kubectl".into()),
        namespace: cli.namespace.clone(),
        context: cli.context.clone(),
        kubeconfig: cli.kubeconfig.clone(),
    };
    let result = match &cli.command {
        Cmd::Status { target } => {
            fetch_status(&kubectl, target).map(|(pod, doc)| print!("{}", render_status(&pod, &doc)))
        }
        Cmd::Explain { upstream, target } => fetch_status(&kubectl, target)
            .and_then(|(pod, doc)| render_explain(&pod, &doc, upstream))
            .map(|text| print!("{text}")),
        Cmd::Admin {
            local_port,
            show_password,
        } => admin(&kubectl, *local_port, *show_password),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err:#}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------------------
// kubectl

struct Kubectl {
    bin: String,
    namespace: Option<String>,
    context: Option<String>,
    kubeconfig: Option<String>,
}

impl Kubectl {
    fn command(&self, namespace: Option<&str>) -> Command {
        let mut cmd = Command::new(&self.bin);
        if let Some(kc) = &self.kubeconfig {
            cmd.args(["--kubeconfig", kc]);
        }
        if let Some(ctx) = &self.context {
            cmd.args(["--context", ctx]);
        }
        if let Some(ns) = namespace.or(self.namespace.as_deref()) {
            cmd.args(["--namespace", ns]);
        }
        cmd
    }

    fn json(&self, args: &[&str]) -> anyhow::Result<Value> {
        let out = self
            .command(None)
            .args(args)
            .args(["-o", "json"])
            .stderr(Stdio::inherit())
            .output()
            .with_context(|| format!("running {}", self.bin))?;
        if !out.status.success() {
            bail!("kubectl {} failed", args.join(" "));
        }
        serde_json::from_slice(&out.stdout).context("parsing kubectl output")
    }

    /// Starts `kubectl port-forward <resource> <local>:<remote>` and returns
    /// the child with the local port it listens on (127.0.0.1).
    fn port_forward(
        &self,
        namespace: &str,
        resource: &str,
        local: Option<u16>,
        remote: u16,
    ) -> anyhow::Result<(ChildGuard, u16)> {
        let spec = format!(
            "{}:{remote}",
            local.map(|p| p.to_string()).unwrap_or_default()
        );
        let mut child = self
            .command(Some(namespace))
            .args(["port-forward", "--address", "127.0.0.1", resource, &spec])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("running {} port-forward", self.bin))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let guard = ChildGuard(child);
        let (tx, rx) = mpsc::channel::<Result<u16, String>>();
        let tx_err = tx.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(port) = parse_forward_line(&line) {
                    let _ = tx.send(Ok(port));
                }
            }
        });
        std::thread::spawn(move || {
            let mut text = String::new();
            let _ = BufReader::new(stderr).read_to_string(&mut text);
            let _ = tx_err.send(Err(text));
        });
        match rx.recv_timeout(FORWARD_TIMEOUT) {
            Ok(Ok(port)) => Ok((guard, port)),
            Ok(Err(stderr)) => bail!("kubectl port-forward {resource} failed: {}", stderr.trim()),
            Err(_) => {
                bail!("kubectl port-forward {resource} did not start within {FORWARD_TIMEOUT:?}")
            }
        }
    }
}

/// Kills the child process when dropped.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// `Forwarding from 127.0.0.1:54321 -> 9091` gives 54321.
fn parse_forward_line(line: &str) -> Option<u16> {
    let rest = line.strip_prefix("Forwarding from 127.0.0.1:")?;
    rest.split_whitespace().next()?.parse().ok()
}

// ---------------------------------------------------------------------------
// status

#[derive(Clone, Debug, PartialEq, Eq)]
struct Pod {
    name: String,
    namespace: String,
    running: bool,
    status_port: Option<u16>,
}

fn parse_pods(list: &Value) -> Vec<Pod> {
    list["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|p| Pod {
                    name: p["metadata"]["name"].as_str().unwrap_or_default().into(),
                    namespace: p["metadata"]["namespace"]
                        .as_str()
                        .unwrap_or_default()
                        .into(),
                    running: p["status"]["phase"] == "Running"
                        && p["metadata"]["deletionTimestamp"].is_null(),
                    status_port: p["metadata"]["annotations"][STATUS_PORT_ANNOTATION]
                        .as_str()
                        .and_then(|s| s.parse().ok()),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Finds the controller pods, asks each one for `/status` and returns the
/// leader's document (or the first one when no leader answered).
fn fetch_status(kubectl: &Kubectl, target: &StatusTarget) -> anyhow::Result<(Pod, StatusDoc)> {
    let list = kubectl.json(&["get", "pods", "-l", &target.selector])?;
    let pods: Vec<Pod> = parse_pods(&list)
        .into_iter()
        .filter(|p| p.running)
        .collect();
    if pods.is_empty() {
        bail!(
            "no running controller pods with labels {} in {}; pass -n <namespace> or -l <selector>",
            target.selector,
            kubectl
                .namespace
                .as_deref()
                .map_or("the current namespace".to_string(), |n| format!(
                    "namespace {n}"
                ))
        );
    }
    let mut first: Option<(Pod, StatusDoc)> = None;
    let mut errors = Vec::new();
    for pod in pods {
        let Some(port) = target.port.or(pod.status_port) else {
            errors.push(format!("{}: {DISABLED_HINT}", pod.name));
            continue;
        };
        match pod_status(kubectl, &pod, port) {
            Ok(doc) if doc.is_leader => return Ok((pod, doc)),
            Ok(doc) => {
                first.get_or_insert((pod, doc));
            }
            Err(err) => errors.push(format!("{}: {err:#}", pod.name)),
        }
    }
    match first {
        Some(found) => Ok(found),
        None => Err(anyhow!("{}", errors.join("\n"))),
    }
}

fn pod_status(kubectl: &Kubectl, pod: &Pod, port: u16) -> anyhow::Result<StatusDoc> {
    let (_forward, local) =
        kubectl.port_forward(&pod.namespace, &format!("pod/{}", pod.name), None, port)?;
    let body = http_get(local, "/status")
        .map_err(|err| anyhow!("no answer on port {port} ({err:#}); {DISABLED_HINT}"))?;
    let doc: StatusDoc = serde_json::from_str(&body).context("parsing /status")?;
    if doc.version != STATUS_VERSION {
        bail!(
            "status version {} is not supported by this plugin (expects {STATUS_VERSION}); \
             use the plugin release that matches the controller",
            doc.version
        );
    }
    Ok(doc)
}

/// Minimal HTTP/1.1 GET against 127.0.0.1 (the port-forward), body as text.
fn http_get(port: u16, path: &str) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(HTTP_TIMEOUT))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    parse_http_response(&String::from_utf8_lossy(&raw))
}

fn parse_http_response(raw: &str) -> anyhow::Result<String> {
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow!("empty or truncated response"))?;
    let status = head.split_whitespace().nth(1).unwrap_or_default();
    if status != "200" {
        bail!("HTTP {status}");
    }
    Ok(body.to_string())
}

fn yes_no(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

fn ago(doc: &StatusDoc, at: u64) -> String {
    let secs = doc.generated_at_unix.saturating_sub(at);
    match secs {
        0..=119 => format!("{secs}s ago"),
        120..=7199 => format!("{}m ago", secs / 60),
        7200..=172_799 => format!("{}h ago", secs / 3600),
        _ => format!("{}d ago", secs / 86_400),
    }
}

fn last_write(doc: &StatusDoc, u: &UpstreamStatus) -> String {
    match &u.last_write {
        Some(w) => format!("{} {}", w.result, ago(doc, w.at_unix)),
        None => "-".into(),
    }
}

fn notes(u: &UpstreamStatus) -> String {
    let mut notes = Vec::new();
    if u.no_endpoints {
        notes.push("no endpoints".to_string());
    }
    if u.fallback {
        notes.push("drain fallback".to_string());
    }
    if u.malformed {
        notes.push("malformed entry".to_string());
    }
    match u.warnings.len() {
        0 => {}
        1 => notes.push("1 warning".into()),
        n => notes.push(format!("{n} warnings")),
    }
    notes.join(", ")
}

fn header(pod: &Pod, doc: &StatusDoc) -> String {
    let synced = doc.watchers.services.iter().filter(|w| w.synced).count();
    format!(
        "controller {} ({}) v{}, phase {}, ready {}{}, etcd prefix {}, watchers {}/{} synced\n",
        pod.name,
        if doc.is_leader { "leader" } else { "follower" },
        doc.controller.version,
        doc.phase,
        yes_no(doc.ready),
        doc.not_ready_reason
            .as_deref()
            .map(|r| format!(" ({r})"))
            .unwrap_or_default(),
        doc.etcd.prefix,
        synced,
        doc.watchers.services.len(),
    )
}

/// The `status` table.
fn render_status(pod: &Pod, doc: &StatusDoc) -> String {
    let mut out = header(pod, doc);
    if doc.upstreams.is_empty() {
        out.push_str("no upstreams\n");
        return out;
    }
    let mut rows = vec![[
        "UPSTREAM".to_string(),
        "TARGET".into(),
        "SERVICES".into(),
        "READY".into(),
        "DRAINING".into(),
        "IN SYNC".into(),
        "LAST WRITE".into(),
        "NOTES".into(),
    ]];
    for u in &doc.upstreams {
        rows.push([
            u.name.clone(),
            u.target.clone(),
            u.services.join(","),
            u.ready.to_string(),
            u.draining.to_string(),
            if doc.is_leader {
                yes_no(u.in_sync).into()
            } else {
                "-".into()
            },
            last_write(doc, u),
            notes(u),
        ]);
    }
    let mut widths = [0usize; 8];
    for row in &rows {
        for (w, cell) in widths.iter_mut().zip(row) {
            *w = (*w).max(cell.chars().count());
        }
    }
    for row in &rows {
        let line: Vec<String> = row
            .iter()
            .zip(widths)
            .map(|(cell, w)| format!("{cell:<w$}"))
            .collect();
        out.push_str(line.join("  ").trim_end());
        out.push('\n');
    }
    out
}

fn list(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".into()
    } else {
        items.join(", ")
    }
}

/// The `explain` text.
fn render_explain(pod: &Pod, doc: &StatusDoc, name: &str) -> anyhow::Result<String> {
    let Some(u) = doc.upstreams.iter().find(|u| u.name == name) else {
        let known: Vec<String> = doc.upstreams.iter().map(|u| u.name.clone()).collect();
        bail!(
            "upstream {name:?} is not managed by this controller (managed: {})",
            list(&known)
        );
    };
    let mut out = header(pod, doc);
    out.push_str(&format!("upstream {} (target {})\n", u.name, u.target));
    out.push_str(&format!("  services:   {}\n", list(&u.services)));
    out.push_str(&format!("  desired:    {}\n", list(&u.desired)));
    out.push_str(&format!(
        "  stored:     {}\n",
        u.stored.as_deref().map_or("unknown".into(), list)
    ));
    out.push_str(&format!("  ready {}, draining {}\n", u.ready, u.draining));
    out.push_str(&format!("  last write: {}\n", last_write(doc, u)));
    out.push_str("  warnings:");
    if u.warnings.is_empty() {
        out.push_str(" none\n");
    } else {
        out.push('\n');
        for w in &u.warnings {
            out.push_str(&format!("    - {w}\n"));
        }
    }
    out.push_str("diagnosis:\n");
    for line in diagnose(doc, u) {
        out.push_str(&format!("  - {line}\n"));
    }
    Ok(out)
}

/// `<service>.<namespace>:<port>[ weight]` gives `<namespace>/<service>`.
fn service_key(entry: &str) -> Option<String> {
    let host = entry.split(':').next()?;
    let (name, ns) = host.split_once('.')?;
    Some(format!("{ns}/{name}"))
}

fn diagnose(doc: &StatusDoc, u: &UpstreamStatus) -> Vec<String> {
    let mut out = Vec::new();
    if doc.phase != "running" {
        out.push(format!(
            "the controller is {}: nothing is written until every watcher has listed once",
            doc.phase
        ));
    }
    if !doc.is_leader {
        out.push("this pod is a follower and never writes; no leader answered".into());
    }
    for entry in &u.services {
        let Some(key) = service_key(entry) else {
            continue;
        };
        match doc.watchers.services.iter().find(|w| w.service == key) {
            Some(w) if !w.synced || !w.up => {
                let ns = key.split_once('/').map_or("", |(ns, _)| ns);
                out.push(format!(
                    "the watch of Service {key} is {}: check the controller's RBAC in namespace \
                     {ns} (services, endpointslices: get/list/watch) and the controller logs",
                    if w.synced { "down" } else { "not synced" },
                ));
            }
            Some(_) => {}
            None => out.push(format!("Service {key} is not watched yet")),
        }
    }
    for w in &u.warnings {
        out.push(format!("{w}: fix the Service or the services entry"));
    }
    if u.desired.is_empty() {
        out.push(
            "no endpoint was ever resolved, so nothing was written: check the Services above, \
             the pods behind them and their readiness probes"
                .into(),
        );
    } else if u.no_endpoints {
        out.push(
            "no ready endpoints now: the last addrs stay in etcd until a pod is ready; \
             check the pods behind the Services and their readiness probes"
                .into(),
        );
    }
    if u.fallback {
        out.push(
            "only terminating endpoints are left: writing draining endpoints until a pod is ready"
                .into(),
        );
    }
    if u.malformed {
        out.push(format!(
            "the stored entry {}/upstreams/{}.toml cannot be edited: fix or delete it in the admin UI",
            doc.etcd.prefix.trim_end_matches('/'),
            u.name
        ));
    }
    if doc.is_leader && !u.in_sync && !u.desired.is_empty() && !u.malformed {
        match u.last_write.as_ref().map(|w| w.result.as_str()) {
            Some("error") => out.push(
                "the last write failed (etcd unreachable?): it is retried with backoff".into(),
            ),
            Some("conflict") => out.push("the last write lost a CAS race: it is retried".into()),
            _ if u.pending => out.push("a write is pending (debounce or in flight)".into()),
            _ => out.push("stored addrs differ from desired: a corrective write follows".into()),
        }
    }
    if out.is_empty() {
        out.push("no problems found".into());
    }
    out
}

// ---------------------------------------------------------------------------
// admin

#[derive(Debug, PartialEq, Eq)]
struct AdminService {
    name: String,
    namespace: String,
    port: u16,
}

/// The quickstart Pingap admin Services: proxy Services with a port named `admin`.
fn find_admin_services(list: &Value) -> Vec<AdminService> {
    let Some(items) = list["items"].as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|svc| {
            let port = svc["spec"]["ports"]
                .as_array()?
                .iter()
                .find(|p| p["name"] == "admin")?["port"]
                .as_u64()?;
            Some(AdminService {
                name: svc["metadata"]["name"].as_str()?.into(),
                namespace: svc["metadata"]["namespace"].as_str()?.into(),
                port: u16::try_from(port).ok()?,
            })
        })
        .collect()
}

/// Exactly one admin Service, or an error naming the choices: the password typed
/// into the forwarded page must only ever reach the chart's own Pingap.
fn single_admin_service(mut found: Vec<AdminService>) -> anyhow::Result<AdminService> {
    match found.len() {
        0 => bail!(
            "no quickstart Pingap admin Service (labels {PROXY_SELECTOR}, port named admin) \
             found; pass -n <namespace> (it exists only with pingap.deploy=true)"
        ),
        1 => Ok(found.remove(0)),
        _ => {
            let names: Vec<String> = found
                .iter()
                .map(|s| format!("{}/{}", s.namespace, s.name))
                .collect();
            bail!(
                "more than one Pingap admin Service matches ({}); pass -n <namespace>",
                names.join(", ")
            )
        }
    }
}

fn admin(kubectl: &Kubectl, local_port: Option<u16>, show_password: bool) -> anyhow::Result<()> {
    let list = kubectl.json(&["get", "services", "-l", PROXY_SELECTOR])?;
    let svc = single_admin_service(find_admin_services(&list))?;
    // The chart names the admin Secret like the admin Service by default.
    let secret = &svc.name;
    let password = if show_password {
        let out = kubectl
            .command(Some(&svc.namespace))
            .args([
                "get",
                "secret",
                secret,
                "-o",
                "go-template={{.data.password | base64decode}}",
            ])
            .stderr(Stdio::inherit())
            .output()
            .context("running kubectl get secret")?;
        if !out.status.success() {
            bail!("reading Secret {secret} failed");
        }
        Some(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        None
    };
    let (mut forward, port) = kubectl.port_forward(
        &svc.namespace,
        &format!("svc/{}", svc.name),
        Some(local_port.unwrap_or(svc.port)),
        svc.port,
    )?;
    print!("{}", admin_message(&svc, port, password.as_deref()));
    std::io::stdout().flush().ok();
    let status = forward
        .0
        .wait()
        .context("waiting for kubectl port-forward")?;
    if !status.success() {
        bail!("kubectl port-forward exited with {status}");
    }
    Ok(())
}

fn admin_message(svc: &AdminService, port: u16, password: Option<&str>) -> String {
    let login = match password {
        Some(p) => format!("password: {p}\n"),
        None => format!(
            "password: kubectl -n {} get secret {} -o go-template='{{{{.data.password | base64decode}}}}'\n\
             (or run with --show-password)\n",
            svc.namespace, svc.name
        ),
    };
    format!(
        "Pingap admin UI: http://127.0.0.1:{port}  (port-forward to svc/{} in {}; Ctrl-C to stop)\n\
         user: admin (chart default, pingap.admin.user)\n{login}",
        svc.name, svc.namespace
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kubectl_pingap::{ControllerInfo, EtcdInfo, LastWrite, WatchInfo, WatchersInfo};

    fn pod() -> Pod {
        Pod {
            name: "pingap-k8s-7d9f-abcde".into(),
            namespace: "pingap-k8s".into(),
            running: true,
            status_port: Some(9091),
        }
    }

    fn doc() -> StatusDoc {
        StatusDoc {
            version: 1,
            controller: ControllerInfo {
                identity: "pingap-k8s-7d9f-abcde".into(),
                version: "0.16.10".into(),
            },
            generated_at_unix: 1_000,
            phase: "running".into(),
            is_leader: true,
            ready: true,
            not_ready_reason: None,
            etcd: EtcdInfo {
                prefix: "/pingap".into(),
            },
            watchers: WatchersInfo {
                synced: false,
                services: vec![
                    WatchInfo {
                        service: "prod/api".into(),
                        synced: true,
                        up: true,
                    },
                    WatchInfo {
                        service: "prod/web".into(),
                        synced: true,
                        up: false,
                    },
                ],
            },
            upstreams: vec![
                UpstreamStatus {
                    name: "api".into(),
                    target: "pods".into(),
                    services: vec!["api.prod:http".into()],
                    desired: vec!["10.0.0.1:8080".into(), "10.0.0.2:8080".into()],
                    stored: Some(vec!["10.0.0.1:8080".into(), "10.0.0.2:8080".into()]),
                    in_sync: true,
                    ready: 2,
                    last_write: Some(LastWrite {
                        at_unix: 988,
                        result: "ok".into(),
                        addrs: vec![],
                    }),
                    ..Default::default()
                },
                UpstreamStatus {
                    name: "web".into(),
                    target: "service".into(),
                    services: vec!["web.prod:80 9".into(), "gone.prod:80 1".into()],
                    no_endpoints: true,
                    warnings: vec!["service prod/gone not found".into()],
                    ..Default::default()
                },
            ],
        }
    }

    #[test]
    fn status_table() {
        let got = render_status(&pod(), &doc());
        let want = "\
controller pingap-k8s-7d9f-abcde (leader) v0.16.10, phase running, ready yes, etcd prefix /pingap, watchers 2/2 synced
UPSTREAM  TARGET   SERVICES                      READY  DRAINING  IN SYNC  LAST WRITE  NOTES
api       pods     api.prod:http                 2      0         yes      ok 12s ago
web       service  web.prod:80 9,gone.prod:80 1  0      0         no       -           no endpoints, 1 warning
";
        assert_eq!(got, want, "\n{got}");
    }

    #[test]
    fn follower_table_has_no_sync_column_values() {
        let mut d = doc();
        d.is_leader = false;
        d.ready = false;
        d.not_ready_reason = Some("watchers not synced".into());
        d.upstreams.truncate(1);
        let got = render_status(&pod(), &d);
        assert!(got.contains("(follower)"), "{got}");
        assert!(got.contains("ready no (watchers not synced)"), "{got}");
        assert!(got.lines().nth(2).unwrap().contains("  -  "), "{got}");
        d.upstreams.clear();
        assert!(render_status(&pod(), &d).ends_with("no upstreams\n"));
    }

    #[test]
    fn explain_reports_reasons() {
        let got = render_explain(&pod(), &doc(), "web").unwrap();
        assert!(got.contains("upstream web (target service)"), "{got}");
        assert!(got.contains("    - service prod/gone not found"), "{got}");
        assert!(
            got.contains("the watch of Service prod/web is down"),
            "{got}"
        );
        assert!(
            got.contains("Service prod/gone is not watched yet"),
            "{got}"
        );
        assert!(got.contains("nothing was written"), "{got}");
        assert!(got.contains("stored:     unknown"), "{got}");

        let got = render_explain(&pod(), &doc(), "api").unwrap();
        assert!(
            got.ends_with("diagnosis:\n  - no problems found\n"),
            "{got}"
        );

        let err = render_explain(&pod(), &doc(), "nope").unwrap_err();
        assert!(err.to_string().contains("managed: api, web"), "{err}");
    }

    #[test]
    fn explain_malformed_and_failed_write() {
        let mut d = doc();
        d.upstreams[0].in_sync = false;
        d.upstreams[0].last_write.as_mut().unwrap().result = "error".into();
        let got = render_explain(&pod(), &d, "api").unwrap();
        assert!(got.contains("the last write failed"), "{got}");
        d.upstreams[0].no_endpoints = true;
        let got = render_explain(&pod(), &d, "api").unwrap();
        assert!(got.contains("no ready endpoints now"), "{got}");
        d.upstreams[0].malformed = true;
        let got = render_explain(&pod(), &d, "api").unwrap();
        assert!(
            got.contains("/pingap/upstreams/api.toml cannot be edited"),
            "{got}"
        );
    }

    #[test]
    fn ages() {
        let d = doc();
        assert_eq!(ago(&d, 1_000), "0s ago");
        assert_eq!(ago(&d, 2_000), "0s ago");
        assert_eq!(ago(&d, 1_000 - 300), "5m ago");
        assert_eq!(ago(&d, 1_000 - 1_000), "16m ago");
        let mut d = d;
        d.generated_at_unix = 400_000;
        assert_eq!(ago(&d, 0), "4d ago");
        assert_eq!(ago(&d, 400_000 - 7_200), "2h ago");
    }

    #[test]
    fn forward_line() {
        assert_eq!(
            parse_forward_line("Forwarding from 127.0.0.1:54321 -> 9091"),
            Some(54321)
        );
        assert_eq!(
            parse_forward_line("Forwarding from [::1]:54321 -> 9091"),
            None
        );
        assert_eq!(parse_forward_line("Handling connection for 54321"), None);
    }

    #[test]
    fn http_response() {
        let ok = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{\"a\":1}";
        assert_eq!(parse_http_response(ok).unwrap(), "{\"a\":1}");
        assert!(parse_http_response("HTTP/1.1 404 Not Found\r\n\r\n").is_err());
        assert!(parse_http_response("").is_err());
    }

    #[test]
    fn pods_and_annotation() {
        let list: Value = serde_json::from_str(
            r#"{"items":[
              {"metadata":{"name":"a","namespace":"ns","annotations":{"pingap-k8s/status-port":"9091"}},"status":{"phase":"Running"}},
              {"metadata":{"name":"b","namespace":"ns"},"status":{"phase":"Pending"}},
              {"metadata":{"name":"c","namespace":"ns","deletionTimestamp":"x"},"status":{"phase":"Running"}}
            ]}"#,
        )
        .unwrap();
        let pods = parse_pods(&list);
        assert_eq!(pods.len(), 3);
        assert_eq!(pods[0].status_port, Some(9091));
        assert!(pods[0].running);
        assert_eq!(pods[1].status_port, None);
        assert!(!pods[1].running && !pods[2].running);
    }

    #[test]
    fn admin_service_and_message() {
        let list: Value = serde_json::from_str(
            r#"{"items":[
              {"metadata":{"name":"rel-pingap","namespace":"ns"},"spec":{"ports":[{"name":"http","port":80}]}},
              {"metadata":{"name":"rel-pingap-admin","namespace":"ns"},"spec":{"ports":[{"name":"admin","port":3018}]}}
            ]}"#,
        )
        .unwrap();
        let svc = single_admin_service(find_admin_services(&list)).unwrap();
        assert_eq!(
            svc,
            AdminService {
                name: "rel-pingap-admin".into(),
                namespace: "ns".into(),
                port: 3018
            }
        );
        let msg = admin_message(&svc, 3018, None);
        assert!(msg.contains("http://127.0.0.1:3018"), "{msg}");
        assert!(
            msg.contains(
                "get secret rel-pingap-admin -o go-template='{{.data.password | base64decode}}'"
            ),
            "{msg}"
        );
        assert!(!msg.contains("password: s3cret"));
        assert!(admin_message(&svc, 3018, Some("s3cret")).contains("password: s3cret"));
        assert!(find_admin_services(&serde_json::json!({"items": []})).is_empty());
        let err = single_admin_service(Vec::new()).unwrap_err().to_string();
        assert!(
            err.contains("app.kubernetes.io/part-of=pingap-k8s"),
            "{err}"
        );
        let two = vec![
            svc,
            AdminService {
                name: "other-admin".into(),
                namespace: "evil".into(),
                port: 3018,
            },
        ];
        let err = single_admin_service(two).unwrap_err().to_string();
        assert!(
            err.contains("more than one") && err.contains("evil/other-admin"),
            "{err}"
        );
    }

    #[test]
    fn service_keys() {
        assert_eq!(service_key("web.prod:80 9").as_deref(), Some("prod/web"));
        assert_eq!(service_key("api.prod:http").as_deref(), Some("prod/api"));
    }

    #[test]
    fn cli_parses() {
        let c = Cli::try_parse_from(["kubectl-pingap", "status", "-n", "edge"]).unwrap();
        assert_eq!(c.namespace.as_deref(), Some("edge"));
        let c = Cli::try_parse_from([
            "kubectl-pingap",
            "-n",
            "edge",
            "explain",
            "web",
            "--port",
            "1",
        ])
        .unwrap();
        assert!(
            matches!(c.command, Cmd::Explain { ref upstream, ref target } if upstream == "web" && target.port == Some(1))
        );
        let c = Cli::try_parse_from(["kubectl-pingap", "admin", "--show-password"]).unwrap();
        assert!(matches!(
            c.command,
            Cmd::Admin {
                show_password: true,
                ..
            }
        ));
        assert!(Cli::try_parse_from(["kubectl-pingap"]).is_err());
    }
}
