//! Timestamped observations for the timing tests: a streamed command's output
//! lines, an etcd watch of one upstream's `addrs`, a request loop through Pingap, and a
//! pod tracker. Every observation carries the wall-clock time it was seen at on the test
//! host, so delays are differences on one clock. Pod `Ready` transition times from the
//! API (whole seconds) are kept too, for comparison.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime};

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};

use crate::harness::Env;
use crate::kubectl::Kubectl;

type Lines = Arc<Mutex<Vec<(SystemTime, String)>>>;

/// Signed seconds `a - b`.
pub fn secs_between(a: SystemTime, b: SystemTime) -> f64 {
    match a.duration_since(b) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}

/// A child process whose stdout lines are read on a thread and timestamped on arrival.
/// The process is killed when the stream is stopped or dropped.
pub struct LineStream {
    child: Option<Child>,
    lines: Lines,
    reader: Option<JoinHandle<()>>,
}

impl LineStream {
    /// `kubectl exec -n ns pod [-c container] -- cmd...`, streamed (stderr discarded).
    pub fn exec<S: AsRef<str>>(
        kubectl: &Kubectl,
        ns: &str,
        pod: &str,
        container: Option<&str>,
        cmd: &[S],
    ) -> anyhow::Result<LineStream> {
        let mut args = vec!["exec".to_string(), "-n".into(), ns.into(), pod.into()];
        if let Some(c) = container {
            args.extend(["-c".into(), c.into()]);
        }
        args.push("--".into());
        args.extend(cmd.iter().map(|a| a.as_ref().to_string()));
        let child = kubectl
            .command(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        Ok(Self::start(child))
    }

    fn start(mut child: Child) -> LineStream {
        let stdout = child.stdout.take().expect("piped stdout");
        let lines: Lines = Arc::default();
        let sink = lines.clone();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                sink.lock().unwrap().push((SystemTime::now(), line));
            }
        });
        LineStream {
            child: Some(child),
            lines,
            reader: Some(reader),
        }
    }

    /// Lines so far.
    pub fn lines(&self) -> Vec<(SystemTime, String)> {
        self.lines.lock().unwrap().clone()
    }

    /// Kills the process and returns every line it printed.
    pub fn stop(mut self) -> Vec<(SystemTime, String)> {
        self.kill();
        self.lines()
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        if let Some(r) = self.reader.take() {
            let _ = r.join();
        }
    }
}

impl Drop for LineStream {
    fn drop(&mut self) {
        self.kill();
    }
}

/// One value of an upstream's `addrs` and when it was seen.
#[derive(Debug, Clone)]
pub struct AddrsChange {
    pub at: SystemTime,
    pub mod_revision: i64,
    /// Sorted; `None` when the key was deleted or has no `addrs`.
    pub addrs: Option<Vec<String>>,
}

impl AddrsChange {
    pub fn contains(&self, addr: &str) -> bool {
        self.addrs
            .as_ref()
            .is_some_and(|a| a.iter().any(|x| x == addr))
    }
}

/// Watches `<prefix>/upstreams/<name>.toml` from the current revision on.
pub struct AddrsWatch {
    upstream: String,
    initial: AddrsChange,
    stream: LineStream,
}

impl AddrsWatch {
    pub async fn start(env: &Env, upstream: &str) -> anyhow::Result<AddrsWatch> {
        let rev = env.etcd.revision().await?;
        let kv = env.etcd.upstream(upstream).await?;
        let initial = AddrsChange {
            at: SystemTime::now(),
            mod_revision: kv.as_ref().map_or(0, |kv| kv.mod_revision),
            addrs: match &kv {
                Some(kv) => sorted(pingap_k8s::etcd::read_addrs(&kv.value, upstream)?),
                None => None,
            },
        };
        let stream = env
            .etcd
            .watch_stream(&env.etcd.upstream_key(upstream), rev + 1)?;
        Ok(AddrsWatch {
            upstream: upstream.to_string(),
            initial,
            stream,
        })
    }

    /// The value at start, then every change seen since, in order.
    pub fn changes(&self) -> Vec<AddrsChange> {
        let mut out = vec![self.initial.clone()];
        for (at, line) in self.stream.lines() {
            let Ok(events) = crate::etcd::parse_watch_output(&line) else {
                continue;
            };
            for e in events {
                let addrs = if e.kind == "DELETE" {
                    None
                } else {
                    pingap_k8s::etcd::read_addrs(&e.kv.value, &self.upstream)
                        .ok()
                        .flatten()
                        .and_then(|a| sorted(Some(a)))
                };
                out.push(AddrsChange {
                    at,
                    mod_revision: e.kv.mod_revision,
                    addrs,
                });
            }
        }
        out
    }

    /// Number of writes seen since start.
    pub fn writes(&self) -> usize {
        self.changes().len() - 1
    }
}

fn sorted(a: Option<Vec<String>>) -> Option<Vec<String>> {
    a.map(|mut a| {
        a.sort();
        a
    })
}

/// The value in force at `after` if it satisfies `pred`, else the first later change that
/// does.
pub fn first_change(
    changes: &[AddrsChange],
    after: SystemTime,
    pred: impl Fn(&AddrsChange) -> bool,
) -> Option<&AddrsChange> {
    if let Some(c) = changes.iter().rfind(|c| c.at <= after)
        && pred(c)
    {
        return Some(c);
    }
    changes.iter().find(|c| c.at > after && pred(c))
}

/// One request of a [`Traffic`] loop.
#[derive(Debug, Clone)]
pub struct Hit {
    pub at: SystemTime,
    /// 0: no response (curl error or timeout).
    pub status: u16,
    /// Response body (the echo pod's name for /hostname) for a 200, else "-" followed by
    /// curl's exit code, the request time and Pingap's address, e.g.
    /// "-curl=52,t=0.004,ip=10.96.0.12".
    pub host: String,
}

/// Sequential GETs through Pingap from the client pod, one line per response, for at
/// most `max` (the loop ends by itself even if the stream is not stopped). Each request
/// has a 5 s timeout, the same as the fortio runs ([`crate::load::LoadOptions`]); a
/// request without a response in that time is recorded with status 0.
pub struct Traffic {
    stream: LineStream,
}

impl Traffic {
    pub fn start(env: &Env, path: &str, max: Duration) -> anyhow::Result<Traffic> {
        let script = format!(
            "end=$(( $(date +%s) + {secs} )); f=/tmp/e2e-traffic.$$; \
             while [ $(date +%s) -lt $end ]; do \
               w=$(curl -s -m 5 -o $f -w '%{{http_code}} %{{time_total}} %{{remote_ip}}' '{url}{path}'); \
               rc=$?; code=${{w%% *}}; \
               if [ \"$code\" = 200 ]; then h=$(head -c 200 $f | tr -d '\\n '); \
               else h=\"-curl=$rc,t=$(echo $w | cut -d' ' -f2),ip=$(echo $w | cut -d' ' -f3)\"; fi; \
               echo \"$code $h\"; \
             done; rm -f $f",
            secs = max.as_secs().max(1),
            url = env.pingap.url,
        );
        let stream = LineStream::exec(
            &env.kubectl,
            &env.pingap.client_namespace,
            &env.pingap.client_pod,
            None,
            &["sh", "-c", &script],
        )?;
        Ok(Traffic { stream })
    }

    pub fn hits(&self) -> Vec<Hit> {
        parse_hits(&self.stream.lines())
    }

    pub fn stop(self) -> Vec<Hit> {
        parse_hits(&self.stream.stop())
    }
}

fn parse_hits(lines: &[(SystemTime, String)]) -> Vec<Hit> {
    lines
        .iter()
        .filter_map(|(at, line)| {
            let (code, host) = line.trim().split_once(' ')?;
            Some(Hit {
                at: *at,
                status: code.parse().ok()?,
                host: host.to_string(),
            })
        })
        .collect()
}

/// Requests without a 2xx response.
pub fn failures(hits: &[Hit]) -> Vec<&Hit> {
    hits.iter()
        .filter(|h| !(200..300).contains(&h.status))
        .collect()
}

/// What the [`PodTracker`] saw of one pod.
#[derive(Debug, Clone)]
pub struct PodSeen {
    pub ip: Option<String>,
    /// Existed when the tracker started.
    pub preexisting: bool,
    pub first_seen: SystemTime,
    /// First poll that saw it Ready and not terminating.
    pub ready: Option<SystemTime>,
    /// `lastTransitionTime` of its Ready condition at that poll (whole seconds).
    pub ready_transition: Option<SystemTime>,
    /// First poll that saw a deletion timestamp.
    pub terminating: Option<SystemTime>,
    /// First poll that no longer listed it.
    pub gone: Option<SystemTime>,
}

/// Polls the pods matching a selector every 100 ms in a background task.
pub struct PodTracker {
    seen: Arc<Mutex<BTreeMap<String, PodSeen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl PodTracker {
    pub async fn start(client: &Client, ns: &str, selector: &str) -> anyhow::Result<PodTracker> {
        let api: Api<Pod> = Api::namespaced(client.clone(), ns);
        let seen: Arc<Mutex<BTreeMap<String, PodSeen>>> = Arc::default();
        let lp = ListParams::default().labels(selector);
        poll_pods(&api, &lp, &seen, true).await?;
        let task = {
            let seen = seen.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = poll_pods(&api, &lp, &seen, false).await;
                }
            })
        };
        Ok(PodTracker { seen, task })
    }

    pub fn snapshot(&self) -> BTreeMap<String, PodSeen> {
        self.seen.lock().unwrap().clone()
    }

    /// Pods created after the tracker started that became Ready.
    pub fn new_ready(&self) -> BTreeMap<String, PodSeen> {
        self.snapshot()
            .into_iter()
            .filter(|(_, p)| !p.preexisting && p.ready.is_some())
            .collect()
    }
}

impl Drop for PodTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn poll_pods(
    api: &Api<Pod>,
    lp: &ListParams,
    seen: &Mutex<BTreeMap<String, PodSeen>>,
    initial: bool,
) -> anyhow::Result<()> {
    let list = api.list(lp).await?;
    let now = SystemTime::now();
    let mut seen = seen.lock().unwrap();
    let mut listed = Vec::new();
    for p in list.items {
        let name = p.metadata.name.clone().unwrap_or_default();
        listed.push(name.clone());
        let status = p.status.clone().unwrap_or_default();
        let ready_cond = status
            .conditions
            .unwrap_or_default()
            .into_iter()
            .find(|c| c.type_ == "Ready" && c.status == "True");
        let terminating = p.metadata.deletion_timestamp.is_some();
        let e = seen.entry(name).or_insert_with(|| PodSeen {
            ip: None,
            preexisting: initial,
            first_seen: now,
            ready: None,
            ready_transition: None,
            terminating: None,
            gone: None,
        });
        if status.pod_ip.is_some() {
            e.ip = status.pod_ip.clone();
        }
        if let Some(c) = ready_cond
            && !terminating
            && e.ready.is_none()
        {
            e.ready = Some(now);
            e.ready_transition = c.last_transition_time.map(|t| SystemTime::from(t.0));
        }
        if terminating && e.terminating.is_none() {
            e.terminating = Some(now);
        }
    }
    for (name, p) in seen.iter_mut() {
        if p.gone.is_none() && !listed.contains(name) {
            p.gone = Some(now);
        }
    }
    Ok(())
}
