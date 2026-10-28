//! pingap-k8s binary: argument parsing, logging, and the wiring of config,
//! watches, reconcile, leader election, the metrics server, config reload and
//! graceful shutdown. The logic lives in the library modules.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use clap::{Parser, ValueEnum};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use pingap_k8s::config::{Config, ConfigError};
use pingap_k8s::etcd::{EtcdError, EtcdTarget, EtcdWriter, parse_etcd_url};
use pingap_k8s::events::Events;
use pingap_k8s::leader;
use pingap_k8s::metrics::{EtcdHealth, Metrics};
use pingap_k8s::reconcile::Reconciler;
use pingap_k8s::server::{self, Readiness};
use pingap_k8s::status::{self, StatusBoard, StatusSources};
use pingap_k8s::telemetry::Telemetry;
use pingap_k8s::types::{ForeignWrite, ServiceKey, UpstreamTarget};
use pingap_k8s::watch::WatchRegistry;

/// Default config path; matches the image's CMD and the Helm chart mount.
const DEFAULT_CONFIG_PATH: &str = "/etc/pingap-k8s/config/config.toml";
/// Service-account namespace file mounted into every pod.
const SA_NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
/// How long one wait for the first list of all watchers lasts before the
/// missing ones are logged. The wait itself continues.
const SYNC_WAIT: Duration = Duration::from_secs(30);
/// Quiet period after a config file event before reloading.
const FILE_DEBOUNCE: Duration = Duration::from_secs(1);
/// How long a reload waits for the reconciler to switch to the new upstreams
/// before it stops the watchers only the old ones used. On timeout they keep
/// running (harmless) until the next reload.
const APPLY_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound for each shutdown step (the election resigns within ~6 s).
const SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(10);
/// Retry backoff for startup connections and the owned-key watch.
const RETRY_MIN: Duration = Duration::from_millis(500);
const RETRY_MAX: Duration = Duration::from_secs(30);

/// Exit code for an invalid config (startup only).
const EXIT_INVALID_CONFIG: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

/// Kubernetes controller that writes Service endpoints into Pingap's etcd config.
///
/// The log level is set with RUST_LOG (EnvFilter syntax, default "info").
#[derive(Debug, Parser)]
#[command(name = "pingap-k8s", version)]
struct Args {
    /// Controller config file (TOML).
    #[arg(long, env = "PINGAP_K8S_CONFIG", value_name = "PATH", default_value = DEFAULT_CONFIG_PATH)]
    config: PathBuf,

    /// Log output format.
    #[arg(long, env = "PINGAP_K8S_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,

    /// Load and validate the config and the etcd URL, print OK and exit
    /// (0 = valid, 1 = invalid). Connects to nothing.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let identity = detect_identity(std::env::var_os("POD_NAME"), hostname);
    let telemetry = match Telemetry::init(identity, !args.check) {
        Ok(telemetry) => telemetry,
        Err(err) => {
            eprintln!("invalid OpenTelemetry configuration: {err:#}");
            return ExitCode::from(EXIT_INVALID_CONFIG);
        }
    };
    init_logging(args.log_format, &telemetry);

    let code = main_inner(args, telemetry.metrics.clone()).await;
    telemetry.shutdown().await;
    code
}

async fn main_inner(args: Args, metrics: Arc<Metrics>) -> ExitCode {
    let namespace = pod_namespace();
    if args.check {
        return check(&args.config, &namespace);
    }

    let (config, target) = match load(&args.config, &namespace) {
        Ok(loaded) => loaded,
        Err(err) => {
            error!(config = %args.config.display(), "invalid config: {err:#}");
            return ExitCode::from(EXIT_INVALID_CONFIG);
        }
    };

    match run(args.config, namespace, config, target, metrics).await {
        Ok(()) => {
            info!("stopped");
            ExitCode::SUCCESS
        }
        Err(err) => {
            error!("fatal: {err:#}");
            ExitCode::FAILURE
        }
    }
}

// ---------------------------------------------------------------------------
// Startup helpers

/// stderr always; OTLP log records too when telemetry has an endpoint.
fn init_logging(format: LogFormat, telemetry: &Telemetry) {
    use tracing_subscriber::{EnvFilter, Layer as _, prelude::*};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let fmt = match format {
        LogFormat::Text => fmt.boxed(),
        LogFormat::Json => fmt.json().flatten_event(true).boxed(),
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(telemetry.log_layer())
        .init();
}

#[derive(Debug, thiserror::Error)]
enum LoadError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("etcd.url: {0}")]
    EtcdUrl(#[from] EtcdError),
}

/// Loads and validates the config file and its etcd URL. No I/O besides
/// reading the file.
fn load(path: &Path, namespace: &str) -> Result<(Config, EtcdTarget), LoadError> {
    let config = Config::load(path, namespace)?;
    let target = parse_etcd_url(&config.etcd.url)?;
    Ok((config, target))
}

/// `--check`: validate and report, without connecting anywhere.
fn check(path: &Path, namespace: &str) -> ExitCode {
    match load(path, namespace) {
        Ok((config, target)) => {
            println!(
                "OK: {} upstream(s), {} Service(s), etcd {:?} prefix {:?}",
                config.upstreams.len(),
                config.service_keys().len(),
                target.endpoints,
                target.prefix
            );
            for line in target_lines(&config) {
                println!("{line}");
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("invalid config {}: {err}", path.display());
            ExitCode::FAILURE
        }
    }
}

/// `--check`: one line per upstream with its target mode.
fn target_lines(config: &Config) -> Vec<String> {
    config
        .upstreams
        .iter()
        .map(|(name, up)| match &up.options.target {
            UpstreamTarget::Pods => format!("  upstream {name:?}: target pods"),
            UpstreamTarget::Service { cluster_domain } => {
                format!("  upstream {name:?}: target service (*.svc.{cluster_domain})")
            }
        })
        .collect()
}

/// Where the pod namespace came from.
#[derive(Debug, PartialEq, Eq)]
enum NamespaceSource {
    Env,
    ServiceAccount,
    Default,
}

/// POD_NAMESPACE, else the service-account namespace file, else "default".
fn detect_namespace(env: Option<OsString>, sa_file: &Path) -> (String, NamespaceSource) {
    if let Some(ns) = env.and_then(non_empty) {
        return (ns, NamespaceSource::Env);
    }
    if let Some(ns) = std::fs::read_to_string(sa_file)
        .ok()
        .and_then(|s| non_empty(s.into()))
    {
        return (ns, NamespaceSource::ServiceAccount);
    }
    ("default".to_string(), NamespaceSource::Default)
}

fn pod_namespace() -> String {
    let (ns, source) = detect_namespace(
        std::env::var_os("POD_NAMESPACE"),
        Path::new(SA_NAMESPACE_FILE),
    );
    if source == NamespaceSource::Default {
        warn!(
            "POD_NAMESPACE is not set and {SA_NAMESPACE_FILE} is not readable; \
             using namespace \"default\" for Services without a namespace"
        );
    }
    ns
}

/// POD_NAME, else the hostname, else a pid-based fallback.
fn detect_identity(
    pod_name: Option<OsString>,
    hostname: impl FnOnce() -> Option<String>,
) -> String {
    pod_name
        .and_then(non_empty)
        .or_else(hostname)
        .unwrap_or_else(|| format!("pingap-k8s-{}", std::process::id()))
}

fn hostname() -> Option<String> {
    if let Some(h) = std::env::var_os("HOSTNAME").and_then(non_empty) {
        return Some(h);
    }
    if let Some(h) = std::fs::read_to_string("/etc/hostname")
        .ok()
        .and_then(|s| non_empty(s.into()))
    {
        return Some(h);
    }
    let out = std::process::Command::new("hostname").output().ok()?;
    if !out.status.success() {
        return None;
    }
    non_empty(OsString::from(
        String::from_utf8_lossy(&out.stdout).into_owned(),
    ))
}

fn non_empty(value: OsString) -> Option<String> {
    let s = value.to_string_lossy().trim().to_string();
    (!s.is_empty()).then_some(s)
}

/// Election name per T09: `<prefix without trailing slash>-k8s-election`.
fn election_name(prefix: &str) -> String {
    format!("{}-k8s-election", prefix.trim_end_matches('/'))
}

/// Kubernetes client per `[kubernetes]`: an explicit kubeconfig file (with an
/// optional context); otherwise in-cluster, then $KUBECONFIG, then
/// ~/.kube/config. A context without a kubeconfig skips in-cluster.
async fn kube_client(
    section: &pingap_k8s::config::KubernetesSection,
) -> anyhow::Result<kube::Client> {
    use kube::config::{KubeConfigOptions, Kubeconfig};
    let options = KubeConfigOptions {
        context: non_empty(section.context.clone().into()),
        ..Default::default()
    };
    let mut config = if !section.kubeconfig.is_empty() {
        let kubeconfig = Kubeconfig::read_from(&section.kubeconfig)
            .with_context(|| format!("reading kubeconfig {}", section.kubeconfig))?;
        kube::Config::from_custom_kubeconfig(kubeconfig, &options)
            .await
            .context("loading kubeconfig")?
    } else if options.context.is_some() {
        kube::Config::from_kubeconfig(&options)
            .await
            .context("loading kubeconfig ($KUBECONFIG or ~/.kube/config)")?
    } else {
        match kube::Config::incluster() {
            Ok(config) => config,
            Err(in_cluster) => kube::Config::from_kubeconfig(&options)
                .await
                .map_err(|kc| {
                    anyhow::anyhow!(
                        "no Kubernetes config: in-cluster: {in_cluster}; kubeconfig: {kc}"
                    )
                })?,
        }
    };
    if enforce_tls_verify(&mut config) {
        warn!(
            "the kubeconfig disables TLS verification (insecure-skip-tls-verify); \
             ignored: the API server certificate is always verified"
        );
    }
    info!(cluster = %config.cluster_url, "Kubernetes API");
    kube::Client::try_from(config).context("creating Kubernetes client")
}

/// API server TLS is always verified, whatever the kubeconfig says.
/// Returns true when verification had been turned off.
fn enforce_tls_verify(config: &mut kube::Config) -> bool {
    std::mem::replace(&mut config.accept_invalid_certs, false)
}

// ---------------------------------------------------------------------------
// Shutdown

/// A future that completes once shutdown was requested (or the sender is gone).
async fn shutdown_signal(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// What triggered a reload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Trigger {
    Sighup,
    File,
}

impl Trigger {
    fn as_str(self) -> &'static str {
        match self {
            Trigger::Sighup => "SIGHUP",
            Trigger::File => "file change",
        }
    }
}

/// Installs SIGTERM/SIGINT (shutdown; a second one exits at once) and SIGHUP
/// (reload) handlers.
fn spawn_signal_handler(
    shutdown: watch::Sender<bool>,
    reload: mpsc::Sender<Trigger>,
) -> anyhow::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).context("installing SIGTERM handler")?;
    let mut int = signal(SignalKind::interrupt()).context("installing SIGINT handler")?;
    let mut hup = signal(SignalKind::hangup()).context("installing SIGHUP handler")?;
    tokio::spawn(async move {
        loop {
            let name = tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
                _ = hup.recv() => {
                    info!("SIGHUP received; reloading config");
                    let _ = reload.try_send(Trigger::Sighup);
                    continue;
                }
            };
            if *shutdown.borrow() {
                warn!(signal = name, "second shutdown signal; exiting now");
                std::process::exit(1);
            }
            info!(signal = name, "shutting down");
            let _ = shutdown.send(true);
        }
    });
    Ok(())
}

/// Runs `f` until it succeeds, with backoff. `None` when shutdown came first.
async fn retry<T, F, Fut>(what: &str, shutdown: &watch::Receiver<bool>, mut f: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut delay = RETRY_MIN;
    loop {
        let result = tokio::select! {
            r = f() => r,
            () = shutdown_signal(shutdown.clone()) => return None,
        };
        match result {
            Ok(value) => return Some(value),
            Err(err) => warn!(retry_in = ?delay, "{what} failed: {err:#}"),
        }
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = shutdown_signal(shutdown.clone()) => return None,
        }
        delay = (delay * 2).min(RETRY_MAX);
    }
}

// ---------------------------------------------------------------------------
// Main run

type AppReconciler = Reconciler<EtcdWriter, WatchRegistry>;

async fn run(
    config_path: PathBuf,
    namespace: String,
    config: Config,
    target: EtcdTarget,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    info!(
        config = %config_path.display(),
        %namespace,
        upstreams = config.upstreams.len(),
        services = config.service_keys().len(),
        "starting pingap-k8s {}",
        env!("CARGO_PKG_VERSION")
    );
    // Reads the TLS files; a missing file is fatal rather than retried.
    target.connect_options().context("etcd TLS/auth options")?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (reload_tx, reload_rx) = mpsc::channel::<Trigger>(8);
    spawn_signal_handler(shutdown_tx.clone(), reload_tx.clone())?;

    // The metrics server first, so /healthz answers during startup.
    let readiness = Readiness::new();
    let listener = TcpListener::bind(config.server.listen)
        .await
        .with_context(|| format!("binding metrics server to {}", config.server.listen))?;
    let server_task = tokio::spawn(server::serve_listener(
        listener,
        metrics.clone(),
        readiness.clone(),
        shutdown_signal(shutdown_rx.clone()),
    ));
    // Optional read-only /status, loopback only.
    let status_board = StatusBoard::new();
    if let Some(addr) = config.server.status_listen {
        let listener = status::bind(addr).await?;
        let identity = detect_identity(std::env::var_os("POD_NAME"), hostname);
        let sources =
            StatusSources::new(identity, &target, status_board.clone(), readiness.clone());
        let stop = shutdown_signal(shutdown_rx.clone());
        tokio::spawn(async move {
            if let Err(err) = status::serve_listener(listener, sources, stop).await {
                error!("{err:#}");
            }
        });
    }

    let client = kube_client(&config.kubernetes).await?;
    let events = kube_events(&config, &client, &namespace);
    let etcd_health = EtcdHealth::new(metrics.clone(), events.clone());

    let Some(writer) = retry("connecting to etcd", &shutdown_rx, || {
        EtcdWriter::connect(&target)
    })
    .await
    else {
        return finish(server_task, None, None).await;
    };
    let writer = writer.with_etcd_health(etcd_health.clone());
    etcd_health.report(true);
    info!(endpoints = ?target.endpoints, prefix = writer.prefix(), "connected to etcd");

    let registry = WatchRegistry::new(client, metrics.clone());
    registry.set_services(config.service_keys());
    status_board.set_watches(Arc::new(registry.clone()));
    // never write a partial set, so nothing starts before every watcher listed once.
    if !wait_synced(&registry, &readiness, &mut reload_drain(), &shutdown_rx).await {
        return finish(server_task, None, None).await;
    }

    // Leader election, on its own client: a follower's Campaign blocks until it is
    // elected and must not be cut by the request timeout of the KV client.
    let Some(election_client) = retry("connecting to etcd (election)", &shutdown_rx, || {
        EtcdWriter::connect_election(&target)
    })
    .await
    else {
        return finish(server_task, None, None).await;
    };
    let (leader_tx, leader_rx) = watch::channel(false);
    let election = tokio::spawn(leader::run_election_with_health(
        election_client,
        election_name(writer.prefix()),
        leader::DEFAULT_TTL,
        detect_identity(std::env::var_os("POD_NAME"), hostname),
        leader_tx,
        shutdown_signal(shutdown_rx.clone()),
        Some(etcd_health),
    ));
    spawn_leader_metric(leader_rx.clone(), metrics.clone());

    // Owned-key watch.
    let names = owned_names(&config);
    let Some(foreign) = retry("watching owned etcd keys", &shutdown_rx, || {
        writer.watch_owned(names.clone())
    })
    .await
    else {
        return finish(server_task, Some(election), None).await;
    };

    // Reconcile loop.
    let reconciler: AppReconciler = Reconciler::new(
        Arc::new(writer.clone()),
        Arc::new(registry.clone()),
        metrics.clone(),
        readiness.clone(),
    )
    .with_status(status_board)
    .with_events(events);
    reconciler.set_upstreams(config.upstreams.clone());
    let reconcile_task = tokio::spawn(reconciler.clone().run(
        registry.subscribe(),
        foreign,
        leader_rx,
        config.defaults.resync,
        shutdown_signal(shutdown_rx.clone()),
    ));
    info!("reconciler started");

    // Config reload until shutdown.
    let _file_watcher = watch_config_file(&config_path, reload_tx);
    let mut controller = Controller {
        path: config_path,
        namespace,
        boot: config.clone(),
        current: config,
        registry,
        reconciler,
        writer,
        readiness,
        owned_watch_pending: false,
    };
    controller.run(reload_rx, &shutdown_rx).await;

    finish(server_task, Some(election), Some(reconcile_task)).await
}

/// Kubernetes Events per `[kubernetes] events`, as this pod.
fn kube_events(config: &Config, client: &kube::Client, namespace: &str) -> Events {
    if !config.kubernetes.events {
        return Events::disabled();
    }
    Events::spawn(
        client.clone(),
        detect_identity(std::env::var_os("POD_NAME"), hostname),
        namespace.to_string(),
        std::env::var("POD_UID").ok(),
    )
}

/// A reload channel with no sender: startup waits are not superseded by reloads
/// (SIGHUP during startup is queued in the real channel and handled afterwards).
fn reload_drain() -> mpsc::Receiver<Trigger> {
    mpsc::channel(1).1
}

/// Waits for the reconciler, the election (which resigns) and the HTTP server,
/// each bounded, all in parallel.
async fn finish(
    server_task: JoinHandle<anyhow::Result<()>>,
    election: Option<JoinHandle<()>>,
    reconcile: Option<JoinHandle<()>>,
) -> anyhow::Result<()> {
    async fn bounded<T>(what: &str, task: Option<JoinHandle<T>>) -> Option<T> {
        let task = task?;
        match tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, task).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(err)) => {
                warn!("{what} task failed: {err}");
                None
            }
            Err(_) => {
                warn!("{what} did not stop within {SHUTDOWN_STEP_TIMEOUT:?}");
                None
            }
        }
    }
    let (_, _, served) = tokio::join!(
        bounded("reconciler", reconcile),
        bounded("leader election", election),
        bounded("metrics server", Some(server_task)),
    );
    match served {
        Some(Err(err)) => Err(err),
        _ => Ok(()),
    }
}

fn spawn_leader_metric(mut rx: watch::Receiver<bool>, metrics: Arc<Metrics>) {
    tokio::spawn(async move {
        loop {
            let leader = *rx.borrow_and_update();
            metrics.set_leader(leader);
            if rx.changed().await.is_err() {
                metrics.set_leader(false);
                return;
            }
        }
    });
}

fn owned_names(config: &Config) -> Vec<String> {
    config.upstreams.keys().cloned().collect()
}

/// How a wait or reload ended.
#[derive(Debug, PartialEq, Eq)]
enum Flow {
    Done,
    /// A newer reload trigger arrived; it is returned to the caller.
    Superseded(Trigger),
    Shutdown,
}

/// Waits until every current watcher has listed once, logging the Services that
/// are still missing every [`SYNC_WAIT`]. Returns false on shutdown.
async fn wait_synced(
    registry: &WatchRegistry,
    readiness: &Readiness,
    triggers: &mut mpsc::Receiver<Trigger>,
    shutdown: &watch::Receiver<bool>,
) -> bool {
    matches!(
        wait_synced_or_reload(registry, readiness, triggers, shutdown).await,
        Flow::Done
    )
}

async fn wait_synced_or_reload(
    registry: &WatchRegistry,
    readiness: &Readiness,
    triggers: &mut mpsc::Receiver<Trigger>,
    shutdown: &watch::Receiver<bool>,
) -> Flow {
    let mut triggers_open = true;
    loop {
        let synced = tokio::select! {
            synced = registry.wait_ready(SYNC_WAIT) => synced,
            t = triggers.recv(), if triggers_open => match t {
                Some(t) => return Flow::Superseded(t),
                None => { triggers_open = false; continue; }
            },
            () = shutdown_signal(shutdown.clone()) => return Flow::Shutdown,
        };
        readiness.set_watchers_synced(synced);
        if synced {
            info!(services = registry.services().len(), "all watchers synced");
            return Flow::Done;
        }
        let missing = registry.unsynced();
        warn!(
            waited = ?SYNC_WAIT,
            not_synced = %join_keys(&missing),
            "watchers have not completed their first list; not writing until they have, still waiting"
        );
    }
}

fn join_keys(keys: &BTreeSet<ServiceKey>) -> String {
    if keys.is_empty() {
        return "(none; synced meanwhile)".to_string();
    }
    keys.iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------------------------------------------------------------------------
// Config reload

/// Watches the config file's directory (Kubernetes swaps a `..data` symlink
/// there on ConfigMap updates). `None` (with a warning) if it cannot be
/// watched; SIGHUP still works then.
fn watch_config_file(path: &Path, tx: mpsc::Sender<Trigger>) -> Option<notify::RecommendedWatcher> {
    use notify::{EventKind, RecursiveMode, Watcher as _};
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let name = path.file_name()?.to_os_string();
    let watcher =
        notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
            Ok(event) => {
                if matches!(event.kind, EventKind::Access(_)) {
                    return;
                }
                if event.paths.iter().any(|p| is_config_path(p, &name)) {
                    let _ = tx.try_send(Trigger::File);
                }
            }
            Err(err) => warn!("config file watch error: {err}"),
        });
    let mut watcher = match watcher {
        Ok(w) => w,
        Err(err) => {
            warn!("cannot watch config file changes ({err}); reload with SIGHUP");
            return None;
        }
    };
    if let Err(err) = watcher.watch(&dir, RecursiveMode::NonRecursive) {
        warn!(dir = %dir.display(), "cannot watch config directory ({err}); reload with SIGHUP");
        return None;
    }
    info!(dir = %dir.display(), "watching config directory for changes");
    Some(watcher)
}

/// The config file itself, or a Kubernetes ConfigMap `..data`/`..<timestamp>` entry.
fn is_config_path(path: &Path, name: &std::ffi::OsStr) -> bool {
    match path.file_name() {
        Some(f) => f == name || f.to_string_lossy().starts_with(".."),
        None => false,
    }
}

/// Owns the running state that reloads change.
struct Controller {
    path: PathBuf,
    namespace: String,
    /// The config at startup: [etcd], [kubernetes], [server] and resync stay as here.
    boot: Config,
    /// The last applied config.
    current: Config,
    registry: WatchRegistry,
    reconciler: AppReconciler,
    writer: EtcdWriter,
    readiness: Readiness,
    /// The owned upstream set changed but restarting the owned-key watch failed.
    owned_watch_pending: bool,
}

impl Controller {
    async fn run(
        &mut self,
        mut triggers: mpsc::Receiver<Trigger>,
        shutdown: &watch::Receiver<bool>,
    ) {
        let mut next: Option<Trigger> = None;
        let mut retry_delay = RETRY_MIN;
        loop {
            let trigger = match next.take() {
                Some(t) => t,
                None => tokio::select! {
                    () = shutdown_signal(shutdown.clone()) => return,
                    t = triggers.recv() => match t {
                        Some(t) => t,
                        None => return,
                    },
                    () = tokio::time::sleep(retry_delay), if self.owned_watch_pending => {
                        if self.restart_owned_watch().await {
                            retry_delay = RETRY_MIN;
                        } else {
                            retry_delay = (retry_delay * 2).min(RETRY_MAX);
                        }
                        continue;
                    }
                },
            };
            let trigger = match debounce(trigger, &mut triggers, shutdown).await {
                Some(t) => t,
                None => return,
            };
            match self.reload(trigger, &mut triggers, shutdown).await {
                Flow::Done => {}
                Flow::Shutdown => return,
                Flow::Superseded(t) => {
                    info!(trigger = t.as_str(), "reload superseded by a newer one");
                    next = Some(t);
                }
            }
        }
    }

    /// Load, validate and apply the config. On error the old config stays.
    async fn reload(
        &mut self,
        trigger: Trigger,
        triggers: &mut mpsc::Receiver<Trigger>,
        shutdown: &watch::Receiver<bool>,
    ) -> Flow {
        let new = match load(&self.path, &self.namespace) {
            Ok((config, _)) => config,
            Err(err) => {
                error!(
                    trigger = trigger.as_str(),
                    config = %self.path.display(),
                    "config reload failed; keeping the running config: {err:#}"
                );
                return Flow::Done;
            }
        };
        if new == self.current {
            info!(trigger = trigger.as_str(), "config unchanged");
            return Flow::Done;
        }
        for field in restart_required(&self.boot, &new) {
            warn!(
                trigger = trigger.as_str(),
                "{field} changed; restart required to apply it (keeping the running value)"
            );
        }

        // Start watchers for new Services and wait for their first list before the
        // reconciler sees upstreams that use them. Old ones keep running until
        // the reconciler has switched.
        let new_keys = new.service_keys();
        let union: BTreeSet<ServiceKey> = self
            .current
            .service_keys()
            .union(&new_keys)
            .cloned()
            .collect();
        self.registry.set_services(union);
        if !self.registry.is_synced() {
            self.readiness.set_watchers_synced(false);
            let flow =
                wait_synced_or_reload(&self.registry, &self.readiness, triggers, shutdown).await;
            if flow != Flow::Done {
                return flow;
            }
        }

        let owned_changed = owned_names(&self.current) != owned_names(&new);
        let generation = self.reconciler.set_upstreams(new.upstreams.clone());
        self.current = new;
        if owned_changed && !self.restart_owned_watch().await {
            self.owned_watch_pending = true;
        }

        info!(
            trigger = trigger.as_str(),
            upstreams = self.current.upstreams.len(),
            services = new_keys.len(),
            owned_changed,
            "config reloaded"
        );
        // Stop watchers of Services no longer referenced, once the reconciler has
        // switched to the new upstreams: until then it may still read them, and a
        // Service that is not watched reads as missing (a partial set).
        let applied = tokio::select! {
            r = tokio::time::timeout(APPLY_TIMEOUT, self.reconciler.wait_applied(generation)) => r.is_ok(),
            () = shutdown_signal(shutdown.clone()) => return Flow::Shutdown,
        };
        if !applied {
            warn!(
                timeout = ?APPLY_TIMEOUT,
                "reconciler has not applied the new upstreams yet; keeping the watchers of removed Services until the next reload"
            );
            return Flow::Done;
        }
        self.registry.set_services(new_keys);
        self.readiness
            .set_watchers_synced(self.registry.is_synced());
        Flow::Done
    }

    /// Restarts the owned-key watch for the current upstream set and hands it to
    /// the reconciler, preceded by a Resync (events between the old and the new
    /// watch may have been missed). Returns false on failure.
    async fn restart_owned_watch(&mut self) -> bool {
        match self.writer.watch_owned(owned_names(&self.current)).await {
            Ok(rx) => {
                self.reconciler.set_foreign_writes(with_resync(rx));
                self.owned_watch_pending = false;
                info!(
                    upstreams = self.current.upstreams.len(),
                    "owned-key watch restarted"
                );
                true
            }
            Err(err) => {
                warn!("restarting the owned-key watch failed (will retry): {err:#}");
                false
            }
        }
    }
}

/// Waits for a quiet period after file events. A SIGHUP ends the wait at once.
/// `None` on shutdown.
async fn debounce(
    first: Trigger,
    triggers: &mut mpsc::Receiver<Trigger>,
    shutdown: &watch::Receiver<bool>,
) -> Option<Trigger> {
    if first == Trigger::Sighup {
        return Some(first);
    }
    loop {
        tokio::select! {
            () = shutdown_signal(shutdown.clone()) => return None,
            () = tokio::time::sleep(FILE_DEBOUNCE) => return Some(Trigger::File),
            t = triggers.recv() => match t {
                Some(Trigger::Sighup) => return Some(Trigger::Sighup),
                Some(Trigger::File) => continue,
                None => return Some(Trigger::File),
            },
        }
    }
}

/// The sections that cannot be hot-applied and differ from the running ones.
fn restart_required(boot: &Config, new: &Config) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if boot.etcd != new.etcd {
        fields.push("[etcd]");
    }
    // default_namespace and cluster_domain are already applied through the
    // upstreams' Service keys and targets.
    if boot.kubernetes.kubeconfig != new.kubernetes.kubeconfig
        || boot.kubernetes.context != new.kubernetes.context
    {
        fields.push("[kubernetes] kubeconfig/context");
    }
    if boot.kubernetes.events != new.kubernetes.events {
        fields.push("[kubernetes] events");
    }
    if boot.server != new.server {
        fields.push("[server]");
    }
    if boot.defaults.resync != new.defaults.resync {
        fields.push("defaults.resync");
    }
    fields
}

/// A receiver that yields `ForeignWrite::Resync` first, then everything from
/// `rx`. The forwarding task ends when either side closes.
fn with_resync(mut rx: mpsc::Receiver<ForeignWrite>) -> mpsc::Receiver<ForeignWrite> {
    let (tx, out) = mpsc::channel(256);
    tokio::spawn(async move {
        if tx.send(ForeignWrite::Resync).await.is_err() {
            return;
        }
        loop {
            tokio::select! {
                () = tx.closed() => return,
                item = rx.recv() => match item {
                    Some(item) => {
                        if tx.send(item).await.is_err() {
                            return;
                        }
                    }
                    None => return,
                },
            }
        }
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        Args::try_parse_from(std::iter::once("pingap-k8s").chain(args.iter().copied()))
    }

    #[test]
    fn check_prints_each_upstream_target() {
        let config = Config::from_toml_str(
            "[etcd]\nurl = \"etcd://e:2379/p\"\n\
             [kubernetes]\ncluster_domain = \"k.internal\"\n\
             [upstreams.a]\nservices = [\"a:80\"]\n\
             [upstreams.b]\nservices = [\"b:80\"]\ntarget = \"service\"\n",
            "ns",
        )
        .unwrap();
        assert_eq!(
            target_lines(&config),
            vec![
                "  upstream \"a\": target pods".to_string(),
                "  upstream \"b\": target service (*.svc.k.internal)".to_string(),
            ]
        );
    }

    fn env_free(var: &str) -> bool {
        std::env::var_os(var).is_none()
    }

    #[test]
    fn args_explicit() {
        let a = parse(&["--config", "/x/c.toml", "--log-format", "json", "--check"]).unwrap();
        assert_eq!(a.config, PathBuf::from("/x/c.toml"));
        assert_eq!(a.log_format, LogFormat::Json);
        assert!(a.check);
    }

    #[test]
    fn args_chart_and_image_form() {
        // Dockerfile CMD and the chart's args.
        let a = parse(&["--config", DEFAULT_CONFIG_PATH, "--log-format", "text"]).unwrap();
        assert_eq!(a.config, PathBuf::from(DEFAULT_CONFIG_PATH));
        assert_eq!(a.log_format, LogFormat::Text);
        assert!(!a.check);
        let a = parse(&["--config=/a.toml", "--log-format=json"]).unwrap();
        assert_eq!(a.config, PathBuf::from("/a.toml"));
        assert_eq!(a.log_format, LogFormat::Json);
    }

    #[test]
    fn args_defaults() {
        if !env_free("PINGAP_K8S_CONFIG") || !env_free("PINGAP_K8S_LOG_FORMAT") {
            return;
        }
        let a = parse(&[]).unwrap();
        assert_eq!(a.config, PathBuf::from(DEFAULT_CONFIG_PATH));
        assert_eq!(a.log_format, LogFormat::Text);
        assert!(!a.check);
    }

    #[test]
    fn args_rejects_bad_values() {
        assert!(parse(&["--log-format", "xml"]).is_err());
        assert!(parse(&["--unknown"]).is_err());
        assert!(parse(&["--config"]).is_err());
    }

    /// A fresh directory in the system temp dir, removed on drop (also when
    /// the test panics), so test runs leave nothing behind.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("pingap-k8s-main-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            TempDir(dir)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A `namespace` file with `content`; keep the guard alive while using the path.
    fn temp_file(name: &str, content: &str) -> (TempDir, PathBuf) {
        let dir = TempDir::new(name);
        let path = dir.0.join("namespace");
        std::fs::write(&path, content).unwrap();
        (dir, path)
    }

    #[test]
    fn temp_dir_is_removed_on_drop() {
        let (dir, file) = temp_file("drop", "x");
        assert!(file.exists());
        let path = dir.0.clone();
        drop(dir);
        assert!(!path.exists());
    }

    #[test]
    fn namespace_from_env_wins() {
        let (_dir, file) = temp_file("env", "from-file\n");
        let got = detect_namespace(Some("from-env".into()), &file);
        assert_eq!(got, ("from-env".to_string(), NamespaceSource::Env));
    }

    #[test]
    fn namespace_from_service_account_file() {
        let (_dir, file) = temp_file("sa", "edge\n");
        assert_eq!(
            detect_namespace(None, &file),
            ("edge".to_string(), NamespaceSource::ServiceAccount)
        );
        // An empty env value counts as unset.
        assert_eq!(
            detect_namespace(Some("  ".into()), &file),
            ("edge".to_string(), NamespaceSource::ServiceAccount)
        );
    }

    #[test]
    fn namespace_default_when_nothing_found() {
        let missing = Path::new("/nonexistent/pingap-k8s/namespace");
        assert_eq!(
            detect_namespace(None, missing),
            ("default".to_string(), NamespaceSource::Default)
        );
        let (_dir, empty) = temp_file("empty", "\n");
        assert_eq!(detect_namespace(None, &empty).1, NamespaceSource::Default);
    }

    #[test]
    fn identity_detection() {
        assert_eq!(
            detect_identity(Some("pod-1".into()), || Some("host".into())),
            "pod-1"
        );
        assert_eq!(
            detect_identity(Some("".into()), || Some("host".into())),
            "host"
        );
        assert_eq!(detect_identity(None, || Some("host".into())), "host");
        assert!(detect_identity(None, || None).starts_with("pingap-k8s-"));
    }

    #[test]
    fn tls_verification_is_always_on() {
        let mut config = kube::Config::new("https://127.0.0.1:6443".parse().unwrap());
        assert!(!enforce_tls_verify(&mut config));
        assert!(!config.accept_invalid_certs);
        config.accept_invalid_certs = true;
        assert!(enforce_tls_verify(&mut config));
        assert!(!config.accept_invalid_certs);
    }

    #[test]
    fn election_name_format() {
        assert_eq!(election_name("/pingap"), "/pingap-k8s-election");
        assert_eq!(election_name("/pingap/"), "/pingap-k8s-election");
    }

    #[test]
    fn config_path_filter() {
        let name = std::ffi::OsStr::new("config.toml");
        assert!(is_config_path(Path::new("/etc/x/config.toml"), name));
        assert!(is_config_path(Path::new("/etc/x/..data"), name));
        assert!(is_config_path(
            Path::new("/etc/x/..2026_10_25_10_00_00.123"),
            name
        ));
        assert!(!is_config_path(Path::new("/etc/x/other.toml"), name));
    }

    const BASE: &str = r#"
[etcd]
url = "etcd://127.0.0.1:2379/pingap"
[upstreams.api]
services = ["api.prod:http"]
"#;

    fn cfg(extra: &str) -> Config {
        Config::from_toml_str(&format!("{BASE}{extra}"), "default").unwrap()
    }

    #[test]
    fn restart_required_sections() {
        let base = cfg("");
        assert!(
            restart_required(&base, &cfg("[upstreams.web]\nservices = [\"web:80\"]\n")).is_empty()
        );
        assert!(
            restart_required(&base, &cfg("[kubernetes]\ndefault_namespace = \"x\"\n")).is_empty()
        );
        assert!(
            restart_required(&base, &cfg("[kubernetes]\ncluster_domain = \"x.y\"\n")).is_empty()
        );
        assert_eq!(
            restart_required(&base, &cfg("[server]\nlisten = \"127.0.0.1:1\"\n")),
            vec!["[server]"]
        );
        assert_eq!(
            restart_required(&base, &cfg("[kubernetes]\ncontext = \"c\"\n")),
            vec!["[kubernetes] kubeconfig/context"]
        );
        assert_eq!(
            restart_required(&base, &cfg("[defaults]\nresync = \"1m\"\n")),
            vec!["defaults.resync"]
        );
        assert_eq!(
            restart_required(&base, &cfg("[kubernetes]\nevents = true\n")),
            vec!["[kubernetes] events"]
        );
    }

    #[test]
    fn load_rejects_bad_etcd_url() {
        let dir = TempDir::new("url");
        let path = dir.0.join("c.toml");
        std::fs::write(&path, BASE.replace("127.0.0.1:2379/pingap", "/pingap")).unwrap();
        assert!(matches!(load(&path, "default"), Err(LoadError::EtcdUrl(_))));
        std::fs::write(&path, BASE).unwrap();
        let (config, target) = load(&path, "default").unwrap();
        assert_eq!(config.upstreams.len(), 1);
        assert_eq!(target.prefix, "/pingap");
        std::fs::write(&path, "nope").unwrap();
        assert!(matches!(load(&path, "default"), Err(LoadError::Config(_))));
    }

    #[tokio::test]
    async fn with_resync_prepends_resync() {
        let (tx, rx) = mpsc::channel(4);
        let mut out = with_resync(rx);
        tx.send(ForeignWrite::Deleted {
            upstream: "a".into(),
            revision: 3,
        })
        .await
        .unwrap();
        assert_eq!(out.recv().await, Some(ForeignWrite::Resync));
        assert_eq!(
            out.recv().await,
            Some(ForeignWrite::Deleted {
                upstream: "a".into(),
                revision: 3
            })
        );
        drop(tx);
        assert_eq!(out.recv().await, None);
    }

    #[tokio::test]
    async fn debounce_coalesces_file_events() {
        tokio::time::pause();
        let (tx, mut rx) = mpsc::channel(8);
        let (_stop_tx, stop_rx) = watch::channel(false);
        tx.send(Trigger::File).await.unwrap();
        tx.send(Trigger::File).await.unwrap();
        assert_eq!(
            debounce(Trigger::File, &mut rx, &stop_rx).await,
            Some(Trigger::File)
        );
        assert!(rx.try_recv().is_err());
        tx.send(Trigger::Sighup).await.unwrap();
        assert_eq!(
            debounce(Trigger::File, &mut rx, &stop_rx).await,
            Some(Trigger::Sighup)
        );
        let (stop_tx, stop_rx) = watch::channel(false);
        stop_tx.send(true).unwrap();
        assert_eq!(debounce(Trigger::File, &mut rx, &stop_rx).await, None);
    }
}
