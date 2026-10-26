//! Writes upstream `addrs` into Pingap's etcd config storage.
//!
//! Three parts:
//! - [`parse_etcd_url`]: Pingap's `--conf etcd://...` URL format, parsed the way
//!   `pingap-config/src/etcd_storage.rs` (`EtcdStorage::new`) parses it, so the
//!   controller and Pingap derive byte-identical keys from the same URL.
//! - Pure TOML helpers ([`replace_addrs`], [`read_addrs`], [`minimal_upstream`]) built on
//!   `toml_edit`, which keep comments and formatting and touch only
//!   `upstreams.<name>.addrs`.
//! - [`EtcdWriter`]: read-modify-write with an etcd `Txn` compare-and-swap, and
//!   [`EtcdWriter::watch_owned`], which reports writes to owned keys that the
//!   controller did not make.
//!
//! Key layout (Pingap `ConfigMode::MultiByItem`, `manager.rs` `get_key`): one key per
//! entry, `path_join("/" + <url path>, "upstreams/<name>.toml")`, holding a
//! `[upstreams.<name>]` table.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use etcd_client::{
    Certificate, Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, Identity,
    TlsOptions, Txn, TxnOp, WatchOptions, WatchStream,
};
use rand::RngExt as _;
use tokio::sync::{RwLock, mpsc};
use toml_edit::{Array, DocumentMut, Item, Table, TableLike, Value};
use tracing::{debug, info, warn};

use crate::types::{AddrsWriter, ForeignWrite, WriteOutcome};

/// The scheme of Pingap's etcd config URL.
pub const ETCD_PROTOCOL: &str = "etcd://";

/// Request timeout when the URL sets none (Pingap `DEFAULT_TIMEOUT`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Connect timeout when the URL sets none (Pingap `DEFAULT_CONNECT_TIMEOUT`).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// HTTP/2 keep-alive, as Pingap sets it (not configurable in the URL).
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How many times a failed compare-and-swap is retried.
pub const MAX_CAS_RETRIES: u32 = 5;
/// How many of the controller's own write revisions are remembered.
const OWN_REVISIONS_CAP: usize = 4096;
/// Capacity of the foreign-write channel returned by `watch_owned`.
const FOREIGN_CHANNEL_CAP: usize = 256;
const WATCH_BACKOFF_MIN: Duration = Duration::from_millis(500);
const WATCH_BACKOFF_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum EtcdError {
    #[error("invalid etcd url: {0}")]
    InvalidUrl(String),
    #[error("invalid upstream name {0:?}")]
    InvalidName(String),
    #[error("upstream TOML could not be parsed: {0}")]
    Toml(String),
    #[error("upstream TOML has no [upstreams.{0}] table")]
    MissingTable(String),
    #[error("upstreams.{0}.addrs is not a list of strings")]
    InvalidAddrs(String),
    #[error("value of etcd key {0} is not UTF-8")]
    NotUtf8(String),
    #[error("etcd TLS: {0}")]
    Tls(String),
    #[error("compare-and-swap on {key} failed {attempts} times in a row")]
    Conflict { key: String, attempts: u32 },
}

/// A parsed Pingap etcd URL:
/// `etcd://host:port[,host:port...]/<path>?user=..&password=..&timeout=..&connect_timeout=..`
/// `&tls=true&ca=..&cert=..&key=..&server_name=..&enable_history=..`.
#[derive(Clone, PartialEq, Eq)]
pub struct EtcdTarget {
    /// `host:port` entries as written (a scheme is added by etcd-client).
    pub endpoints: Vec<String>,
    /// `"/" + <url path>`, exactly as Pingap stores it (`"/pingap"`, `"/pingap/"`, `"/"`).
    pub prefix: String,
    /// Only applied when both user and password are set (as in Pingap).
    pub user: Option<String>,
    pub password: Option<String>,
    pub ca: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    /// `tls=true`; TLS is also implied by `ca`, `cert`, `key` or `server_name`.
    pub tls: bool,
    pub server_name: Option<String>,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    /// Pingap's own history switch. Parsed for completeness; controller writes never
    /// record history.
    pub enable_history: bool,
}

impl fmt::Debug for EtcdTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdTarget")
            .field("endpoints", &self.endpoints)
            .field("prefix", &self.prefix)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("ca", &self.ca)
            .field("cert", &self.cert)
            .field("key", &self.key)
            .field("tls", &self.tls)
            .field("server_name", &self.server_name)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("enable_history", &self.enable_history)
            .finish()
    }
}

impl EtcdTarget {
    /// Whether the connection uses TLS (Pingap: `tls` or any of ca/cert/key/server_name).
    pub fn tls_wanted(&self) -> bool {
        self.tls
            || self.ca.is_some()
            || self.cert.is_some()
            || self.key.is_some()
            || self.server_name.is_some()
    }

    /// The keys of `<prefix>/upstreams/<name>.toml` start with this.
    pub fn upstreams_prefix(&self) -> String {
        path_join(&self.prefix, "upstreams/")
    }

    /// Connection options equivalent to Pingap's. Reads the PEM files.
    pub fn connect_options(&self) -> Result<ConnectOptions, EtcdError> {
        Ok(self.base_options()?.with_timeout(self.timeout))
    }

    /// Like [`connect_options`](Self::connect_options) but without the client-wide
    /// request timeout, for the leader election only. A follower's `Campaign` call blocks
    /// until it is elected, which may take far longer than any request timeout; with one,
    /// the call is cancelled and the follower re-campaigns at the back of the queue.
    /// `leader.rs` bounds every other election RPC with its own timeout.
    pub fn election_connect_options(&self) -> Result<ConnectOptions, EtcdError> {
        self.base_options()
    }

    fn base_options(&self) -> Result<ConnectOptions, EtcdError> {
        let mut options = ConnectOptions::new();
        if let Some(tls) = self.tls_options()? {
            options = options.with_tls(tls);
        }
        match (&self.user, &self.password) {
            (Some(user), Some(password)) => {
                options = options.with_user(user.clone(), password.clone());
            }
            (None, None) => {}
            _ => warn!("etcd url sets only one of user and password; connecting without auth"),
        }
        Ok(options
            .with_connect_timeout(self.connect_timeout)
            .with_keep_alive(KEEP_ALIVE_INTERVAL, KEEP_ALIVE_TIMEOUT))
    }

    fn tls_options(&self) -> Result<Option<TlsOptions>, EtcdError> {
        if !self.tls_wanted() {
            return Ok(None);
        }
        let read = |what: &str, path: &PathBuf| -> Result<Vec<u8>, EtcdError> {
            let pem = std::fs::read(path).map_err(|e| {
                EtcdError::Tls(format!("{what} ({}) can not be read: {e}", path.display()))
            })?;
            if !pem.windows(11).any(|w| w == b"-----BEGIN ") {
                return Err(EtcdError::Tls(format!(
                    "{what} ({}) holds no PEM data",
                    path.display()
                )));
            }
            Ok(pem)
        };
        // Without `ca` the roots enabled at build time are used (none unless etcd-client's
        // `tls-native-roots`/`tls-webpki-roots` feature is on).
        let mut tls = TlsOptions::new().with_enabled_roots();
        if let Some(ca) = &self.ca {
            tls = tls.ca_certificate(Certificate::from_pem(read("ca", ca)?));
        }
        match (&self.cert, &self.key) {
            (Some(cert), Some(key)) => {
                tls = tls.identity(Identity::from_pem(read("cert", cert)?, read("key", key)?));
            }
            (None, None) => {}
            _ => return Err(EtcdError::Tls("cert and key should be set together".into())),
        }
        if let Some(name) = &self.server_name {
            tls = tls.domain_name(name.clone());
        }
        Ok(Some(tls))
    }
}

/// Pingap's `pingap_util::path_join`, byte for byte.
fn path_join(value1: &str, value2: &str) -> String {
    let end_slash = value1.ends_with('/');
    let value2 = match value2.strip_prefix('/') {
        Some(rest) if end_slash => rest,
        _ => value2,
    };
    let mut joined = String::with_capacity(value1.len() + value2.len() + 1);
    joined.push_str(value1);
    if !end_slash && !value2.starts_with('/') {
        joined.push('/');
    }
    joined.push_str(value2);
    joined
}

/// Pingap's `resolve_path`: `~` expands to `$HOME`. Relative paths stay relative and
/// are read against the working directory, which is what Pingap's absolutize gives.
fn resolve_path(path: &str) -> PathBuf {
    if (path == "~" || path.starts_with("~/"))
        && let Some(home) = std::env::var_os("HOME")
    {
        let mut expanded = PathBuf::from(home);
        if path.len() > 2 {
            expanded.push(&path[2..]);
        }
        return expanded;
    }
    PathBuf::from(path)
}

/// `application/x-www-form-urlencoded` decoding, as serde_qs does it: `+` is a space,
/// then `%XX` escapes. Invalid escapes are kept as written.
fn form_decode(input: &str) -> Result<String, EtcdError> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = |b: u8| (b as char).to_digit(16);
                match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                    (Some(h), Some(l)) => {
                        out.push((h * 16 + l) as u8);
                        i += 2;
                    }
                    _ => out.push(b'%'),
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out)
        .map_err(|_| EtcdError::InvalidUrl(format!("query value {input:?} is not UTF-8")))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, EtcdError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(EtcdError::InvalidUrl(format!(
            "{name}={value:?}: expected true or false"
        ))),
    }
}

fn parse_duration(name: &str, value: &str) -> Result<Duration, EtcdError> {
    humantime::parse_duration(value)
        .map_err(|e| EtcdError::InvalidUrl(format!("{name}={value:?}: {e}")))
}

/// Parses Pingap's etcd URL (`pingap-config/src/etcd_storage.rs`, `EtcdStorage::new`).
///
/// Matches Pingap on: optional `etcd://` scheme, comma-separated hosts, the path taken
/// verbatim with a `/` in front (no path is `/`), and the query parameters `user`,
/// `password`, `timeout`, `connect_timeout` (humantime), `enable_history`, `tls`, `ca`,
/// `cert`, `key`, `server_name`. Empty values mean "unset", unknown parameters are
/// ignored, as with Pingap's serde_qs. Stricter than Pingap (these would only fail
/// later at connect time there): no host or an empty host entry, a query with no path
/// before it (`etcd://h:2379?x=y`), a repeated parameter, and `cert` without `key`.
pub fn parse_etcd_url(url: &str) -> Result<EtcdTarget, EtcdError> {
    let rest = url.strip_prefix(ETCD_PROTOCOL).unwrap_or(url);
    let (hosts, path_query) = rest.split_once('/').unwrap_or((rest, ""));
    let (path, query) = path_query.split_once('?').unwrap_or((path_query, ""));
    if hosts.is_empty() {
        return Err(EtcdError::InvalidUrl(format!("{url} names no host")));
    }
    if hosts.contains('?') {
        return Err(EtcdError::InvalidUrl(format!(
            "{url}: put a path before the query, e.g. etcd://host:2379/pingap?..."
        )));
    }
    let endpoints: Vec<String> = hosts.split(',').map(str::to_string).collect();
    if endpoints.iter().any(String::is_empty) {
        return Err(EtcdError::InvalidUrl(format!(
            "{url} has an empty host entry"
        )));
    }

    let mut target = EtcdTarget {
        endpoints,
        prefix: format!("/{path}"),
        user: None,
        password: None,
        ca: None,
        cert: None,
        key: None,
        tls: false,
        server_name: None,
        timeout: DEFAULT_TIMEOUT,
        connect_timeout: DEFAULT_CONNECT_TIMEOUT,
        enable_history: false,
    };

    let mut seen = HashSet::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        let name = form_decode(name)?;
        let value = form_decode(value)?;
        if !seen.insert(name.clone()) {
            return Err(EtcdError::InvalidUrl(format!(
                "parameter {name} is repeated"
            )));
        }
        let non_empty = || (!value.is_empty()).then(|| value.clone());
        match name.as_str() {
            "user" => target.user = non_empty(),
            "password" => target.password = non_empty(),
            "ca" => target.ca = non_empty().map(|v| resolve_path(&v)),
            "cert" => target.cert = non_empty().map(|v| resolve_path(&v)),
            "key" => target.key = non_empty().map(|v| resolve_path(&v)),
            "server_name" => target.server_name = non_empty(),
            "tls" => target.tls = parse_bool(&name, &value)?,
            "enable_history" => target.enable_history = parse_bool(&name, &value)?,
            "timeout" => target.timeout = parse_duration(&name, &value)?,
            "connect_timeout" => target.connect_timeout = parse_duration(&name, &value)?,
            other => debug!(parameter = other, "ignoring unknown etcd url parameter"),
        }
    }
    if target.cert.is_some() != target.key.is_some() {
        return Err(EtcdError::InvalidUrl(
            "cert and key should be set together".into(),
        ));
    }
    Ok(target)
}

/// Pingap rejects names with a path separator or NUL (`manager.rs` `get_key`).
fn check_name(upstream: &str) -> Result<(), EtcdError> {
    if upstream.is_empty() || upstream.contains(['/', '\\', '\0']) {
        return Err(EtcdError::InvalidName(upstream.to_string()));
    }
    Ok(())
}

fn parse_doc(toml: &str) -> Result<DocumentMut, EtcdError> {
    toml.parse::<DocumentMut>()
        .map_err(|e| EtcdError::Toml(e.to_string()))
}

fn upstream_table<'a>(doc: &'a DocumentMut, upstream: &str) -> Option<&'a dyn TableLike> {
    doc.get("upstreams")
        .and_then(Item::as_table_like)
        .and_then(|t| t.get(upstream))
        .and_then(Item::as_table_like)
}

fn array_strings(array: &Array, upstream: &str) -> Result<Vec<String>, EtcdError> {
    array
        .iter()
        .map(|v| {
            v.as_str()
                .map(str::to_string)
                .ok_or_else(|| EtcdError::InvalidAddrs(upstream.to_string()))
        })
        .collect()
}

/// Reads `upstreams.<name>.addrs`. `Ok(None)` when the table or its `addrs` is absent;
/// an error for malformed TOML or an `addrs` that is not a list of strings.
pub fn read_addrs(toml: &str, upstream: &str) -> Result<Option<Vec<String>>, EtcdError> {
    let doc = parse_doc(toml)?;
    let Some(table) = upstream_table(&doc, upstream) else {
        return Ok(None);
    };
    match table.get("addrs") {
        None => Ok(None),
        Some(item) => {
            let array = item
                .as_array()
                .ok_or_else(|| EtcdError::InvalidAddrs(upstream.to_string()))?;
            array_strings(array, upstream).map(Some)
        }
    }
}

/// Replaces `upstreams.<name>.addrs` with `addrs` (already sorted by the caller) and
/// returns the new document, or `None` when the stored list is already equal. Every
/// other byte of the document is kept, including comments and the array's layout
/// (single-line or one entry per line). An absent `addrs` is added to the table. A
/// missing `[upstreams.<name>]` table or malformed TOML is an error: nothing is written.
pub fn replace_addrs(
    toml: &str,
    upstream: &str,
    addrs: &[String],
) -> Result<Option<String>, EtcdError> {
    let mut doc = parse_doc(toml)?;
    let table = doc
        .get_mut("upstreams")
        .and_then(Item::as_table_like_mut)
        .and_then(|t| t.get_mut(upstream))
        .and_then(Item::as_table_like_mut)
        .ok_or_else(|| EtcdError::MissingTable(upstream.to_string()))?;

    match table.get_mut("addrs") {
        None => {
            table.insert("addrs", toml_edit::value(new_array(addrs)));
        }
        Some(item) => {
            let array = item
                .as_array_mut()
                .ok_or_else(|| EtcdError::InvalidAddrs(upstream.to_string()))?;
            if array_strings(array, upstream)? == addrs {
                return Ok(None);
            }
            rewrite_array(array, addrs);
        }
    }
    Ok(Some(doc.to_string()))
}

fn new_array(addrs: &[String]) -> Array {
    addrs.iter().map(String::as_str).collect()
}

/// Replaces the elements of `array`, keeping its outer decor and whether it is laid
/// out one element per line.
fn rewrite_array(array: &mut Array, addrs: &[String]) {
    let prefix_of = |v: &Value| {
        v.decor()
            .prefix()
            .and_then(|p| p.as_str())
            .map(str::to_string)
    };
    let multiline_prefix = array
        .iter()
        .filter_map(prefix_of)
        .find(|p| p.contains('\n'))
        .map(|p| format!("\n{}", p.rsplit('\n').next().unwrap_or("")));
    let had_trailing_comma = array.trailing_comma();
    array.clear();
    for addr in addrs {
        array.push(addr.as_str());
    }
    match multiline_prefix {
        Some(prefix) => {
            for value in array.iter_mut() {
                value.decor_mut().set_prefix(prefix.clone());
                value.decor_mut().set_suffix("");
            }
            array.set_trailing_comma(true);
            array.set_trailing("\n");
        }
        None => {
            array.set_trailing_comma(had_trailing_comma && !addrs.is_empty());
            array.set_trailing("");
        }
    }
}

/// The entry written when an owned upstream key does not exist :
/// `[upstreams.<name>]` with only `addrs`.
pub fn minimal_upstream(upstream: &str, addrs: &[String]) -> String {
    let mut entry = Table::new();
    entry.insert("addrs", toml_edit::value(new_array(addrs)));
    let mut upstreams = Table::new();
    upstreams.set_implicit(true);
    upstreams.insert(upstream, Item::Table(entry));
    let mut doc = DocumentMut::new();
    doc.insert("upstreams", Item::Table(upstreams));
    doc.to_string()
}

/// Revisions of the controller's own writes, so the owned-key watch can tell them apart.
///
/// `gate`: a writer holds it shared from sending its Txn until the revision is
/// recorded; the watch takes it exclusively before looking a revision up. So an event
/// for the controller's own write is never checked before that write is recorded.
#[derive(Default)]
struct OwnWrites {
    gate: RwLock<()>,
    seen: Mutex<OwnRevisions>,
}

#[derive(Default)]
struct OwnRevisions {
    order: VecDeque<(String, i64)>,
    set: HashSet<(String, i64)>,
}

impl OwnWrites {
    fn record(&self, key: &str, revision: i64) {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        let entry = (key.to_string(), revision);
        if seen.set.insert(entry.clone()) {
            seen.order.push_back(entry);
        }
        while seen.order.len() > OWN_REVISIONS_CAP {
            if let Some(old) = seen.order.pop_front() {
                seen.set.remove(&old);
            }
        }
    }

    async fn is_own(&self, key: &str, revision: i64) -> bool {
        let _gate = self.gate.write().await;
        let seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        seen.set.contains(&(key.to_string(), revision))
    }
}

/// Called after the GET and before the Txn of each attempt (tests only).
type PreCommitHook = Arc<dyn Fn(u32) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Writes `addrs` into Pingap's etcd storage. Cheap to clone; clones share the
/// connection and the record of own writes.
#[derive(Clone)]
pub struct EtcdWriter {
    client: Client,
    prefix: String,
    own: Arc<OwnWrites>,
    pre_commit_hook: Option<PreCommitHook>,
}

impl fmt::Debug for EtcdWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EtcdWriter")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl EtcdWriter {
    pub async fn connect(target: &EtcdTarget) -> anyhow::Result<Self> {
        let options = target.connect_options()?;
        let client = Client::connect(&target.endpoints, Some(options))
            .await
            .with_context(|| format!("connecting to etcd {:?}", target.endpoints))?;
        Ok(Self {
            client,
            prefix: target.prefix.clone(),
            own: Arc::default(),
            pre_commit_hook: None,
        })
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// A separate client for the leader election, without a request timeout (see
    /// [`EtcdTarget::election_connect_options`]). Not for KV writes.
    pub async fn connect_election(target: &EtcdTarget) -> anyhow::Result<Client> {
        let options = target.election_connect_options()?;
        Client::connect(&target.endpoints, Some(options))
            .await
            .with_context(|| format!("connecting to etcd {:?} (election)", target.endpoints))
    }

    /// `"/" + <url path>` as Pingap keeps it.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The key Pingap reads for `upstreams/<name>`: `path_join(prefix, "upstreams/<name>.toml")`.
    pub fn upstream_key(&self, upstream: &str) -> String {
        path_join(&self.prefix, &format!("upstreams/{upstream}.toml"))
    }

    /// Test hook, called with the attempt number (0-based) between the GET and the Txn
    /// of each write attempt. Used by the integration tests to force CAS conflicts.
    #[doc(hidden)]
    pub fn set_pre_commit_hook<F, Fut>(&mut self, hook: F)
    where
        F: Fn(u32) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.pre_commit_hook = Some(Arc::new(move |attempt| Box::pin(hook(attempt))));
    }

    async fn write_addrs_inner(
        &self,
        upstream: &str,
        addrs: &[String],
    ) -> anyhow::Result<WriteOutcome> {
        check_name(upstream)?;
        let key = self.upstream_key(upstream);
        let mut kv = self.client.kv_client();
        for attempt in 0..=MAX_CAS_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(retry_delay(attempt)).await;
            }
            let resp = kv
                .get(key.as_str(), None)
                .await
                .with_context(|| format!("etcd get {key}"))?;
            let (txn, created) = match resp.kvs().first() {
                Some(current) => {
                    let text = std::str::from_utf8(current.value())
                        .map_err(|_| EtcdError::NotUtf8(key.clone()))?;
                    let Some(updated) = replace_addrs(text, upstream, addrs)? else {
                        return Ok(WriteOutcome::Unchanged);
                    };
                    let txn = Txn::new()
                        .when([Compare::mod_revision(
                            key.as_str(),
                            CompareOp::Equal,
                            current.mod_revision(),
                        )])
                        .and_then([TxnOp::put(key.as_str(), updated, None)]);
                    (txn, false)
                }
                None => {
                    let txn = Txn::new()
                        .when([Compare::create_revision(key.as_str(), CompareOp::Equal, 0)])
                        .and_then([TxnOp::put(
                            key.as_str(),
                            minimal_upstream(upstream, addrs),
                            None,
                        )]);
                    (txn, true)
                }
            };
            if let Some(hook) = &self.pre_commit_hook {
                hook(attempt).await;
            }

            let gate = self.own.gate.read().await;
            let resp = kv
                .txn(txn)
                .await
                .with_context(|| format!("etcd txn {key}"))?;
            if resp.succeeded() {
                let revision = resp
                    .header()
                    .map(|h| h.revision())
                    .context("etcd txn response without header")?;
                self.own.record(&key, revision);
                drop(gate);
                if created {
                    info!(
                        upstream,
                        key, revision, "created missing upstream with addrs only"
                    );
                    return Ok(WriteOutcome::Created { revision });
                }
                return Ok(WriteOutcome::Updated { revision });
            }
            drop(gate);
            debug!(
                upstream,
                key, attempt, "etcd compare-and-swap conflict, retrying"
            );
        }
        Err(EtcdError::Conflict {
            key,
            attempts: MAX_CAS_RETRIES + 1,
        }
        .into())
    }

    /// Watches the owned upstream keys and reports changes the controller did not make.
    ///
    /// One watch on the `<prefix>/upstreams/` range (a single stream however many
    /// upstreams are owned); events for keys that are not `upstream_key(name)` of an
    /// owned name are dropped. The watch starts at the current revision. Events whose
    /// `mod_revision` matches a recorded own write are skipped. After a stream error it
    /// reconnects with backoff (0.5 s to 30 s) from the last seen revision + 1 (progress
    /// notifications advance it while idle). If etcd reports that revision compacted,
    /// [`ForeignWrite::Resync`] is sent and the watch resumes from the compact revision.
    /// The task ends when the receiver is dropped.
    pub async fn watch_owned(
        &self,
        upstreams: Vec<String>,
    ) -> anyhow::Result<mpsc::Receiver<ForeignWrite>> {
        let range = path_join(&self.prefix, "upstreams/");
        let resp = self
            .client
            .kv_client()
            .get(
                range.as_str(),
                Some(GetOptions::new().with_prefix().with_count_only()),
            )
            .await
            .context("etcd get current revision")?;
        let start = resp
            .header()
            .map(|h| h.revision() + 1)
            .context("etcd response without header")?;
        self.watch_owned_from(upstreams, start).await
    }

    /// `watch_owned` from a given revision (tests use it to exercise compaction).
    #[doc(hidden)]
    pub async fn watch_owned_from(
        &self,
        upstreams: Vec<String>,
        start: i64,
    ) -> anyhow::Result<mpsc::Receiver<ForeignWrite>> {
        let mut names = HashMap::new();
        for upstream in upstreams {
            check_name(&upstream)?;
            names.insert(self.upstream_key(&upstream).into_bytes(), upstream);
        }
        let range = path_join(&self.prefix, "upstreams/");
        let stream = open_watch(&self.client, &range, start).await?;

        let (tx, rx) = mpsc::channel(FOREIGN_CHANNEL_CAP);
        let task = OwnedWatch {
            client: self.client.clone(),
            own: self.own.clone(),
            range,
            names,
            next: start,
            tx,
        };
        tokio::spawn(task.run(stream));
        Ok(rx)
    }
}

impl AddrsWriter for EtcdWriter {
    /// Read-modify-write with a compare-and-swap Txn. Errors are
    /// `EtcdError` inside the `anyhow::Error` where they come from this module
    /// (`Conflict` after the retries are used up, `Toml`/`MissingTable`/`InvalidAddrs`
    /// for an entry that is not written), etcd-client errors otherwise.
    fn write_addrs(
        &self,
        upstream: &str,
        addrs: &[String],
    ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send {
        self.write_addrs_inner(upstream, addrs)
    }
}

fn retry_delay(attempt: u32) -> Duration {
    let base = 10u64 << attempt.min(6);
    Duration::from_millis(base + rand::rng().random_range(0..=base))
}

async fn open_watch(client: &Client, range: &str, start: i64) -> anyhow::Result<WatchStream> {
    let options = WatchOptions::new()
        .with_prefix()
        .with_start_revision(start)
        .with_progress_notify();
    client
        .watch_client()
        .watch(range, Some(options))
        .await
        .with_context(|| format!("etcd watch {range} from revision {start}"))
}

struct OwnedWatch {
    client: Client,
    own: Arc<OwnWrites>,
    range: String,
    names: HashMap<Vec<u8>, String>,
    /// The first revision not seen yet.
    next: i64,
    tx: mpsc::Sender<ForeignWrite>,
}

enum StreamEnd {
    /// The receiver was dropped.
    Closed,
    /// Reopen from `self.next`; `true` when this was an error worth backing off for.
    Reopen(bool),
}

impl OwnedWatch {
    async fn run(mut self, stream: WatchStream) {
        let mut backoff = WATCH_BACKOFF_MIN;
        let mut stream = Some(stream);
        loop {
            let current = match stream.take() {
                Some(stream) => stream,
                None => match open_watch(&self.client, &self.range, self.next).await {
                    Ok(stream) => {
                        info!(
                            range = self.range,
                            revision = self.next,
                            "owned-key watch reconnected"
                        );
                        stream
                    }
                    Err(e) => {
                        warn!(error = %e, range = self.range, "owned-key watch: reconnect failed");
                        if !self.sleep(&mut backoff).await {
                            return;
                        }
                        continue;
                    }
                },
            };
            match self.read(current, &mut backoff).await {
                StreamEnd::Closed => return,
                StreamEnd::Reopen(false) => {}
                StreamEnd::Reopen(true) => {
                    if !self.sleep(&mut backoff).await {
                        return;
                    }
                }
            }
        }
    }

    /// Sleeps for the backoff and doubles it; `false` when the receiver was dropped.
    async fn sleep(&self, backoff: &mut Duration) -> bool {
        let jitter = rand::rng().random_range(0..=backoff.as_millis() as u64 / 4);
        let delay = *backoff + Duration::from_millis(jitter);
        *backoff = (*backoff * 2).min(WATCH_BACKOFF_MAX);
        tokio::select! {
            _ = self.tx.closed() => false,
            _ = tokio::time::sleep(delay) => true,
        }
    }

    async fn read(&mut self, mut stream: WatchStream, backoff: &mut Duration) -> StreamEnd {
        loop {
            let message = tokio::select! {
                _ = self.tx.closed() => return StreamEnd::Closed,
                message = stream.message() => message,
            };
            let resp = match message {
                Ok(Some(resp)) => resp,
                Ok(None) => {
                    warn!(range = self.range, "owned-key watch stream ended");
                    return StreamEnd::Reopen(true);
                }
                Err(e) => {
                    warn!(error = %e, range = self.range, "owned-key watch failed");
                    return StreamEnd::Reopen(true);
                }
            };
            *backoff = WATCH_BACKOFF_MIN;
            if resp.canceled() {
                if resp.compact_revision() > 0 {
                    warn!(
                        range = self.range,
                        from = self.next,
                        compacted = resp.compact_revision(),
                        "owned-key watch revision was compacted; requesting a full resync"
                    );
                    self.next = self.next.max(resp.compact_revision());
                    if self.tx.send(ForeignWrite::Resync).await.is_err() {
                        return StreamEnd::Closed;
                    }
                    return StreamEnd::Reopen(false);
                }
                warn!(
                    range = self.range,
                    reason = resp.cancel_reason(),
                    "owned-key watch canceled"
                );
                return StreamEnd::Reopen(true);
            }
            if resp.events().is_empty() {
                // A progress notification: every event up to the header revision was sent.
                if !resp.created()
                    && let Some(header) = resp.header()
                {
                    self.next = self.next.max(header.revision() + 1);
                }
                continue;
            }
            for event in resp.events() {
                let Some(kv) = event.kv() else { continue };
                let revision = kv.mod_revision();
                self.next = self.next.max(revision + 1);
                let Some(upstream) = self.names.get(kv.key()) else {
                    continue;
                };
                let write = match event.event_type() {
                    EventType::Put => {
                        let key = String::from_utf8_lossy(kv.key());
                        if self.own.is_own(&key, revision).await {
                            continue;
                        }
                        let addrs = std::str::from_utf8(kv.value())
                            .ok()
                            .and_then(|text| read_addrs(text, upstream).ok().flatten());
                        ForeignWrite::Put {
                            upstream: upstream.clone(),
                            addrs,
                            revision,
                        }
                    }
                    EventType::Delete => ForeignWrite::Deleted {
                        upstream: upstream.clone(),
                        revision,
                    },
                };
                debug!(?write, "foreign write to owned upstream key");
                if self.tx.send(write).await.is_err() {
                    return StreamEnd::Closed;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn key_for(url: &str, name: &str) -> String {
        let target = parse_etcd_url(url).unwrap();
        path_join(&target.prefix, &format!("upstreams/{name}.toml"))
    }

    #[test]
    fn url_like_pingap_test_parse_url() {
        let t =
            parse_etcd_url("etcd://a:2379,b:2379/pingap/?timeout=10s&enable_history=true").unwrap();
        assert_eq!(t.endpoints, s(&["a:2379", "b:2379"]));
        assert_eq!(t.prefix, "/pingap/");
        assert!(t.enable_history);
        assert_eq!(t.timeout, Duration::from_secs(10));
        assert_eq!(path_join(&t.prefix, "basic.toml"), "/pingap/basic.toml");

        let t = parse_etcd_url("etcd://127.0.0.1:2379").unwrap();
        assert_eq!(t.prefix, "/");
        assert!(parse_etcd_url("etcd:///pingap").is_err());
    }

    #[test]
    fn keys_match_pingap() {
        assert_eq!(
            key_for("etcd://h:2379/pingap", "api"),
            "/pingap/upstreams/api.toml"
        );
        assert_eq!(
            key_for("etcd://h:2379/pingap/", "api"),
            "/pingap/upstreams/api.toml"
        );
        assert_eq!(key_for("etcd://h:2379", "api"), "/upstreams/api.toml");
        assert_eq!(key_for("etcd://h:2379/", "api"), "/upstreams/api.toml");
        assert_eq!(
            key_for("etcd://h:2379/a/b", "x-1"),
            "/a/b/upstreams/x-1.toml"
        );
        // The path is taken verbatim, as Pingap does (no decoding, no normalising).
        assert_eq!(key_for("etcd://h:2379/a//", "x"), "/a//upstreams/x.toml");
        assert_eq!(key_for("h:2379/pingap", "x"), "/pingap/upstreams/x.toml");
        let t = parse_etcd_url("etcd://h:2379/pingap").unwrap();
        assert_eq!(t.upstreams_prefix(), "/pingap/upstreams/");
    }

    #[test]
    fn url_defaults() {
        let t = parse_etcd_url("etcd://h:2379/pingap").unwrap();
        assert_eq!(t.timeout, DEFAULT_TIMEOUT);
        assert_eq!(t.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert!(!t.tls_wanted());
        assert_eq!(t.user, None);
        assert!(!t.enable_history);
    }

    #[test]
    fn url_all_params() {
        let t = parse_etcd_url(
            "etcd://h1:2379,h2:2379/pingap?user=ctrl&password=p%40ss+w&timeout=3s\
             &connect_timeout=500ms&tls=true&ca=/etc/ca.pem&cert=/etc/c.pem&key=/etc/c.key\
             &server_name=etcd.internal&unknown=1",
        )
        .unwrap();
        assert_eq!(t.endpoints, s(&["h1:2379", "h2:2379"]));
        assert_eq!(t.user.as_deref(), Some("ctrl"));
        assert_eq!(t.password.as_deref(), Some("p@ss w"));
        assert_eq!(t.timeout, Duration::from_secs(3));
        assert_eq!(t.connect_timeout, Duration::from_millis(500));
        assert!(t.tls);
        assert_eq!(t.ca, Some(PathBuf::from("/etc/ca.pem")));
        assert_eq!(t.cert, Some(PathBuf::from("/etc/c.pem")));
        assert_eq!(t.key, Some(PathBuf::from("/etc/c.key")));
        assert_eq!(t.server_name.as_deref(), Some("etcd.internal"));
        assert!(!format!("{t:?}").contains("p@ss"));
    }

    #[test]
    fn url_tls_implied_and_empty_values() {
        let t = parse_etcd_url("etcd://h:2379/p?ca=/x/ca.pem").unwrap();
        assert!(t.tls_wanted());
        let t = parse_etcd_url("etcd://h:2379/p?server_name=e").unwrap();
        assert!(t.tls_wanted());
        let t = parse_etcd_url("etcd://h:2379/p?user=&password=&ca=").unwrap();
        assert_eq!(t.user, None);
        assert_eq!(t.ca, None);
        assert!(!t.tls_wanted());
    }

    #[test]
    fn url_home_expansion() {
        let Some(home) = std::env::var_os("HOME") else {
            return;
        };
        let t = parse_etcd_url("etcd://h:2379/p?ca=~/certs/ca.pem").unwrap();
        assert_eq!(t.ca, Some(PathBuf::from(home).join("certs/ca.pem")));
    }

    #[test]
    fn url_errors() {
        for bad in [
            "etcd://",
            "etcd:///p",
            "etcd://a:1,,b:2/p",
            "etcd://a:1?user=x",
            "etcd://h:2379/p?timeout=soon",
            "etcd://h:2379/p?tls=yes",
            "etcd://h:2379/p?cert=/c.pem",
            "etcd://h:2379/p?key=/c.key",
            "etcd://h:2379/p?user=a&user=b",
        ] {
            assert!(parse_etcd_url(bad).is_err(), "{bad} should fail");
        }
    }

    #[test]
    fn tls_files_checked() {
        let t = parse_etcd_url("etcd://h:2379/p?ca=/nonexistent/ca.pem").unwrap();
        let err = t.connect_options().unwrap_err().to_string();
        assert!(err.contains("can not be read"), "{err}");
    }

    #[test]
    fn form_decoding() {
        assert_eq!(form_decode("a%2Fb+c").unwrap(), "a/b c");
        assert_eq!(form_decode("100%").unwrap(), "100%");
        assert_eq!(form_decode("%zz").unwrap(), "%zz");
        assert_eq!(form_decode("%41%").unwrap(), "A%");
    }

    const STORED: &str = r#"# managed partly by pingap-k8s
[upstreams.api]
# backends
addrs = ["10.0.0.9:80"] # replaced by the controller
discovery = ""
health_check = "http://api/ping"   # keep me
connection_timeout = "10s"
"#;

    #[test]
    fn replace_keeps_everything_else() {
        let out = replace_addrs(STORED, "api", &s(&["10.0.0.1:80", "10.0.0.2:80 5"]))
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            STORED.replace(r#"["10.0.0.9:80"]"#, r#"["10.0.0.1:80", "10.0.0.2:80 5"]"#)
        );
        assert_eq!(
            read_addrs(&out, "api").unwrap(),
            Some(s(&["10.0.0.1:80", "10.0.0.2:80 5"]))
        );
    }

    #[test]
    fn replace_unchanged_is_none() {
        assert_eq!(
            replace_addrs(STORED, "api", &s(&["10.0.0.9:80"])).unwrap(),
            None
        );
        // Same set in another order is rewritten sorted.
        let doc = "[upstreams.a]\naddrs = [\"b:1\", \"a:1\"]\n";
        assert_eq!(
            replace_addrs(doc, "a", &s(&["a:1", "b:1"]))
                .unwrap()
                .unwrap(),
            "[upstreams.a]\naddrs = [\"a:1\", \"b:1\"]\n"
        );
    }

    #[test]
    fn replace_multiline_layout_kept() {
        // The layout toml::to_string_pretty (Pingap's admin) writes.
        let doc = "[upstreams.api]\naddrs = [\n    \"10.0.0.9:80\",\n    \"10.0.0.8:80\",\n]\nalgo = \"hash:cookie\"\n";
        let out = replace_addrs(
            doc,
            "api",
            &s(&["[2001:db8::1]:8080", "[2001:db8::2]:8080 3"]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            out,
            "[upstreams.api]\naddrs = [\n    \"[2001:db8::1]:8080\",\n    \"[2001:db8::2]:8080 3\",\n]\nalgo = \"hash:cookie\"\n"
        );
    }

    #[test]
    fn replace_other_upstreams_untouched() {
        let doc = "[upstreams.a]\naddrs = [\"1.1.1.1:1\"]\n\n[upstreams.b]\naddrs = [\"2.2.2.2:2\"] # b\n";
        let out = replace_addrs(doc, "b", &s(&["3.3.3.3:3"]))
            .unwrap()
            .unwrap();
        assert_eq!(out, doc.replace("2.2.2.2:2", "3.3.3.3:3"));
    }

    #[test]
    fn replace_inserts_missing_addrs() {
        let doc = "[upstreams.api]\nhealth_check = \"tcp://x\"\n";
        let out = replace_addrs(doc, "api", &s(&["10.0.0.1:80"]))
            .unwrap()
            .unwrap();
        assert_eq!(read_addrs(&out, "api").unwrap(), Some(s(&["10.0.0.1:80"])));
        assert!(out.contains("health_check = \"tcp://x\""));
    }

    #[test]
    fn replace_dotted_and_inline_tables() {
        let doc = "upstreams.api.addrs = [\"1.1.1.1:1\"]\n";
        let out = replace_addrs(doc, "api", &s(&["2.2.2.2:2"]))
            .unwrap()
            .unwrap();
        assert_eq!(out, "upstreams.api.addrs = [\"2.2.2.2:2\"]\n");
        let doc = "[upstreams]\napi = { addrs = [\"1.1.1.1:1\"], weight = 1 }\n";
        let out = replace_addrs(doc, "api", &s(&["2.2.2.2:2"]))
            .unwrap()
            .unwrap();
        assert_eq!(out, doc.replace("1.1.1.1:1", "2.2.2.2:2"));
    }

    #[test]
    fn replace_errors() {
        let addrs = s(&["1.1.1.1:1"]);
        assert!(matches!(
            replace_addrs("[upstreams.api\naddrs = [", "api", &addrs),
            Err(EtcdError::Toml(_))
        ));
        assert!(matches!(
            replace_addrs("[upstreams.other]\naddrs = []\n", "api", &addrs),
            Err(EtcdError::MissingTable(_))
        ));
        assert!(matches!(
            replace_addrs("", "api", &addrs),
            Err(EtcdError::MissingTable(_))
        ));
        assert!(matches!(
            replace_addrs("[upstreams.api]\naddrs = \"1.1.1.1:1\"\n", "api", &addrs),
            Err(EtcdError::InvalidAddrs(_))
        ));
        assert!(matches!(
            replace_addrs("[upstreams.api]\naddrs = [1]\n", "api", &addrs),
            Err(EtcdError::InvalidAddrs(_))
        ));
        assert!(matches!(
            replace_addrs("[servers.api]\naddrs = []\n", "api", &addrs),
            Err(EtcdError::MissingTable(_))
        ));
    }

    #[test]
    fn read_addrs_cases() {
        assert_eq!(
            read_addrs(STORED, "api").unwrap(),
            Some(s(&["10.0.0.9:80"]))
        );
        assert_eq!(read_addrs(STORED, "other").unwrap(), None);
        assert_eq!(read_addrs("[upstreams.api]\n", "api").unwrap(), None);
        assert!(read_addrs("not toml [", "api").is_err());
        assert!(read_addrs("[upstreams.api]\naddrs = [1]\n", "api").is_err());
    }

    #[test]
    fn minimal_upstream_shape() {
        let out = minimal_upstream("api-1", &s(&["10.0.0.1:80", "[::1]:80 2"]));
        assert_eq!(
            out,
            "[upstreams.api-1]\naddrs = [\"10.0.0.1:80\", \"[::1]:80 2\"]\n"
        );
        assert_eq!(
            read_addrs(&out, "api-1").unwrap(),
            Some(s(&["10.0.0.1:80", "[::1]:80 2"]))
        );
        // And it parses as plain TOML with the table Pingap expects.
        let value: toml::Table = toml::from_str(&out).unwrap();
        assert!(value["upstreams"]["api-1"]["addrs"].is_array());
    }

    #[test]
    fn names_checked() {
        for bad in ["", "a/b", "a\\b", "a\0b"] {
            assert!(check_name(bad).is_err());
        }
        check_name("api_1-x").unwrap();
    }

    #[test]
    fn own_revisions_bounded() {
        let own = OwnWrites::default();
        for rev in 0..(OWN_REVISIONS_CAP as i64 + 10) {
            own.record("/k", rev);
        }
        let seen = own.seen.lock().unwrap();
        assert_eq!(seen.order.len(), OWN_REVISIONS_CAP);
        assert!(!seen.set.contains(&("/k".to_string(), 0)));
        assert!(
            seen.set
                .contains(&("/k".to_string(), OWN_REVISIONS_CAP as i64 + 9))
        );
    }

    #[test]
    fn path_join_like_pingap() {
        assert_eq!(path_join("/a", "b"), "/a/b");
        assert_eq!(path_join("/a/", "b"), "/a/b");
        assert_eq!(path_join("/a/", "/b"), "/a/b");
        assert_eq!(path_join("/a", "/b"), "/a/b");
        assert_eq!(path_join("/", "b"), "/b");
    }
}
