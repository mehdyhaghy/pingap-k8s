//! The etcd side of the Gateway API provider: reads every Pingap entry at one revision,
//! applies a [`Plan`] step by step, and watches the prefix for changes that need a new
//! plan.
//!
//! A step is applied as one or more Txns (split by [`TxnLimits`]); the ops of a step do
//! not depend on each other, so the stored config is valid after each Txn. Every op
//! compares the revision it was planned from: when a compare fails nothing of that Txn
//! is written and [`apply`](PingapStore::apply) returns [`StoreConflict`], so the caller
//! reads again and plans again.

use std::future::Future;
use std::time::Duration;

use anyhow::Context as _;
use etcd_client::{
    Client, Compare, CompareOp, EventType, GetOptions, Txn, TxnOp, TxnOpResponse, WatchOptions,
    WatchStream,
};
use rand::RngExt as _;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::etcd::{EtcdWriter, client_error_unreachable, path_join};
use crate::gateway::model::{ApplyOutcome, Expect, Op, PingapStore, Plan, Stored, StoredEntry};
use crate::gateway::render::upstream_without_addrs;

/// The categories of Pingap entries, as in their keys.
pub const CATEGORIES: [&str; 5] = [
    "servers",
    "locations",
    "upstreams",
    "certificates",
    "plugins",
];

const WATCH_BACKOFF_MIN: Duration = Duration::from_millis(500);
const WATCH_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// How large one Txn may get. etcd rejects a Txn with more than `--max-txn-ops`
/// operations (default 128) in its compare or success list, or a request larger than
/// `--max-request-bytes` (default 1.5 MiB).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TxnLimits {
    /// Ops per Txn; each [`Op`] adds at most one compare and one write.
    pub max_ops: usize,
    /// Sum of key and value bytes per Txn. A single larger op still goes alone.
    pub max_bytes: usize,
}

impl Default for TxnLimits {
    fn default() -> Self {
        TxnLimits {
            max_ops: 64,
            max_bytes: 1024 * 1024,
        }
    }
}

/// A compare of the plan failed: etcd changed since it was read. Read and plan again.
#[derive(Debug, thiserror::Error)]
#[error("etcd changed since it was read (step {step:?}); plan again")]
pub struct StoreConflict {
    pub step: &'static str,
}

fn op_bytes(op: &Op) -> usize {
    match op {
        Op::Put { key, value, .. } => key.len() + value.len(),
        Op::Delete { key, .. } | Op::RequireExists { key } => key.len(),
    }
}

/// Splits the ops of a step into Txns within `limits`, keeping their order.
pub fn chunk_ops<'a>(ops: &'a [Op], limits: &TxnLimits) -> Vec<&'a [Op]> {
    let max_ops = limits.max_ops.max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut bytes = 0;
    for (i, op) in ops.iter().enumerate() {
        let size = op_bytes(op);
        if i > start && (i - start >= max_ops || bytes + size > limits.max_bytes) {
            chunks.push(&ops[start..i]);
            start = i;
            bytes = 0;
        }
        bytes += size;
    }
    if start < ops.len() {
        chunks.push(&ops[start..]);
    }
    chunks
}

fn txn_for(ops: &[Op]) -> Txn {
    let mut compares = Vec::new();
    let mut writes = Vec::new();
    for op in ops {
        let (key, expect) = match op {
            Op::Put { key, value, expect } => {
                writes.push(TxnOp::put(key.as_str(), value.as_str(), None));
                (key, Some(expect))
            }
            Op::Delete { key, expect } => {
                writes.push(TxnOp::delete(key.as_str(), None));
                (key, Some(expect))
            }
            Op::RequireExists { key } => (key, None),
        };
        compares.push(match expect {
            Some(Expect::Absent) => Compare::create_revision(key.as_str(), CompareOp::Equal, 0),
            Some(Expect::ModRevision(revision)) => {
                Compare::mod_revision(key.as_str(), CompareOp::Equal, *revision)
            }
            None => Compare::create_revision(key.as_str(), CompareOp::Greater, 0),
        });
    }
    Txn::new().when(compares).and_then(writes)
}

/// `(category, name)` of a key `<prefix>/<category>/<name>.toml`.
pub fn parse_entry_key<'a>(prefix: &str, key: &'a str) -> Option<(&'static str, &'a str)> {
    CATEGORIES.iter().find_map(|category| {
        let range = path_join(prefix, &format!("{category}/"));
        let name = key.strip_prefix(range.as_str())?.strip_suffix(".toml")?;
        (!name.is_empty() && !name.contains('/'))
            .then_some(*category)
            .map(|c| (c, name))
    })
}

/// Reads and writes the Pingap entries under one etcd prefix. Cheap to clone.
#[derive(Clone)]
pub struct GatewayWriter {
    client: Client,
    prefix: String,
    limits: TxnLimits,
}

impl std::fmt::Debug for GatewayWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayWriter")
            .field("prefix", &self.prefix)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl GatewayWriter {
    /// Shares the connection and prefix of the endpoint writer.
    pub fn new(writer: &EtcdWriter, limits: TxnLimits) -> Self {
        Self::with_client(writer.client(), writer.prefix(), limits)
    }

    pub fn with_client(client: Client, prefix: &str, limits: TxnLimits) -> Self {
        GatewayWriter {
            client,
            prefix: prefix.to_string(),
            limits,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Every entry of every category, read in one Txn (one revision). Keys that are not
    /// `<category>/<name>.toml` are ignored; a value that is not UTF-8 is an error.
    pub async fn read_stored(&self) -> anyhow::Result<Stored> {
        let gets: Vec<TxnOp> = CATEGORIES
            .iter()
            .map(|category| {
                TxnOp::get(
                    path_join(&self.prefix, &format!("{category}/")),
                    Some(GetOptions::new().with_prefix()),
                )
            })
            .collect();
        let resp = self
            .client
            .kv_client()
            .txn(Txn::new().and_then(gets))
            .await
            .context("etcd read of the Pingap entries")?;
        let mut stored = Stored {
            revision: resp
                .header()
                .map(|h| h.revision())
                .context("etcd txn response without header")?,
            ..Stored::default()
        };
        for op in resp.op_responses() {
            let TxnOpResponse::Get(get) = op else {
                continue;
            };
            for kv in get.kvs() {
                let key = String::from_utf8_lossy(kv.key()).into_owned();
                let Some((category, name)) = parse_entry_key(&self.prefix, &key) else {
                    debug!(key, "ignoring a key that is not a Pingap entry");
                    continue;
                };
                let value = std::str::from_utf8(kv.value())
                    .with_context(|| format!("value of etcd key {key} is not UTF-8"))?
                    .to_string();
                let map = match category {
                    "servers" => &mut stored.servers,
                    "locations" => &mut stored.locations,
                    "upstreams" => &mut stored.upstreams,
                    "certificates" => &mut stored.certificates,
                    _ => &mut stored.plugins,
                };
                map.insert(
                    name.to_string(),
                    StoredEntry {
                        key: key.clone(),
                        value,
                        mod_revision: kv.mod_revision(),
                    },
                );
            }
        }
        Ok(stored)
    }

    async fn apply_plan(&self, plan: &Plan) -> anyhow::Result<ApplyOutcome> {
        if plan.steps.is_empty() {
            return Ok(ApplyOutcome::Unchanged);
        }
        let mut kv = self.client.kv_client();
        let (mut revision, mut puts, mut deletes) = (0, 0, 0);
        for step in &plan.steps {
            for chunk in chunk_ops(&step.ops, &self.limits) {
                let resp = kv
                    .txn(txn_for(chunk))
                    .await
                    .with_context(|| format!("etcd txn ({})", step.what))?;
                if !resp.succeeded() {
                    return Err(StoreConflict { step: step.what }.into());
                }
                revision = resp
                    .header()
                    .map(|h| h.revision())
                    .context("etcd txn response without header")?;
                for op in chunk {
                    match op {
                        Op::Put { .. } => puts += 1,
                        Op::Delete { .. } => deletes += 1,
                        Op::RequireExists { .. } => {}
                    }
                }
                debug!(step = step.what, ops = chunk.len(), revision, "applied");
            }
        }
        info!(revision, puts, deletes, "applied Gateway plan");
        Ok(ApplyOutcome::Applied {
            revision,
            puts,
            deletes,
        })
    }

    /// Watches the prefix and signals (coalesced, capacity 1) when a new plan may be
    /// needed: any change to servers, locations, certificates or plugins, an upstream
    /// created or deleted, or an upstream changed in more than its `addrs` (phase 1
    /// rewrites `addrs` all the time). Also signals after a reconnect that lost history
    /// (compaction). The task ends when the receiver is dropped.
    pub async fn watch_changes(&self) -> anyhow::Result<mpsc::Receiver<()>> {
        let range = path_join(&self.prefix, "");
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
        let stream = open_watch(&self.client, &range, start).await?;
        let (tx, rx) = mpsc::channel(1);
        let task = ChangeWatch {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            range,
            next: start,
            tx,
        };
        tokio::spawn(task.run(stream));
        Ok(rx)
    }
}

impl PingapStore for GatewayWriter {
    fn read_stored(&self) -> impl Future<Output = anyhow::Result<Stored>> + Send {
        GatewayWriter::read_stored(self)
    }

    /// Errors: [`StoreConflict`] when etcd changed since the read (steps before the
    /// failed Txn stay applied, and the config is valid), etcd-client errors otherwise.
    fn apply(&self, plan: &Plan) -> impl Future<Output = anyhow::Result<ApplyOutcome>> + Send {
        self.apply_plan(plan)
    }
}

async fn open_watch(client: &Client, range: &str, start: i64) -> anyhow::Result<WatchStream> {
    let options = WatchOptions::new()
        .with_prefix()
        .with_prev_key()
        .with_start_revision(start)
        .with_progress_notify();
    client
        .watch_client()
        .watch(range, Some(options))
        .await
        .with_context(|| format!("etcd watch {range} from revision {start}"))
}

/// Whether a change to `key` may need a new plan. `prev` is the value before a put
/// (`None` for a creation or when etcd did not send it).
fn is_relevant(prefix: &str, key: &str, deleted: bool, value: &[u8], prev: Option<&[u8]>) -> bool {
    let Some((category, name)) = parse_entry_key(prefix, key) else {
        return false;
    };
    if category != "upstreams" || deleted {
        return true;
    }
    let Some(prev) = prev else {
        return true;
    };
    let read = |bytes: &[u8]| {
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| upstream_without_addrs(text, name))
    };
    match (read(prev), read(value)) {
        (Some(before), Some(after)) => before != after,
        _ => true,
    }
}

struct ChangeWatch {
    client: Client,
    prefix: String,
    range: String,
    /// The first revision not seen yet.
    next: i64,
    tx: mpsc::Sender<()>,
}

impl ChangeWatch {
    /// `false` when the receiver was dropped.
    fn signal(&self) -> bool {
        !matches!(
            self.tx.try_send(()),
            Err(mpsc::error::TrySendError::Closed(()))
        )
    }

    async fn sleep(&self, backoff: &mut Duration) -> bool {
        let jitter = rand::rng().random_range(0..=backoff.as_millis() as u64 / 4);
        let delay = *backoff + Duration::from_millis(jitter);
        *backoff = (*backoff * 2).min(WATCH_BACKOFF_MAX);
        tokio::select! {
            _ = self.tx.closed() => false,
            _ = tokio::time::sleep(delay) => true,
        }
    }

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
                            "Pingap entries watch reconnected"
                        );
                        stream
                    }
                    Err(e) => {
                        warn!(error = %e, range = self.range, "Pingap entries watch: reconnect failed");
                        if !self.sleep(&mut backoff).await {
                            return;
                        }
                        continue;
                    }
                },
            };
            match self.read(current, &mut backoff).await {
                None => return,
                Some(false) => {}
                Some(true) => {
                    if !self.sleep(&mut backoff).await {
                        return;
                    }
                }
            }
        }
    }

    /// Reads until the stream ends: `None` when the receiver was dropped, otherwise
    /// whether to back off before reopening.
    async fn read(&mut self, mut stream: WatchStream, backoff: &mut Duration) -> Option<bool> {
        loop {
            let message = tokio::select! {
                _ = self.tx.closed() => return None,
                message = stream.message() => message,
            };
            let resp = match message {
                Ok(Some(resp)) => resp,
                Ok(None) => {
                    warn!(range = self.range, "Pingap entries watch stream ended");
                    return Some(true);
                }
                Err(e) => {
                    let unreachable = client_error_unreachable(&e);
                    warn!(error = %e, unreachable, range = self.range, "Pingap entries watch failed");
                    return Some(true);
                }
            };
            *backoff = WATCH_BACKOFF_MIN;
            if resp.canceled() {
                if resp.compact_revision() > 0 {
                    warn!(
                        range = self.range,
                        from = self.next,
                        compacted = resp.compact_revision(),
                        "Pingap entries watch revision was compacted; planning again"
                    );
                    self.next = self.next.max(resp.compact_revision());
                    if !self.signal() {
                        return None;
                    }
                    return Some(false);
                }
                warn!(
                    range = self.range,
                    reason = resp.cancel_reason(),
                    "Pingap entries watch canceled"
                );
                return Some(true);
            }
            if resp.events().is_empty() {
                if !resp.created()
                    && let Some(header) = resp.header()
                {
                    self.next = self.next.max(header.revision() + 1);
                }
                continue;
            }
            let mut relevant = false;
            for event in resp.events() {
                let Some(kv) = event.kv() else { continue };
                self.next = self.next.max(kv.mod_revision() + 1);
                let key = String::from_utf8_lossy(kv.key());
                relevant |= is_relevant(
                    &self.prefix,
                    &key,
                    event.event_type() == EventType::Delete,
                    kv.value(),
                    event.prev_kv().map(|p| p.value()),
                );
            }
            if relevant && !self.signal() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value_len: usize) -> Op {
        Op::Put {
            key: key.to_string(),
            value: "x".repeat(value_len),
            expect: Expect::Absent,
        }
    }

    #[test]
    fn chunks_respect_op_limit() {
        let ops: Vec<Op> = (0..10).map(|i| put(&format!("k{i}"), 1)).collect();
        let limits = TxnLimits {
            max_ops: 4,
            max_bytes: 1 << 20,
        };
        let chunks = chunk_ops(&ops, &limits);
        assert_eq!(
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        assert_eq!(chunks.concat(), ops);
        assert!(chunk_ops(&[], &limits).is_empty());
    }

    #[test]
    fn chunks_respect_byte_limit() {
        // Each op is 2 + 98 = 100 bytes.
        let ops: Vec<Op> = (0..5).map(|i| put(&format!("k{i}"), 98)).collect();
        let limits = TxnLimits {
            max_ops: 64,
            max_bytes: 250,
        };
        let sizes: Vec<usize> = chunk_ops(&ops, &limits).iter().map(|c| c.len()).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
        // An op larger than the limit goes alone.
        let ops = vec![put("a", 10), put("b", 1000), put("c", 10)];
        let sizes: Vec<usize> = chunk_ops(&ops, &limits).iter().map(|c| c.len()).collect();
        assert_eq!(sizes, vec![1, 1, 1]);
        let ops = vec![
            Op::RequireExists { key: "a".into() },
            Op::Delete {
                key: "b".into(),
                expect: Expect::ModRevision(3),
            },
        ];
        assert_eq!(chunk_ops(&ops, &limits).len(), 1);
    }

    #[test]
    fn entry_keys_parsed() {
        assert_eq!(
            parse_entry_key("/pingap", "/pingap/locations/k8s-a.toml"),
            Some(("locations", "k8s-a"))
        );
        assert_eq!(
            parse_entry_key("/pingap/", "/pingap/servers/http.toml"),
            Some(("servers", "http"))
        );
        assert_eq!(
            parse_entry_key("/", "/plugins/auth.toml"),
            Some(("plugins", "auth"))
        );
        for key in [
            "/pingap/locations/a.toml.bak",
            "/pingap/locations/a/b.toml",
            "/pingap/locations/.toml",
            "/pingap/basic.toml",
            "/pingap/storages/x.toml",
            "/other/locations/a.toml",
        ] {
            assert_eq!(parse_entry_key("/pingap", key), None, "{key}");
        }
    }

    #[test]
    fn relevance_of_changes() {
        let p = "/pingap";
        let up = |addrs: &str, extra: &str| {
            format!("[upstreams.u]\naddrs = [\"{addrs}\"]\n{extra}").into_bytes()
        };
        let key = "/pingap/upstreams/u.toml";
        // addrs-only update: ignored.
        assert!(!is_relevant(
            p,
            key,
            false,
            &up("2.2.2.2:1", ""),
            Some(&up("1.1.1.1:1", ""))
        ));
        // Other field changed, creation, deletion, unknown previous value: relevant.
        assert!(is_relevant(
            p,
            key,
            false,
            &up("1.1.1.1:1", "discovery = \"dns\"\n"),
            Some(&up("1.1.1.1:1", ""))
        ));
        assert!(is_relevant(p, key, false, &up("1.1.1.1:1", ""), None));
        assert!(is_relevant(p, key, true, b"", Some(&up("1.1.1.1:1", ""))));
        // Any change to the other categories.
        for category in ["servers", "locations", "certificates", "plugins"] {
            let key = format!("/pingap/{category}/x.toml");
            assert!(is_relevant(p, &key, false, b"a", Some(b"a")), "{category}");
        }
        // Keys that are not entries.
        assert!(!is_relevant(p, "/pingap/basic.toml", false, b"", None));
        assert!(!is_relevant(p, "/pingap/locations/x.bak", false, b"", None));
    }
}
