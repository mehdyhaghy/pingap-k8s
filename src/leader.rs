//! Leader election over the etcd election API.
//!
//! [`run_election`] grants a lease, keeps it alive in a background task, campaigns under the
//! election name with the pod identity as value, and reports leadership on a
//! `tokio::sync::watch` channel. Only the leader writes to etcd; the reconciler watches the
//! channel.
//!
//! # Safety margin
//!
//! A keep-alive is sent every `ttl / 5` (2 s for the default 10 s TTL), each with a timeout of
//! the same length. The lease is known to live at least until `send time + ttl` of the last
//! keep-alive the server answered with a positive TTL (the server renews on receipt, which is
//! after the send). This module stops claiming leadership once `ttl / 2` has passed since the
//! send time of the last successful keep-alive (5 s for the default TTL), leaving the other half
//! of the TTL as margin for in-flight writes and clock-rate differences. Two failed keep-alives
//! in a row are therefore tolerated before stepping down. The lease is also treated as lost when
//! the server reports it gone (TTL 0, e.g. revoked by an operator) and when our election key
//! disappears (checked every keep-alive interval).
//!
//! On every loss the channel goes to `false` first, then the lease is revoked (best effort) and
//! the module campaigns again after a jittered exponential backoff (500 ms up to 10 s, reset
//! after being elected). On shutdown it sends `false`, resigns and revokes the lease, so a
//! follower takes over at once instead of after the TTL.
//!
//! # Metrics
//!
//! This module does not touch [`crate::metrics::Metrics`]. The caller watches the channel and
//! calls `Metrics::set_leader`, which keeps the contract signature unchanged.
//!
//! # etcd permissions
//!
//! The election server stores one key per candidate at `<election_name>/<lease id in hex>`, so
//! with `election_name = "<prefix>-k8s-election"` the controller's etcd role needs readwrite on
//! the key prefix `<prefix>-k8s-election/` (for example
//! `etcdctl role grant-permission pingap-k8s readwrite --prefix=true /pingap-k8s-election/`).
//! Lease grant and keep-alive need only an authenticated user; revoking a lease needs write
//! access to the keys attached to it, which are inside that same prefix.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use etcd_client::{
    Client, Error as EtcdClientError, LeaderKey, LeaseClient, LeaseKeepAliveStream, LeaseKeeper,
    ResignOptions,
};
use rand::Rng as _;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};
use tracing::{debug, info, warn};

/// Default lease TTL.
pub const DEFAULT_TTL: Duration = Duration::from_secs(10);

/// First backoff delay after a lost lease or a failed campaign.
const BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Backoff cap.
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Timeout for grant, resign and revoke calls.
const RPC_TIMEOUT: Duration = Duration::from_secs(3);
/// Lower bound for the keep-alive interval (TTLs are whole seconds, so this rarely applies).
const MIN_KEEPALIVE_INTERVAL: Duration = Duration::from_millis(100);

/// Runs the election until `shutdown` completes.
///
/// `tx` carries `true` while this process is the leader and `false` otherwise. It is only
/// changed (and receivers only woken) on transitions. `ttl` is rounded up to whole seconds
/// (etcd leases have second granularity, minimum 1 s).
pub async fn run_election(
    client: Client,
    election_name: String,
    ttl: Duration,
    identity: String,
    tx: watch::Sender<bool>,
    shutdown: impl Future<Output = ()>,
) {
    let shutdown = shutdown;
    tokio::pin!(shutdown);
    let ttl = round_ttl(ttl);
    let mut backoff = Backoff::new();
    loop {
        let end = session(
            &client,
            &election_name,
            ttl,
            &identity,
            &tx,
            shutdown.as_mut(),
            &mut backoff,
        )
        .await;
        publish(&tx, false);
        match end {
            End::Shutdown => {
                info!(election = %election_name, "leader election stopped");
                return;
            }
            End::Lost(reason) => {
                let delay = backoff.next_delay();
                warn!(
                    election = %election_name,
                    %reason,
                    retry_in = ?delay,
                    "not leader; campaigning again"
                );
                tokio::select! {
                    _ = shutdown.as_mut() => {
                        info!(election = %election_name, "leader election stopped");
                        return;
                    }
                    _ = sleep(delay) => {}
                }
            }
        }
    }
}

/// Why a session (one lease) ended.
#[derive(Debug)]
enum End {
    Shutdown,
    Lost(String),
}

/// Lease state as seen by the keep-alive task.
#[derive(Clone, Copy, Debug)]
enum LeaseState {
    /// Send time of the last keep-alive (or grant) the server confirmed.
    Alive(Instant),
    /// The server reported the lease as gone.
    Lost,
}

/// One lease: grant, keep alive, campaign, hold; always revokes the lease at the end.
async fn session<F: Future<Output = ()>>(
    client: &Client,
    name: &str,
    ttl: Duration,
    identity: &str,
    tx: &watch::Sender<bool>,
    mut shutdown: Pin<&mut F>,
    backoff: &mut Backoff,
) -> End {
    let mut lease = client.lease_client();
    let granted_at = Instant::now();
    let grant = tokio::select! {
        _ = shutdown.as_mut() => return End::Shutdown,
        r = timeout(RPC_TIMEOUT, lease.grant(ttl.as_secs() as i64, None)) => r,
    };
    let (id, ttl) = match grant {
        Ok(Ok(resp)) => {
            // Never assume more than we asked for, nor more than the server granted.
            let granted = Duration::from_secs(resp.ttl().max(0) as u64);
            (resp.id(), ttl.min(granted))
        }
        Ok(Err(e)) => return End::Lost(format!("lease grant failed: {e}")),
        Err(_) => return End::Lost("lease grant timed out".into()),
    };
    if ttl.is_zero() {
        return End::Lost("server granted a lease with TTL 0".into());
    }
    debug!(election = %name, lease = id, ?ttl, "lease granted");

    let every = keepalive_interval(ttl);
    let (state_tx, state_rx) = watch::channel(LeaseState::Alive(granted_at));
    let keeper = AbortOnDrop(tokio::spawn(keep_alive_loop(
        client.lease_client(),
        id,
        every,
        state_tx,
    )));

    let end = campaign_and_hold(
        client, name, ttl, every, identity, id, tx, shutdown, state_rx, backoff,
    )
    .await;

    // Stop claiming before anything else, then release the lease.
    publish(tx, false);
    drop(keeper);
    match timeout(RPC_TIMEOUT, lease.revoke(id)).await {
        Ok(Ok(_)) => debug!(election = %name, lease = id, "lease revoked"),
        Ok(Err(e)) => debug!(election = %name, lease = id, error = %e, "lease revoke failed"),
        Err(_) => debug!(election = %name, lease = id, "lease revoke timed out"),
    }
    end
}

#[allow(clippy::too_many_arguments)]
async fn campaign_and_hold<F: Future<Output = ()>>(
    client: &Client,
    name: &str,
    ttl: Duration,
    every: Duration,
    identity: &str,
    lease_id: i64,
    tx: &watch::Sender<bool>,
    mut shutdown: Pin<&mut F>,
    mut state_rx: watch::Receiver<LeaseState>,
    backoff: &mut Backoff,
) -> End {
    // Campaign: blocks server-side until our key is the oldest under the election prefix.
    let leader_key: LeaderKey = {
        let mut election = client.election_client();
        let campaign = election.campaign(name, identity, lease_id);
        tokio::pin!(campaign);
        loop {
            let expiry = match *state_rx.borrow() {
                LeaseState::Alive(t) => t + ttl,
                LeaseState::Lost => return End::Lost("lease lost while campaigning".into()),
            };
            tokio::select! {
                _ = shutdown.as_mut() => return End::Shutdown,
                r = &mut campaign => match r {
                    Ok(mut resp) => match resp.take_leader() {
                        Some(key) => break key,
                        None => return End::Lost("campaign returned no leader key".into()),
                    },
                    Err(e) => return End::Lost(format!("campaign failed: {e}")),
                },
                r = state_rx.changed() => if r.is_err() {
                    return End::Lost("lease keep-alive task stopped".into());
                },
                _ = sleep_until(expiry) => {
                    return End::Lost("lease expired while campaigning".into());
                }
            }
        }
    };

    // Hold: claim leadership while the lease is provably alive and our key exists.
    let key = leader_key.key().to_vec();
    let mut kv = client.kv_client();
    let mut check = tokio::time::interval(every);
    check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut announced = false;
    loop {
        let deadline = match *state_rx.borrow() {
            LeaseState::Alive(t) => t + ttl / 2,
            LeaseState::Lost => return End::Lost("lease lost (expired or revoked)".into()),
        };
        if Instant::now() >= deadline {
            return End::Lost(format!(
                "no successful lease keep-alive within {:?}",
                ttl / 2
            ));
        }
        if !announced {
            announced = true;
            backoff.reset();
            publish(tx, true);
            info!(
                election = %name,
                key = %String::from_utf8_lossy(&key),
                "elected leader"
            );
        }
        tokio::select! {
            _ = shutdown.as_mut() => {
                publish(tx, false);
                let mut election = client.election_client();
                let resign = election.resign(Some(ResignOptions::new().with_leader(leader_key)));
                match timeout(RPC_TIMEOUT, resign).await {
                    Ok(Ok(_)) => info!(election = %name, "resigned leadership"),
                    Ok(Err(e)) => warn!(election = %name, error = %e, "resign failed"),
                    Err(_) => warn!(election = %name, "resign timed out"),
                }
                return End::Shutdown;
            }
            r = state_rx.changed() => if r.is_err() {
                return End::Lost("lease keep-alive task stopped".into());
            },
            _ = sleep_until(deadline) => {}
            _ = check.tick() => {
                let until = deadline.min(Instant::now() + every);
                if let Ok(Ok(resp)) = timeout_at(until, kv.get(key.clone(), None)).await
                    && resp.kvs().is_empty()
                {
                    return End::Lost("election key was deleted".into());
                }
            }
        }
    }
}

/// Outcome of one keep-alive round trip.
enum KeepAlive {
    Renewed,
    NotFound,
}

/// Keeps lease `id` alive every `every`, publishing the send time of each confirmed keep-alive.
/// Reopens the stream after errors; exits when the server reports the lease gone or when no one
/// listens any more.
async fn keep_alive_loop(
    mut lease: LeaseClient,
    id: i64,
    every: Duration,
    state: watch::Sender<LeaseState>,
) {
    let mut stream: Option<(LeaseKeeper, LeaseKeepAliveStream)> = None;
    loop {
        sleep(every).await;
        if state.is_closed() {
            return;
        }
        let sent_at = Instant::now();
        match timeout(every, keep_alive_once(&mut lease, id, &mut stream)).await {
            Ok(Ok(KeepAlive::Renewed)) => {
                state.send_replace(LeaseState::Alive(sent_at));
            }
            Ok(Ok(KeepAlive::NotFound)) => {
                warn!(lease = id, "etcd reports the lease as expired or revoked");
                state.send_replace(LeaseState::Lost);
                return;
            }
            Ok(Err(e)) => {
                debug!(lease = id, error = %e, "lease keep-alive failed");
                stream = None;
            }
            Err(_) => {
                debug!(lease = id, "lease keep-alive timed out");
                stream = None;
            }
        }
    }
}

async fn keep_alive_once(
    lease: &mut LeaseClient,
    id: i64,
    stream: &mut Option<(LeaseKeeper, LeaseKeepAliveStream)>,
) -> Result<KeepAlive, EtcdClientError> {
    match stream {
        // Opening the stream sends a keep-alive and checks its answer.
        None => match lease.keep_alive(id).await {
            Ok(s) => {
                *stream = Some(s);
                Ok(KeepAlive::Renewed)
            }
            Err(EtcdClientError::LeaseKeepAliveError(msg)) if msg == "lease not found" => {
                Ok(KeepAlive::NotFound)
            }
            Err(e) => Err(e),
        },
        Some((keeper, messages)) => {
            keeper.keep_alive().await?;
            match messages.message().await? {
                Some(resp) if resp.ttl() > 0 => Ok(KeepAlive::Renewed),
                Some(_) => Ok(KeepAlive::NotFound),
                None => Err(EtcdClientError::LeaseKeepAliveError(
                    "keep-alive stream closed".into(),
                )),
            }
        }
    }
}

/// Sets the channel to `value`, waking receivers only on a change.
fn publish(tx: &watch::Sender<bool>, value: bool) {
    tx.send_if_modified(|current| {
        if *current == value {
            false
        } else {
            *current = value;
            true
        }
    });
}

/// Whole seconds, rounded up, at least 1 s.
fn round_ttl(ttl: Duration) -> Duration {
    let mut secs = ttl.as_secs();
    if ttl.subsec_nanos() > 0 {
        secs += 1;
    }
    Duration::from_secs(secs.max(1))
}

fn keepalive_interval(ttl: Duration) -> Duration {
    (ttl / 5).max(MIN_KEEPALIVE_INTERVAL)
}

/// Exponential backoff with jitter: each delay is uniform in [d/2, d], d doubling from
/// `BACKOFF_MIN` up to `BACKOFF_MAX`.
struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Self {
            current: BACKOFF_MIN,
        }
    }

    fn reset(&mut self) {
        self.current = BACKOFF_MIN;
    }

    fn next_delay(&mut self) -> Duration {
        let d = self.current;
        self.current = (self.current * 2).min(BACKOFF_MAX);
        let half = d.as_millis() as u64 / 2;
        Duration::from_millis(half + rand::rng().random_range(0..=half))
    }
}

/// Aborts the task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_rounds_up_to_whole_seconds() {
        assert_eq!(round_ttl(Duration::from_secs(10)), Duration::from_secs(10));
        assert_eq!(
            round_ttl(Duration::from_millis(2500)),
            Duration::from_secs(3)
        );
        assert_eq!(round_ttl(Duration::ZERO), Duration::from_secs(1));
        assert_eq!(round_ttl(Duration::from_millis(1)), Duration::from_secs(1));
    }

    #[test]
    fn keepalive_interval_leaves_two_tries_before_step_down() {
        let ttl = DEFAULT_TTL;
        let every = keepalive_interval(ttl);
        assert_eq!(every, Duration::from_secs(2));
        // Step-down at ttl/2 must allow at least two keep-alive attempts.
        assert!(every * 2 <= ttl / 2);
        assert_eq!(
            keepalive_interval(Duration::from_millis(200)),
            MIN_KEEPALIVE_INTERVAL
        );
    }

    #[test]
    fn backoff_grows_with_jitter_and_caps() {
        let mut b = Backoff::new();
        let mut expected = BACKOFF_MIN;
        for _ in 0..10 {
            let d = b.next_delay();
            assert!(d >= expected / 2 && d <= expected, "{d:?} vs {expected:?}");
            expected = (expected * 2).min(BACKOFF_MAX);
        }
        assert_eq!(b.current, BACKOFF_MAX);
        b.reset();
        assert!(b.next_delay() <= BACKOFF_MIN);
    }

    #[test]
    fn publish_only_wakes_on_change() {
        let (tx, mut rx) = watch::channel(false);
        publish(&tx, false);
        assert!(!rx.has_changed().unwrap());
        publish(&tx, true);
        assert!(rx.has_changed().unwrap());
        assert!(*rx.borrow_and_update());
        publish(&tx, true);
        assert!(!rx.has_changed().unwrap());
    }
}
