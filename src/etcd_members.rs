//! Keeps a multi-endpoint etcd client off members that do not answer.
//!
//! etcd-client balances requests over every endpoint of the URL and does not skip one it
//! cannot reach: a request sent to a member that is down, not resolvable yet (a
//! StatefulSet pod name before the pod exists) or not serving yet fails, so with one of
//! three members gone about a third of the requests fail, and on a fresh install most of
//! them until a restart. [`spawn`] probes every member every [`PROBE_EVERY`] with a
//! `Status` call on a client of its own and removes the members that do not answer within
//! [`PROBE_TIMEOUT`] from the shared client's balancer, then adds them back once they
//! answer again. When no member answers, all of them stay in, so a request can still go
//! out once any comes back. A URL with one endpoint (a Service) is left alone.

use std::time::Duration;

use etcd_client::{Client, ConnectOptions};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tracing::{debug, info, warn};

use crate::etcd::{EtcdError, EtcdTarget};

/// How often every member is probed.
pub const PROBE_EVERY: Duration = Duration::from_secs(2);
/// How long a member has to answer a probe.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

struct Member {
    /// As written in the URL; what `add_endpoint` takes.
    endpoint: String,
    /// With the scheme etcd-client added; what `remove_endpoint` compares.
    uri: String,
    /// In the shared client's balancer.
    active: bool,
    /// A client for this member only, connected on the first probe.
    probe: Option<Client>,
}

/// Starts the probe task for `client` when `target` lists more than one endpoint.
pub fn spawn(client: &Client, target: &EtcdTarget) -> Result<(), EtcdError> {
    if target.endpoints.len() < 2 {
        return Ok(());
    }
    let options = target
        .connect_options()?
        .with_timeout(PROBE_TIMEOUT)
        .with_connect_timeout(PROBE_TIMEOUT);
    let scheme = if target.tls_wanted() {
        "https://"
    } else {
        "http://"
    };
    let members = target
        .endpoints
        .iter()
        .map(|endpoint| Member {
            endpoint: endpoint.clone(),
            uri: if endpoint.contains("://") {
                endpoint.clone()
            } else {
                format!("{scheme}{endpoint}")
            },
            active: true,
            probe: None,
        })
        .collect();
    tokio::spawn(run(client.clone(), members, options));
    Ok(())
}

async fn run(client: Client, mut members: Vec<Member>, options: ConnectOptions) {
    let mut tick = interval(PROBE_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let answers = probe_all(&mut members, &options).await;
        let any = answers.iter().any(|ok| *ok);
        for (member, ok) in members.iter_mut().zip(answers) {
            // With no member answering there is nothing better to send requests to.
            let wanted = ok || !any;
            if wanted && !member.active {
                match client.add_endpoint(&member.endpoint).await {
                    Ok(()) => {
                        member.active = true;
                        info!(endpoint = %member.endpoint, "etcd member answers again; sending requests to it");
                    }
                    Err(e) => {
                        debug!(endpoint = %member.endpoint, error = %e, "adding etcd endpoint failed")
                    }
                }
            } else if !wanted && member.active {
                match client.remove_endpoint(&member.uri).await {
                    Ok(()) => {
                        member.active = false;
                        warn!(endpoint = %member.endpoint, "etcd member does not answer; not sending requests to it");
                    }
                    Err(e) => {
                        debug!(endpoint = %member.endpoint, error = %e, "removing etcd endpoint failed")
                    }
                }
            }
        }
    }
}

/// Whether each member answered a `Status` call within [`PROBE_TIMEOUT`].
async fn probe_all(members: &mut [Member], options: &ConnectOptions) -> Vec<bool> {
    let mut probes = JoinSet::new();
    for (i, member) in members.iter_mut().enumerate() {
        if member.probe.is_none() {
            // Lazy: connects on the first call, except to authenticate a user.
            let connect = Client::connect([member.endpoint.as_str()], Some(options.clone()));
            match timeout(PROBE_TIMEOUT, connect).await {
                Ok(Ok(c)) => member.probe = Some(c),
                Ok(Err(e)) => {
                    debug!(endpoint = %member.endpoint, error = %e, "etcd probe connect failed")
                }
                Err(_) => debug!(endpoint = %member.endpoint, "etcd probe connect timed out"),
            }
        }
        if let Some(probe) = member.probe.clone() {
            probes.spawn(async move {
                let mut maintenance = probe.maintenance_client();
                let ok = matches!(
                    timeout(PROBE_TIMEOUT, maintenance.status()).await,
                    Ok(Ok(_))
                );
                (i, ok)
            });
        }
    }
    let mut answers = vec![false; members.len()];
    while let Some(done) = probes.join_next().await {
        if let Ok((i, ok)) = done {
            answers[i] = ok;
        }
    }
    answers
}
