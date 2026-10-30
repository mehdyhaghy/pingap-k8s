//! Kubernetes Events, opt-in with `[kubernetes] events = true`.
//!
//! What is posted (events.k8s.io/v1, `reportingController` "pingap-k8s",
//! `reportingInstance` the pod name):
//!
//! | Reason | Type | Regarding | Posted by |
//! |---|---|---|---|
//! | `ServiceMissing`, `PortNotInService`, `PortNotInSlices` | Warning | the Service | leader |
//! | `DrainFallbackStarted` / `DrainFallbackEnded` | Warning / Normal | each Service of the upstream | leader |
//! | `MalformedUpstream` | Warning | the controller's own Pod | leader |
//! | `EtcdUnreachable` / `EtcdRecovered` | Warning / Normal | the controller's own Pod | every replica |
//!
//! A malformed upstream is an etcd entry, not a Kubernetes object, so its
//! Event is about the controller Pod that found it (the note names the
//! upstream). etcd reachability is per replica, so each replica reports its
//! own on its own Pod.
//!
//! Spam protection, in three layers:
//! - the callers post once per episode, with the same dedupe as their warning
//!   logs (a resolve warning when it first appears, the drain fallback on
//!   enter and exit, a malformed entry once until it is writable again, etcd
//!   on a change of `etcd_up`);
//! - kube's [`Recorder`] turns a repeat of the same reason on the same object
//!   within 6 minutes into a PATCH of the existing Event (`series.count`),
//!   not a new Event;
//! - [`SpamFilter`]: per regarding object a burst of 25, then one Event per
//!   5 minutes (client-go's `EventSourceObjectSpamFilter` defaults).
//!
//! Posting never blocks the caller: notices go through a bounded queue
//! (dropped when full) to one task that calls the API server. A failed post
//! (for example missing RBAC) is logged once as a warning, then at debug
//! level until a post succeeds again.

use std::collections::HashMap;
use std::time::Duration;

use k8s_openapi::api::core::v1::ObjectReference;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::resolve::ResolveWarning;
use crate::types::ServiceKey;

/// `reportingController` of every Event (`kubectl get events
/// --field-selector reportingComponent=pingap-k8s`).
pub const REPORTING_CONTROLLER: &str = "pingap-k8s";
/// Notices waiting to be posted; more are dropped.
const QUEUE: usize = 256;
/// Events per regarding object before the refill rate applies.
const BURST: u32 = 25;
/// One more Event per regarding object per this interval after the burst.
const REFILL: Duration = Duration::from_secs(300);
/// The API server limit of an Event note.
const NOTE_MAX: usize = 1024;

/// The object an Event is about.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Regarding {
    /// `uid` makes the Event show in `kubectl describe service` (which
    /// filters by UID); `None` when the Service does not exist.
    Service {
        key: ServiceKey,
        uid: Option<String>,
    },
    /// The controller's own Pod.
    Pod,
}

/// One Event to post.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notice {
    pub regarding: Regarding,
    pub type_: EventType,
    pub reason: &'static str,
    pub action: &'static str,
    pub note: String,
}

impl Notice {
    /// A `services` entry of `upstream` resolved to nothing.
    pub fn resolve_warning(upstream: &str, warning: &ResolveWarning, uid: Option<String>) -> Self {
        let (key, reason) = match warning {
            ResolveWarning::ServiceMissing(key) => (key, "ServiceMissing"),
            ResolveWarning::PortNotInService { service, .. } => (service, "PortNotInService"),
            ResolveWarning::PortNotInSlices { service, .. } => (service, "PortNotInSlices"),
            ResolveWarning::ExternalNameNotEnabled(key) => (key, "ExternalNameNotEnabled"),
            ResolveWarning::ExternalNameInvalid { service, .. } => (service, "ExternalNameInvalid"),
            ResolveWarning::TrafficPolicyIgnored { service, .. } => {
                (service, "TrafficPolicyIgnored")
            }
        };
        Notice {
            regarding: Regarding::Service {
                key: key.clone(),
                uid,
            },
            type_: EventType::Warning,
            reason,
            action: "Resolve",
            note: note(format!("upstream {upstream}: {warning}")),
        }
    }

    /// The drain fallback of `upstream` started (`endpoints` draining ones
    /// are written) or ended (`endpoints` ready ones), on one of its Services.
    pub fn drain_fallback(
        upstream: &str,
        service: &ServiceKey,
        uid: Option<String>,
        started: bool,
        endpoints: usize,
    ) -> Self {
        let (type_, reason, text) = if started {
            (
                EventType::Warning,
                "DrainFallbackStarted",
                format!(
                    "upstream {upstream}: no ready endpoints; writing {endpoints} draining endpoint(s) until one is ready"
                ),
            )
        } else {
            (
                EventType::Normal,
                "DrainFallbackEnded",
                format!("upstream {upstream}: {endpoints} ready endpoint(s) again"),
            )
        };
        Notice {
            regarding: Regarding::Service {
                key: service.clone(),
                uid,
            },
            type_,
            reason,
            action: "WriteAddrs",
            note: note(text),
        }
    }

    /// The stored entry of `upstream` cannot be edited.
    pub fn malformed_upstream(upstream: &str, error: &str) -> Self {
        Notice {
            regarding: Regarding::Pod,
            type_: EventType::Warning,
            reason: "MalformedUpstream",
            action: "WriteAddrs",
            note: note(format!(
                "upstream {upstream}: stored entry is malformed; not written until it is fixed: {error}"
            )),
        }
    }

    /// `etcd_up` changed.
    pub fn etcd(reachable: bool) -> Self {
        let (type_, reason, text) = if reachable {
            (
                EventType::Normal,
                "EtcdRecovered",
                "etcd is reachable again",
            )
        } else {
            (
                EventType::Warning,
                "EtcdUnreachable",
                "etcd cannot be reached",
            )
        };
        Notice {
            regarding: Regarding::Pod,
            type_,
            reason,
            action: "ConnectEtcd",
            note: text.to_string(),
        }
    }

    fn to_event(&self) -> Event {
        Event {
            type_: self.type_,
            reason: self.reason.to_string(),
            note: Some(self.note.clone()),
            action: self.action.to_string(),
            secondary: None,
        }
    }
}

/// Truncates to the API server's note limit, on a character boundary.
fn note(mut text: String) -> String {
    if text.len() > NOTE_MAX {
        let mut end = NOTE_MAX - 3;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("...");
    }
    text
}

/// Where notices go. Cheap to clone; the default posts nothing.
#[derive(Clone, Debug, Default)]
pub struct Events {
    tx: Option<mpsc::Sender<Notice>>,
}

impl Events {
    /// Events off (`[kubernetes] events = false`).
    pub fn disabled() -> Self {
        Self::default()
    }

    /// A handle whose notices arrive on the returned receiver (tests, and
    /// [`Events::spawn`]).
    pub fn channel(capacity: usize) -> (Self, mpsc::Receiver<Notice>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx: Some(tx) }, rx)
    }

    /// Starts the task that posts notices as `pod_name` (reportingInstance).
    /// Pod-scoped Events are about `pod_namespace/pod_name` (`pod_uid`, from
    /// the downward API, makes them show in `kubectl describe pod`).
    pub fn spawn(
        client: kube::Client,
        pod_name: String,
        pod_namespace: String,
        pod_uid: Option<String>,
    ) -> Self {
        let pod = ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("Pod".into()),
            name: Some(pod_name.clone()),
            namespace: Some(pod_namespace),
            uid: pod_uid.filter(|u| !u.is_empty()),
            ..Default::default()
        };
        let recorder = Recorder::new(
            client,
            Reporter {
                controller: REPORTING_CONTROLLER.into(),
                instance: Some(pod_name),
            },
        );
        let (events, rx) = Self::channel(QUEUE);
        tokio::spawn(publish(recorder, pod, rx));
        info!("posting Kubernetes Events (events.k8s.io/v1)");
        events
    }

    pub fn is_enabled(&self) -> bool {
        self.tx.is_some()
    }

    /// Queues `notice`; never waits. Dropped when the queue is full.
    pub fn post(&self, notice: Notice) {
        let Some(tx) = &self.tx else { return };
        if let Err(err) = tx.try_send(notice) {
            debug!(
                reason = err.into_inner().reason,
                "event not posted: queue full or closed"
            );
        }
    }
}

/// The reference the API server gets for `regarding`.
fn reference(regarding: &Regarding, pod: &ObjectReference) -> ObjectReference {
    match regarding {
        Regarding::Service { key, uid } => ObjectReference {
            api_version: Some("v1".into()),
            kind: Some("Service".into()),
            name: Some(key.name.clone()),
            namespace: Some(key.namespace.clone()),
            uid: uid.clone(),
            ..Default::default()
        },
        Regarding::Pod => pod.clone(),
    }
}

async fn publish(recorder: Recorder, pod: ObjectReference, mut rx: mpsc::Receiver<Notice>) {
    let mut filter = SpamFilter::default();
    let mut failing = false;
    while let Some(notice) = rx.recv().await {
        let reference = reference(&notice.regarding, &pod);
        let object = format!(
            "{}/{}/{}",
            reference.kind.as_deref().unwrap_or_default(),
            reference.namespace.as_deref().unwrap_or_default(),
            reference.name.as_deref().unwrap_or_default()
        );
        if !filter.allow(&object, Instant::now()) {
            debug!(%object, reason = notice.reason, "event suppressed by the spam filter");
            continue;
        }
        match recorder.publish(&notice.to_event(), &reference).await {
            Ok(()) => {
                if std::mem::take(&mut failing) {
                    info!("posting Kubernetes Events works again");
                }
            }
            Err(err) if !failing => {
                failing = true;
                warn!(
                    %object,
                    reason = notice.reason,
                    "posting a Kubernetes Event failed (needs create and patch on events.k8s.io events in that namespace): {err}"
                );
            }
            Err(err) => {
                debug!(%object, reason = notice.reason, "posting a Kubernetes Event failed: {err}")
            }
        }
    }
}

/// Token bucket per regarding object: `BURST` Events, then one per `REFILL`.
#[derive(Default)]
struct SpamFilter {
    buckets: HashMap<String, (u32, Instant)>,
}

impl SpamFilter {
    fn allow(&mut self, object: &str, now: Instant) -> bool {
        // Keep the map small: a bucket that has refilled completely is the
        // same as no bucket.
        if self.buckets.len() > 1024 {
            self.buckets
                .retain(|_, (tokens, at)| refill(*tokens, *at, now).0 < BURST);
        }
        let (tokens, at) = self
            .buckets
            .get(object)
            .map_or((BURST, now), |(tokens, at)| refill(*tokens, *at, now));
        let allowed = tokens > 0;
        self.buckets
            .insert(object.to_string(), (tokens.saturating_sub(1), at));
        allowed
    }
}

/// Tokens after refilling since `at`, and the time the next refill counts from.
fn refill(tokens: u32, at: Instant, now: Instant) -> (u32, Instant) {
    let gained = (now.saturating_duration_since(at).as_secs() / REFILL.as_secs()) as u32;
    if gained == 0 {
        return (tokens, at);
    }
    let tokens = tokens.saturating_add(gained).min(BURST);
    (tokens, at + REFILL * gained)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PortRef;

    fn key(name: &str) -> ServiceKey {
        ServiceKey {
            namespace: "apps".into(),
            name: name.into(),
        }
    }

    #[test]
    fn resolve_warnings_map_to_service_events() {
        let cases = [
            (ResolveWarning::ServiceMissing(key("web")), "ServiceMissing"),
            (
                ResolveWarning::PortNotInService {
                    service: key("web"),
                    port: PortRef::Name("http".into()),
                },
                "PortNotInService",
            ),
            (
                ResolveWarning::PortNotInSlices {
                    service: key("web"),
                    port: PortRef::Number(80),
                    port_name: "http".into(),
                },
                "PortNotInSlices",
            ),
            (
                ResolveWarning::ExternalNameNotEnabled(key("web")),
                "ExternalNameNotEnabled",
            ),
            (
                ResolveWarning::ExternalNameInvalid {
                    service: key("web"),
                    reason: "spec.externalName is empty".into(),
                },
                "ExternalNameInvalid",
            ),
            (
                ResolveWarning::TrafficPolicyIgnored {
                    service: key("web"),
                    setting: "trafficDistribution: PreferSameZone".into(),
                },
                "TrafficPolicyIgnored",
            ),
        ];
        for (warning, reason) in cases {
            let n = Notice::resolve_warning("up", &warning, Some("u1".into()));
            assert_eq!(n.reason, reason);
            assert_eq!(n.type_, EventType::Warning);
            assert_eq!(
                n.regarding,
                Regarding::Service {
                    key: key("web"),
                    uid: Some("u1".into())
                }
            );
            assert_eq!(n.note, format!("upstream up: {warning}"));
            let event = n.to_event();
            assert_eq!(event.reason, reason);
            assert_eq!(event.action, "Resolve");
        }
    }

    #[test]
    fn drain_malformed_and_etcd_mapping() {
        let n = Notice::drain_fallback("up", &key("web"), None, true, 2);
        assert_eq!(
            (n.type_, n.reason),
            (EventType::Warning, "DrainFallbackStarted")
        );
        assert!(n.note.contains("2 draining"), "{}", n.note);
        let n = Notice::drain_fallback("up", &key("web"), None, false, 3);
        assert_eq!(
            (n.type_, n.reason),
            (EventType::Normal, "DrainFallbackEnded")
        );

        let n = Notice::malformed_upstream("up", "bad toml");
        assert_eq!(n.regarding, Regarding::Pod);
        assert_eq!(
            (n.type_, n.reason),
            (EventType::Warning, "MalformedUpstream")
        );
        assert!(n.note.starts_with("upstream up: ") && n.note.ends_with("bad toml"));

        assert_eq!(Notice::etcd(false).reason, "EtcdUnreachable");
        assert_eq!(Notice::etcd(false).type_, EventType::Warning);
        assert_eq!(Notice::etcd(true).reason, "EtcdRecovered");
        assert_eq!(Notice::etcd(true).type_, EventType::Normal);
        assert_eq!(Notice::etcd(true).regarding, Regarding::Pod);
    }

    #[test]
    fn long_notes_are_truncated_on_a_char_boundary() {
        let n = Notice::malformed_upstream("up", &"é".repeat(2000));
        assert!(n.note.len() <= NOTE_MAX, "{}", n.note.len());
        assert!(n.note.ends_with("..."));
    }

    #[test]
    fn references() {
        let pod = ObjectReference {
            kind: Some("Pod".into()),
            name: Some("p".into()),
            ..Default::default()
        };
        assert_eq!(reference(&Regarding::Pod, &pod), pod);
        let svc = reference(
            &Regarding::Service {
                key: key("web"),
                uid: Some("u".into()),
            },
            &pod,
        );
        assert_eq!(svc.kind.as_deref(), Some("Service"));
        assert_eq!(svc.api_version.as_deref(), Some("v1"));
        assert_eq!(svc.namespace.as_deref(), Some("apps"));
        assert_eq!(svc.name.as_deref(), Some("web"));
        assert_eq!(svc.uid.as_deref(), Some("u"));
    }

    #[test]
    fn spam_filter_bursts_then_refills_per_object() {
        let mut f = SpamFilter::default();
        let t0 = Instant::now();
        for _ in 0..BURST {
            assert!(f.allow("Service/apps/web", t0));
        }
        assert!(!f.allow("Service/apps/web", t0));
        // Other objects have their own bucket.
        assert!(f.allow("Service/apps/api", t0));
        // One more per REFILL.
        assert!(!f.allow("Service/apps/web", t0 + REFILL - Duration::from_secs(1)));
        assert!(f.allow("Service/apps/web", t0 + REFILL));
        assert!(!f.allow("Service/apps/web", t0 + REFILL + Duration::from_secs(1)));
        // Refills up to the burst, no further.
        let later = t0 + REFILL * 1000;
        for _ in 0..BURST {
            assert!(f.allow("Service/apps/web", later));
        }
        assert!(!f.allow("Service/apps/web", later));
    }

    type Requests = std::sync::Arc<std::sync::Mutex<Vec<(String, String, serde_json::Value)>>>;

    /// A kube client whose API server records every request (method, path, JSON
    /// body) and answers with the body it got; 403 for namespace "denied".
    fn mock_client() -> (kube::Client, Requests) {
        let requests = Requests::default();
        let log = requests.clone();
        let service = tower::service_fn(move |req: http::Request<kube::client::Body>| {
            let log = log.clone();
            async move {
                let (parts, body) = req.into_parts();
                let body = body.collect_bytes().await?;
                let path = parts.uri.path().to_string();
                let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                log.lock()
                    .unwrap()
                    .push((parts.method.to_string(), path.clone(), json));
                let response = if path.contains("/namespaces/denied/") {
                    http::Response::builder().status(403).body(kube::client::Body::from(
                        br#"{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}"#.to_vec(),
                    ))
                } else {
                    http::Response::builder()
                        .status(201)
                        .body(kube::client::Body::from(body.to_vec()))
                };
                Ok::<_, kube::Error>(response.unwrap())
            }
        });
        (kube::Client::new(service, "default"), requests)
    }

    async fn wait_requests(
        requests: &Requests,
        n: usize,
    ) -> Vec<(String, String, serde_json::Value)> {
        for _ in 0..500 {
            let seen = requests.lock().unwrap().clone();
            if seen.len() >= n {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("expected {n} requests, got {:?}", requests.lock().unwrap());
    }

    #[tokio::test]
    async fn publish_posts_service_and_pod_events() {
        let (client, requests) = mock_client();
        let events = Events::spawn(
            client,
            "pk-0".into(),
            "pingap-k8s".into(),
            Some("pod-uid".into()),
        );
        let missing = ResolveWarning::ServiceMissing(key("web"));
        events.post(Notice::resolve_warning(
            "up",
            &missing,
            Some("svc-uid".into()),
        ));
        events.post(Notice::etcd(false));
        events.post(Notice::etcd(false));

        let seen = wait_requests(&requests, 3).await;
        let (method, path, body) = &seen[0];
        assert_eq!(method, "POST");
        assert_eq!(path, "/apis/events.k8s.io/v1/namespaces/apps/events");
        assert_eq!(body["regarding"]["kind"], "Service");
        assert_eq!(body["regarding"]["name"], "web");
        assert_eq!(body["regarding"]["uid"], "svc-uid");
        assert_eq!(body["reason"], "ServiceMissing");
        assert_eq!(body["type"], "Warning");
        assert_eq!(body["reportingController"], REPORTING_CONTROLLER);
        assert_eq!(body["reportingInstance"], "pk-0");
        assert_eq!(body["note"], format!("upstream up: {missing}"));

        let (method, path, body) = &seen[1];
        assert_eq!(method, "POST");
        assert_eq!(path, "/apis/events.k8s.io/v1/namespaces/pingap-k8s/events");
        assert_eq!(body["regarding"]["kind"], "Pod");
        assert_eq!(body["regarding"]["name"], "pk-0");
        assert_eq!(body["regarding"]["uid"], "pod-uid");
        assert_eq!(body["reason"], "EtcdUnreachable");

        // The same Event again: the recorder updates its series instead of a new Event.
        let (method, path, body) = &seen[2];
        assert_eq!(method, "PATCH");
        let name = seen[1].2["metadata"]["name"].as_str().unwrap();
        assert_eq!(
            path,
            &format!("/apis/events.k8s.io/v1/namespaces/pingap-k8s/events/{name}")
        );
        assert_eq!(body["series"]["count"], 2);
    }

    #[tokio::test]
    async fn publish_applies_the_spam_filter_and_survives_errors() {
        let (client, requests) = mock_client();
        let events = Events::spawn(client, "pk-0".into(), "pingap-k8s".into(), None);
        let denied = ResolveWarning::ServiceMissing(ServiceKey {
            namespace: "denied".into(),
            name: "web".into(),
        });
        events.post(Notice::resolve_warning("up", &denied, None));
        let missing = ResolveWarning::ServiceMissing(key("web"));
        for _ in 0..BURST + 5 {
            events.post(Notice::resolve_warning("up", &missing, None));
        }
        events.post(Notice::etcd(true));

        let seen = wait_requests(&requests, 1 + BURST as usize + 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let seen_after = requests.lock().unwrap().clone();
        assert_eq!(seen_after.len(), seen.len(), "{seen_after:?}");
        assert!(seen[0].1.contains("/namespaces/denied/"));
        let web = seen
            .iter()
            .filter(|r| r.1.contains("/namespaces/apps/"))
            .count();
        assert_eq!(web, BURST as usize);
        let last = seen.last().unwrap();
        assert_eq!(last.2["reason"], "EtcdRecovered");
        assert!(last.2["regarding"].get("uid").is_none());
    }

    #[test]
    fn post_never_blocks_and_disabled_posts_nothing() {
        Events::disabled().post(Notice::etcd(true));
        assert!(!Events::disabled().is_enabled());

        let (events, mut rx) = Events::channel(1);
        assert!(events.is_enabled());
        events.post(Notice::etcd(false));
        events.post(Notice::etcd(true)); // queue full: dropped
        assert_eq!(rx.try_recv().unwrap(), Notice::etcd(false));
        assert!(rx.try_recv().is_err());
        drop(rx);
        events.post(Notice::etcd(true)); // closed: dropped
    }
}
