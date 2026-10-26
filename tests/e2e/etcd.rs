//! Reads and writes Pingap's etcd prefix with `kubectl exec <etcd pod> -- etcdctl`.
//!
//! The bundled etcd pod has ETCDCTL_ENDPOINTS/CACERT/CERT/KEY set (client-cert auth over
//! 127.0.0.1), so nothing is port-forwarded and etcd is never reachable from outside the
//! cluster. Output is read with `-w json` (keys and values base64).

use std::process::Child;

use k8s_openapi::serde_json::{self, Value};

use crate::kubectl::{CmdOutput, Kubectl, collect};

#[derive(Debug, Clone)]
pub struct Etcd {
    kubectl: Kubectl,
    namespace: String,
    pod: String,
    /// Key prefix with a leading slash and no trailing one, e.g. "/pingap".
    prefix: String,
}

/// One key as etcd stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
}

/// One event of an [`EtcdWatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchEvent {
    /// "PUT" or "DELETE".
    pub kind: String,
    pub kv: KeyValue,
}

impl Etcd {
    pub fn new(kubectl: Kubectl, namespace: &str, pod: &str, prefix: &str) -> Self {
        Etcd {
            kubectl,
            namespace: namespace.into(),
            pod: pod.into(),
            prefix: format!("/{}", prefix.trim_matches('/')),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The key Pingap reads for upstream `name`: `<prefix>/upstreams/<name>.toml`.
    pub fn upstream_key(&self, name: &str) -> String {
        format!("{}/upstreams/{name}.toml", self.prefix)
    }

    /// Runs `etcdctl <args>` in the etcd pod; non-zero exit is an error.
    pub async fn etcdctl(&self, args: &[&str]) -> anyhow::Result<String> {
        let out = self.etcdctl_output(args, None).await?;
        if !out.success {
            anyhow::bail!("etcdctl {} failed: {}", args.join(" "), out.stderr.trim());
        }
        Ok(out.stdout)
    }

    async fn etcdctl_output(
        &self,
        args: &[&str],
        stdin: Option<&[u8]>,
    ) -> anyhow::Result<CmdOutput> {
        let mut cmd = vec!["etcdctl"];
        cmd.extend_from_slice(args);
        match stdin {
            Some(input) => {
                self.kubectl
                    .exec_stdin(&self.namespace, &self.pod, Some("etcd"), &cmd, input)
                    .await
            }
            None => {
                self.kubectl
                    .exec(&self.namespace, &self.pod, Some("etcd"), &cmd)
                    .await
            }
        }
    }

    /// `etcdctl endpoint health`: Ok when etcd answers.
    pub async fn health(&self) -> anyhow::Result<()> {
        self.etcdctl(&["endpoint", "health"]).await.map(|_| ())
    }

    /// Current store revision.
    pub async fn revision(&self) -> anyhow::Result<i64> {
        // Any key will do: the response header carries the store revision.
        let v = self.get_json(&[&self.prefix]).await?;
        v.pointer("/header/revision")
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow::anyhow!("no header.revision in etcdctl output"))
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Option<KeyValue>> {
        let v = self.get_json(&[key]).await?;
        Ok(kvs(&v)?.into_iter().next())
    }

    /// All keys under `prefix` (an absolute key prefix), sorted by key.
    pub async fn get_prefix(&self, prefix: &str) -> anyhow::Result<Vec<KeyValue>> {
        let v = self.get_json(&[prefix, "--prefix"]).await?;
        kvs(&v)
    }

    async fn get_json(&self, args: &[&str]) -> anyhow::Result<Value> {
        let mut a = vec!["get", "-w", "json"];
        a.extend_from_slice(args);
        let out = self.etcdctl(&a).await?;
        Ok(serde_json::from_str(out.trim())?)
    }

    /// Writes `value` to `key` (value passed on stdin, so any TOML works).
    pub async fn put(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let out = self
            .etcdctl_output(&["put", key], Some(value.as_bytes()))
            .await?;
        if !out.success {
            anyhow::bail!("etcdctl put {key} failed: {}", out.stderr.trim());
        }
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.etcdctl(&["del", key]).await.map(|_| ())
    }

    /// The stored TOML of upstream `name`.
    pub async fn upstream(&self, name: &str) -> anyhow::Result<Option<KeyValue>> {
        self.get(&self.upstream_key(name)).await
    }

    /// `upstreams.<name>.addrs` as stored (Pingap's "ip:port[ weight]" strings), sorted.
    /// `None` when the key or the field does not exist.
    pub async fn upstream_addrs(&self, name: &str) -> anyhow::Result<Option<Vec<String>>> {
        let Some(kv) = self.upstream(name).await? else {
            return Ok(None);
        };
        let mut addrs = pingap_k8s::etcd::read_addrs(&kv.value, name)?;
        if let Some(a) = addrs.as_mut() {
            a.sort();
        }
        Ok(addrs)
    }

    /// Starts `etcdctl watch --prefix <prefix>` (from revision `from` when given). Events are
    /// read when the watch is stopped with [`EtcdWatch::stop`].
    pub fn watch(&self, prefix: &str, from: Option<i64>) -> anyhow::Result<EtcdWatch> {
        let rev = from.map(|r| format!("--rev={r}"));
        let mut cmd = vec!["etcdctl", "watch", "-w", "json", "--prefix", prefix];
        if let Some(r) = rev.as_deref() {
            cmd.push(r);
        }
        let child = self
            .kubectl
            .spawn_exec(&self.namespace, &self.pod, Some("etcd"), &cmd)?;
        Ok(EtcdWatch { child: Some(child) })
    }
}

/// A running `etcdctl watch`. For counting writes over a period (e.g. during a rollout),
/// prefer comparing [`KeyValue::version`] before and after: it needs no background process.
pub struct EtcdWatch {
    child: Option<Child>,
}

impl EtcdWatch {
    /// Stops the watch and returns every event it printed.
    pub async fn stop(mut self) -> anyhow::Result<Vec<WatchEvent>> {
        let mut child = self.child.take().expect("watch already stopped");
        // kubectl exec forwards the kill as a closed stream; etcdctl exits with it.
        let _ = child.kill();
        let out = collect(child).await?;
        parse_watch_output(&out.stdout)
    }
}

impl Drop for EtcdWatch {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn b64_field(v: &Value, field: &str) -> anyhow::Result<String> {
    match v.get(field).and_then(Value::as_str) {
        Some(s) => Ok(String::from_utf8(base64_decode(s)?)?),
        None => Ok(String::new()),
    }
}

fn kv_from(v: &Value) -> anyhow::Result<KeyValue> {
    let num = |f: &str| v.get(f).and_then(Value::as_i64).unwrap_or(0);
    Ok(KeyValue {
        key: b64_field(v, "key")?,
        value: b64_field(v, "value")?,
        create_revision: num("create_revision"),
        mod_revision: num("mod_revision"),
        version: num("version"),
    })
}

fn kvs(v: &Value) -> anyhow::Result<Vec<KeyValue>> {
    let mut out = match v.get("kvs").and_then(Value::as_array) {
        Some(a) => a.iter().map(kv_from).collect::<anyhow::Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    out.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(out)
}

/// `etcdctl watch -w json` prints one JSON object per watch response, one per line.
fn parse_watch_output(stdout: &str) -> anyhow::Result<Vec<WatchEvent>> {
    let mut events = Vec::new();
    for line in stdout.lines().map(str::trim).filter(|l| l.starts_with('{')) {
        let v: Value = serde_json::from_str(line)?;
        for e in v
            .get("Events")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let kind = match e.get("type").and_then(Value::as_i64).unwrap_or(0) {
                1 => "DELETE",
                _ => "PUT",
            };
            let kv = e
                .get("kv")
                .map(kv_from)
                .transpose()?
                .ok_or_else(|| anyhow::anyhow!("watch event without kv"))?;
            events.push(WatchEvent {
                kind: kind.into(),
                kv,
            });
        }
    }
    Ok(events)
}

/// Standard base64 (padding optional), as etcdctl prints keys and values.
pub fn base64_decode(input: &str) -> anyhow::Result<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    }
    let bytes: Vec<u8> = input.bytes().filter(|&c| c != b'=').collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        for (n, &c) in chunk.iter().enumerate() {
            let v = val(c).ok_or_else(|| anyhow::anyhow!("bad base64 byte {c}"))?;
            acc |= v << (18 - 6 * n);
        }
        let produced = match chunk.len() {
            4 => 3,
            3 => 2,
            2 => 1,
            _ => anyhow::bail!("bad base64 length"),
        };
        out.extend_from_slice(&acc.to_be_bytes()[1..1 + produced]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_base64() {
        assert_eq!(base64_decode("L3BpbmdhcC91").unwrap(), b"/pingap/u");
        assert_eq!(base64_decode("YQ==").unwrap(), b"a");
        assert_eq!(base64_decode("YWI=").unwrap(), b"ab");
        assert!(base64_decode("Y").is_err());
    }

    #[test]
    fn parses_get_json() {
        let v: Value = serde_json::from_str(
            r#"{"header":{"revision":9},"kvs":[{"key":"L2I=","create_revision":2,"mod_revision":5,"version":3,"value":"eA=="},{"key":"L2E=","create_revision":1,"mod_revision":1,"version":1}],"count":2}"#,
        )
        .unwrap();
        let kv = kvs(&v).unwrap();
        assert_eq!(kv[0].key, "/a");
        assert_eq!(kv[0].value, "");
        assert_eq!(kv[1].value, "x");
        assert_eq!((kv[1].mod_revision, kv[1].version), (5, 3));
    }

    #[test]
    fn parses_watch_lines() {
        let out = "{\"Header\":{},\"Events\":[{\"kv\":{\"key\":\"L2E=\",\"mod_revision\":7,\"version\":2,\"value\":\"eA==\"}},{\"type\":1,\"kv\":{\"key\":\"L2E=\",\"mod_revision\":8}}]}\n";
        let ev = parse_watch_output(out).unwrap();
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].kind, "PUT");
        assert_eq!(ev[0].kv.value, "x");
        assert_eq!(ev[1].kind, "DELETE");
    }
}
