//! Requests through Pingap, made with curl from the in-cluster client pod (agnhost image),
//! so they follow the same path as real traffic: client pod -> Pingap Service -> Pingap ->
//! echo pod. The admin API is reached the same way (its Service is ClusterIP only).

use std::collections::BTreeMap;

use crate::kubectl::Kubectl;

#[derive(Debug, Clone)]
pub struct Pingap {
    pub kubectl: Kubectl,
    pub client_namespace: String,
    pub client_pod: String,
    /// In-cluster proxy URL, e.g. http://pingap-k8s-pingap.pingap-k8s.svc:80.
    pub url: String,
    pub admin_url: String,
    pub admin_user: String,
    pub admin_password: String,
    /// Namespace and name of the proxy Service (for port-forwarding, see `ws.rs`).
    pub namespace: String,
    pub service: String,
    /// Location routed to the WebSocket upstream.
    pub ws_path: String,
}

/// One HTTP response. `status` is 0 when no response arrived (connect error, timeout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
}

impl HttpResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Outcome of [`Pingap::sample`]: responses by status code, and by body for 200s
/// (the echo pod's hostname for `/hostname`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Sample {
    pub by_status: BTreeMap<u16, usize>,
    pub by_host: BTreeMap<String, usize>,
}

impl Sample {
    pub fn total(&self) -> usize {
        self.by_status.values().sum()
    }

    pub fn count(&self, status: u16) -> usize {
        self.by_status.get(&status).copied().unwrap_or(0)
    }

    /// Responses with status >= 500, plus requests without a response (status 0).
    pub fn failures(&self) -> usize {
        self.by_status
            .iter()
            .filter(|(s, _)| **s == 0 || **s >= 500)
            .map(|(_, n)| n)
            .sum()
    }
}

impl Pingap {
    async fn curl(&self, args: &[String], stdin: Option<&[u8]>) -> anyhow::Result<HttpResponse> {
        let mut cmd = vec![
            "curl".to_string(),
            "-s".into(),
            "-m".into(),
            "5".into(),
            "-w".into(),
            "\n%{http_code}".into(),
        ];
        cmd.extend_from_slice(args);
        let out = match stdin {
            Some(input) => {
                self.kubectl
                    .exec_stdin(&self.client_namespace, &self.client_pod, None, &cmd, input)
                    .await?
            }
            None => {
                self.kubectl
                    .exec(&self.client_namespace, &self.client_pod, None, &cmd)
                    .await?
            }
        };
        parse_curl(&out.stdout).ok_or_else(|| {
            anyhow::anyhow!(
                "unexpected curl output {:?} (stderr {:?})",
                out.stdout,
                out.stderr
            )
        })
    }

    /// GET `<pingap url><path>` with optional extra headers ("Name: value").
    pub async fn get(&self, path: &str, headers: &[&str]) -> anyhow::Result<HttpResponse> {
        let mut args = Vec::new();
        for h in headers {
            args.push("-H".to_string());
            args.push(h.to_string());
        }
        args.push(format!("{}{path}", self.url));
        self.curl(&args, None).await
    }

    /// The echo pod that served a GET /hostname, or an error with the status.
    pub async fn hostname(&self) -> anyhow::Result<String> {
        let r = self.get("/hostname", &[]).await?;
        if r.status != 200 {
            anyhow::bail!(
                "GET /hostname via Pingap: status {} body {:?}",
                r.status,
                r.body
            );
        }
        Ok(r.body.trim().to_string())
    }

    /// Sends `n` sequential GETs of `path` from the client pod in one exec (fast enough for a
    /// few hundred requests) and counts the outcomes. `timeout_secs` is per request.
    pub async fn sample(&self, path: &str, n: usize, timeout_secs: u32) -> anyhow::Result<Sample> {
        let script = format!(
            "i=0; while [ $i -lt {n} ]; do \
               code=$(curl -s -m {timeout_secs} -o /tmp/e2e-body -w '%{{http_code}}' '{url}{path}'); \
               if [ \"$code\" = 200 ]; then h=$(head -c 200 /tmp/e2e-body | tr -d '\\n '); else h=-; fi; \
               echo \"$code $h\"; i=$((i+1)); \
             done",
            url = self.url,
        );
        let out = self
            .kubectl
            .exec(
                &self.client_namespace,
                &self.client_pod,
                None,
                &["sh", "-c", &script],
            )
            .await?;
        if !out.success {
            anyhow::bail!("sample loop failed: {}", out.stderr.trim());
        }
        Ok(parse_sample(&out.stdout))
    }

    /// Calls the Pingap admin API (`path` like "/api/configs/upstream"). `body` is sent as
    /// JSON.
    ///
    /// Pingap 0.16.1 does not take basic auth: like its web UI (web/src/states/token.ts),
    /// every request carries `Authorization: <sha256 hex of "user:password:ts">:<ts>` with
    /// `ts` the current Unix time in seconds (src/plugin/admin.rs `auth_validate`). The
    /// token is computed in the client pod, so the password never leaves the cluster in a
    /// command line of the test host. Routes (src/plugin/admin.rs): `GET /api/configs`
    /// (what the UI loads), `GET /api/configs/<category>`, `POST
    /// /api/configs/<category>/<name>` (the whole entry as JSON, what the UI saves).
    pub async fn admin(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> anyhow::Result<HttpResponse> {
        let data = if body.is_some() {
            "-H 'Content-Type: application/json' --data-binary @-"
        } else {
            ""
        };
        let script = format!(
            "ts=$(date +%s); \
             tok=$(printf '%s' \"$1:$2:$ts\" | sha256sum | cut -d' ' -f1); \
             curl -s -m 5 -w '\\n%{{http_code}}' -X '{method}' -H \"Authorization: $tok:$ts\" \
               {data} '{url}{path}'",
            url = self.admin_url,
        );
        let cmd = [
            "sh",
            "-c",
            &script,
            "sh",
            &self.admin_user,
            &self.admin_password,
        ];
        let out = match body {
            Some(b) => {
                self.kubectl
                    .exec_stdin(
                        &self.client_namespace,
                        &self.client_pod,
                        None,
                        &cmd,
                        b.as_bytes(),
                    )
                    .await?
            }
            None => {
                self.kubectl
                    .exec(&self.client_namespace, &self.client_pod, None, &cmd)
                    .await?
            }
        };
        parse_curl(&out.stdout).ok_or_else(|| {
            anyhow::anyhow!(
                "unexpected admin curl output {:?} (stderr {:?})",
                out.stdout,
                out.stderr
            )
        })
    }

    /// The upstream entry `name` as the admin UI loads it (`GET /api/configs`, then
    /// `upstreams.<name>`): a JSON object.
    pub async fn admin_upstream(
        &self,
        name: &str,
    ) -> anyhow::Result<k8s_openapi::serde_json::Value> {
        let r = self.admin("GET", "/api/configs", None).await?;
        if r.status != 200 {
            anyhow::bail!("admin GET /api/configs: {} {:?}", r.status, r.body);
        }
        let v: k8s_openapi::serde_json::Value = k8s_openapi::serde_json::from_str(&r.body)?;
        v.get("upstreams")
            .and_then(|u| u.get(name))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("admin config has no upstreams.{name}: {}", r.body))
    }

    /// Saves upstream `name` the way the admin UI does: `POST /api/configs/upstream/<name>`
    /// with the whole entry (null fields dropped).
    pub async fn admin_save_upstream(
        &self,
        name: &str,
        entry: &k8s_openapi::serde_json::Value,
    ) -> anyhow::Result<()> {
        let mut entry = entry.clone();
        if let Some(m) = entry.as_object_mut() {
            m.retain(|_, v| !v.is_null());
        }
        let body = k8s_openapi::serde_json::to_string(&entry)?;
        let r = self
            .admin(
                "POST",
                &format!("/api/configs/upstream/{name}"),
                Some(&body),
            )
            .await?;
        if !r.is_success() {
            anyhow::bail!(
                "admin POST /api/configs/upstream/{name}: {} {:?}",
                r.status,
                r.body
            );
        }
        Ok(())
    }

    /// GET a URL from the client pod (e.g. an echo pod IP, or the controller's metrics
    /// Service), not through Pingap.
    pub async fn get_url(&self, url: &str) -> anyhow::Result<HttpResponse> {
        self.curl(&[url.to_string()], None).await
    }
}

/// curl was run with `-w '\n%{http_code}'`: the last line is the status.
fn parse_curl(stdout: &str) -> Option<HttpResponse> {
    let (body, code) = stdout.rsplit_once('\n')?;
    Some(HttpResponse {
        status: code.trim().parse().ok()?,
        body: body.to_string(),
    })
}

fn parse_sample(stdout: &str) -> Sample {
    let mut s = Sample::default();
    for line in stdout.lines() {
        let mut parts = line.splitn(2, ' ');
        let Some(code) = parts.next().and_then(|c| c.trim().parse::<u16>().ok()) else {
            continue;
        };
        *s.by_status.entry(code).or_default() += 1;
        if code == 200 {
            let host = parts.next().unwrap_or("").trim().to_string();
            *s.by_host.entry(host).or_default() += 1;
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_curl_output() {
        let r = parse_curl("echo-abc\n200").unwrap();
        assert_eq!((r.status, r.body.as_str()), (200, "echo-abc"));
        let r = parse_curl("\n000").unwrap();
        assert_eq!(r.status, 0);
        assert!(parse_curl("garbage").is_none());
    }

    #[test]
    fn parses_samples() {
        let s = parse_sample("200 echo-a\n200 echo-b\n200 echo-a\n503 -\n000 -\n");
        assert_eq!(s.total(), 5);
        assert_eq!(s.by_host["echo-a"], 2);
        assert_eq!(s.failures(), 2);
    }
}
