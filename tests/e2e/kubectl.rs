//! `kubectl` subprocesses bound to the harness kubeconfig.
//!
//! The crate's kube client has no `ws` feature, so exec, logs and port-forward go
//! through kubectl. Blocking calls run on tokio's blocking pool, so a test can keep
//! other futures (a load run, a poll loop) going meanwhile.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};

#[derive(Debug, Clone)]
pub struct Kubectl {
    kubeconfig: PathBuf,
}

/// Result of a command whose failure is not necessarily an error.
#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub success: bool,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl From<Output> for CmdOutput {
    fn from(o: Output) -> Self {
        CmdOutput {
            success: o.status.success(),
            code: o.status.code(),
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        }
    }
}

impl Kubectl {
    pub fn new(kubeconfig: PathBuf) -> Self {
        Kubectl { kubeconfig }
    }

    /// A `kubectl` command with the kubeconfig set; add args and run it yourself.
    pub fn command<S: AsRef<str>>(&self, args: &[S]) -> Command {
        let mut c = Command::new("kubectl");
        c.arg("--kubeconfig").arg(&self.kubeconfig);
        c.args(args.iter().map(|a| a.as_ref()));
        c
    }

    /// Runs kubectl, returns its output whatever the exit status.
    pub async fn output<S: AsRef<str>>(
        &self,
        args: &[S],
        stdin: Option<&[u8]>,
    ) -> anyhow::Result<CmdOutput> {
        let mut cmd = self.command(args);
        let input = stdin.map(|b| b.to_vec());
        let shown = format!(
            "kubectl {}",
            args.iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .join(" ")
        );
        tokio::task::spawn_blocking(move || -> anyhow::Result<CmdOutput> {
            cmd.stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
            let mut child = cmd
                .spawn()
                .map_err(|e| anyhow::anyhow!("{shown}: spawn failed: {e}"))?;
            if let Some(input) = input {
                let mut sin = child.stdin.take().expect("piped stdin");
                sin.write_all(&input)?;
                drop(sin);
            }
            Ok(child.wait_with_output()?.into())
        })
        .await?
    }

    /// Runs kubectl and returns stdout; a non-zero exit is an error carrying stderr.
    pub async fn run<S: AsRef<str>>(&self, args: &[S]) -> anyhow::Result<String> {
        self.run_stdin(args, None).await
    }

    /// Like [`Kubectl::run`], feeding `stdin` to the process.
    pub async fn run_stdin<S: AsRef<str>>(
        &self,
        args: &[S],
        stdin: Option<&[u8]>,
    ) -> anyhow::Result<String> {
        let out = self.output(args, stdin).await?;
        if !out.success {
            anyhow::bail!(
                "kubectl {} failed ({:?}): {}",
                args.iter()
                    .map(|a| a.as_ref())
                    .collect::<Vec<_>>()
                    .join(" "),
                out.code,
                out.stderr.trim()
            );
        }
        Ok(out.stdout)
    }

    /// `kubectl exec -n ns pod [-c container] -- cmd...`.
    pub async fn exec<S: AsRef<str>>(
        &self,
        ns: &str,
        pod: &str,
        container: Option<&str>,
        cmd: &[S],
    ) -> anyhow::Result<CmdOutput> {
        self.output(&exec_args(ns, pod, container, false, cmd), None)
            .await
    }

    /// `kubectl exec -i` with `stdin` piped to the command.
    pub async fn exec_stdin<S: AsRef<str>>(
        &self,
        ns: &str,
        pod: &str,
        container: Option<&str>,
        cmd: &[S],
        stdin: &[u8],
    ) -> anyhow::Result<CmdOutput> {
        self.output(&exec_args(ns, pod, container, true, cmd), Some(stdin))
            .await
    }

    /// Starts a long-running `kubectl exec` (stdout and stderr piped, stdin closed). Collect it
    /// with [`collect`].
    pub fn spawn_exec<S: AsRef<str>>(
        &self,
        ns: &str,
        pod: &str,
        container: Option<&str>,
        cmd: &[S],
    ) -> anyhow::Result<Child> {
        let child = self
            .command(&exec_args(ns, pod, container, false, cmd))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(child)
    }

    /// `kubectl logs` of a pod (all containers when `container` is `None` and the pod has
    /// several), optionally only the last `since` (e.g. "2m").
    pub async fn logs(
        &self,
        ns: &str,
        pod: &str,
        container: Option<&str>,
        since: Option<&str>,
    ) -> anyhow::Result<String> {
        let mut args = vec!["logs".to_string(), "-n".into(), ns.into(), pod.into()];
        match container {
            Some(c) => args.extend(["-c".into(), c.into()]),
            None => args.push("--all-containers".into()),
        }
        if let Some(s) = since {
            args.push(format!("--since={s}"));
        }
        self.run(&args).await
    }
}

fn exec_args<S: AsRef<str>>(
    ns: &str,
    pod: &str,
    container: Option<&str>,
    stdin: bool,
    cmd: &[S],
) -> Vec<String> {
    let mut args = vec!["exec".to_string()];
    if stdin {
        args.push("-i".into());
    }
    args.extend(["-n".into(), ns.into(), pod.into()]);
    if let Some(c) = container {
        args.extend(["-c".into(), c.into()]);
    }
    args.push("--".into());
    args.extend(cmd.iter().map(|a| a.as_ref().to_string()));
    args
}

/// Waits for a child started with [`Kubectl::spawn_exec`] without blocking the runtime.
pub async fn collect(child: Child) -> anyhow::Result<CmdOutput> {
    Ok(
        tokio::task::spawn_blocking(move || child.wait_with_output())
            .await??
            .into(),
    )
}
