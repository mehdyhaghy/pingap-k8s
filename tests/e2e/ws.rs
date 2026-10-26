//! WebSocket helper for e2e test 6.
//!
//! Path: test process -> `kubectl port-forward` to Pingap's proxy Service (127.0.0.1 only)
//! -> Pingap -> location `/ws` -> upstream "echo-ws" -> the echo pod's jmalloc/echo-server
//! container. On connect that server sends "Request served by <pod name>", then echoes every
//! message, so a test knows which pod holds the connection and can check it is still alive.
//!
//! Only Pingap's proxy listener is port-forwarded (to localhost); etcd, the admin API and
//! metrics are never forwarded. The client is a small RFC 6455 implementation on a blocking
//! `TcpStream` (no WebSocket crate in the dependency tree): calls block for at most the
//! timeout they are given.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Stdio};
use std::time::Duration;

use crate::kubectl::Kubectl;

/// A `kubectl port-forward` to a Service port on a random local port. Killed on drop.
pub struct PortForward {
    child: Child,
    pub local_port: u16,
}

impl PortForward {
    /// `target` is e.g. "svc/pingap-k8s-pingap".
    pub fn start(
        kubectl: &Kubectl,
        ns: &str,
        target: &str,
        remote_port: u16,
    ) -> anyhow::Result<PortForward> {
        let mut child = kubectl
            .command(&[
                "port-forward",
                "-n",
                ns,
                "--address",
                "127.0.0.1",
                target,
                &format!(":{remote_port}"),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let stdout = child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();
        // "Forwarding from 127.0.0.1:41234 -> 8080"
        let first = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("port-forward exited without output"))??;
        let port = first
            .split("127.0.0.1:")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| anyhow::anyhow!("unexpected port-forward output {first:?}"))?;
        // Keep draining stdout ("Handling connection for ..."), or kubectl dies of SIGPIPE.
        std::thread::spawn(move || for _ in lines {});
        Ok(PortForward {
            child,
            local_port: port,
        })
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.local_port)
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsMessage {
    Text(String),
    Binary(Vec<u8>),
    /// Close frame (status code if any), or the TCP stream ended.
    Closed(Option<u16>),
}

pub struct WsClient {
    stream: TcpStream,
    /// Pod name from the server's greeting ("Request served by <pod>").
    pub served_by: String,
}

impl WsClient {
    /// Connects to `addr` (host:port), upgrades `path`, and reads the greeting.
    pub fn connect(addr: &str, path: &str, timeout: Duration) -> anyhow::Result<WsClient> {
        let stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        stream.set_nodelay(true)?;
        let mut c = WsClient {
            stream,
            served_by: String::new(),
        };
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: pingap\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        c.stream.write_all(req.as_bytes())?;
        let head = c.read_http_head()?;
        let status = head.lines().next().unwrap_or("");
        if !status.contains(" 101 ") {
            anyhow::bail!("WebSocket upgrade refused: {head:?}");
        }
        match c.recv(timeout)? {
            WsMessage::Text(t) if t.starts_with("Request served by ") => {
                c.served_by = t["Request served by ".len()..].trim().to_string();
            }
            other => anyhow::bail!("unexpected first WebSocket message {other:?}"),
        }
        Ok(c)
    }

    fn read_http_head(&mut self) -> anyhow::Result<String> {
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if self.stream.read(&mut byte)? == 0 {
                anyhow::bail!(
                    "connection closed during upgrade: {:?}",
                    String::from_utf8_lossy(&head)
                );
            }
            head.push(byte[0]);
            if head.len() > 16 * 1024 {
                anyhow::bail!("HTTP response head too large");
            }
        }
        Ok(String::from_utf8_lossy(&head).into_owned())
    }

    fn send_frame(&mut self, opcode: u8, payload: &[u8]) -> anyhow::Result<()> {
        let mut f = vec![0x80 | opcode];
        let len = payload.len();
        if len < 126 {
            f.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            f.push(0x80 | 126);
            f.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            f.push(0x80 | 127);
            f.extend_from_slice(&(len as u64).to_be_bytes());
        }
        // Client frames must be masked; the key need not be secret in a test.
        let key = [0x12u8, 0x34, 0x56, 0x78];
        f.extend_from_slice(&key);
        f.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        self.stream.write_all(&f)?;
        Ok(())
    }

    pub fn send_text(&mut self, text: &str) -> anyhow::Result<()> {
        self.send_frame(0x1, text.as_bytes())
    }

    /// Next data or close message, answering pings on the way. A read timeout is an error
    /// (`is_timeout` tells it apart from a broken connection).
    pub fn recv(&mut self, timeout: Duration) -> anyhow::Result<WsMessage> {
        self.stream.set_read_timeout(Some(timeout))?;
        loop {
            let mut h = [0u8; 2];
            match self.stream.read_exact(&mut h) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(WsMessage::Closed(None));
                }
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                    return Ok(WsMessage::Closed(None));
                }
                Err(e) => return Err(e.into()),
            }
            let opcode = h[0] & 0x0f;
            let masked = h[1] & 0x80 != 0;
            let mut len = (h[1] & 0x7f) as u64;
            if len == 126 {
                let mut b = [0u8; 2];
                self.stream.read_exact(&mut b)?;
                len = u16::from_be_bytes(b) as u64;
            } else if len == 127 {
                let mut b = [0u8; 8];
                self.stream.read_exact(&mut b)?;
                len = u64::from_be_bytes(b);
            }
            let mut key = [0u8; 4];
            if masked {
                self.stream.read_exact(&mut key)?;
            }
            let mut payload = vec![0u8; len as usize];
            self.stream.read_exact(&mut payload)?;
            if masked {
                for (i, b) in payload.iter_mut().enumerate() {
                    *b ^= key[i % 4];
                }
            }
            match opcode {
                0x1 => return Ok(WsMessage::Text(String::from_utf8(payload)?)),
                0x2 => return Ok(WsMessage::Binary(payload)),
                0x8 => {
                    let code =
                        (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]));
                    return Ok(WsMessage::Closed(code));
                }
                0x9 => self.send_frame(0xA, &payload)?,
                _ => {} // pong, continuation: ignore
            }
        }
    }

    /// Sends `text` and expects it echoed back within `timeout`: the connection is alive end
    /// to end (test process -> Pingap -> pod).
    pub fn echo(&mut self, text: &str, timeout: Duration) -> anyhow::Result<()> {
        self.send_text(text)?;
        match self.recv(timeout)? {
            WsMessage::Text(t) if t == text => Ok(()),
            other => anyhow::bail!("expected echo of {text:?}, got {other:?}"),
        }
    }

    /// Sends a close frame (best effort).
    pub fn close(mut self) {
        let _ = self.send_frame(0x8, &1000u16.to_be_bytes());
    }
}

/// True when `err` is a read timeout from [`WsClient::recv`] (connection still open).
pub fn is_timeout(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>().is_some_and(|e| {
        matches!(
            e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        )
    })
}

/// One echo round trip of a [`WsProbe`]: when it finished and how.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub at: std::time::SystemTime,
    pub result: Result<(), String>,
}

/// A WebSocket connection kept busy on its own thread: an echo round trip every `every`
/// until one fails (connection closed or no echo within 3 s) or the probe is stopped.
/// Every round trip is recorded with its time.
pub struct WsProbe {
    pub served_by: String,
    log: std::sync::Arc<std::sync::Mutex<Vec<ProbeResult>>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WsProbe {
    pub fn start(addr: &str, path: &str, every: Duration) -> anyhow::Result<WsProbe> {
        use std::sync::atomic::Ordering;
        let mut client = WsClient::connect(addr, path, Duration::from_secs(5))?;
        let served_by = client.served_by.clone();
        let log: std::sync::Arc<std::sync::Mutex<Vec<ProbeResult>>> = Default::default();
        let stop: std::sync::Arc<std::sync::atomic::AtomicBool> = Default::default();
        let handle = {
            let log = log.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut n = 0u64;
                loop {
                    if stop.load(Ordering::Relaxed) {
                        client.close();
                        return;
                    }
                    n += 1;
                    let r = client
                        .echo(&format!("ping {n}"), Duration::from_secs(3))
                        .map_err(|e| format!("{e:#}"));
                    let failed = r.is_err();
                    log.lock().unwrap().push(ProbeResult {
                        at: std::time::SystemTime::now(),
                        result: r,
                    });
                    if failed {
                        return;
                    }
                    std::thread::sleep(every);
                }
            })
        };
        Ok(WsProbe {
            served_by,
            log,
            stop,
            handle: Some(handle),
        })
    }

    pub fn results(&self) -> Vec<ProbeResult> {
        self.log.lock().unwrap().clone()
    }

    /// The first failed round trip, if any.
    pub fn failure(&self) -> Option<ProbeResult> {
        self.results().into_iter().find(|r| r.result.is_err())
    }

    /// Time of the last successful round trip.
    pub fn last_ok(&self) -> Option<std::time::SystemTime> {
        self.results()
            .into_iter()
            .filter(|r| r.result.is_ok())
            .map(|r| r.at)
            .max()
    }
}

impl Drop for WsProbe {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
