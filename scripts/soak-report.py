#!/usr/bin/env python3
"""Report for scripts/soak.sh: reads the raw data in <dir> and writes <dir>/report.md
and <dir>/summary.json. Exits 1 when a threshold is exceeded (see scripts/soak.sh).

Usage: scripts/soak-report.py <dir>
Env:   SOAK_MAX_ERROR_RATIO (default 0.001), SOAK_MAX_PLANNED_ERRORS (default 0),
       SOAK_MAX_RESTARTS (default 0), SOAK_MAX_MISSING_CHUNKS (fraction, default 0.05)
"""

import json
import math
import os
import re
import sys
from collections import defaultdict

# A failed request is attributed to an event when its load chunk overlaps
# [event start, event end + SLACK].
SLACK = 30.0


def read_tsv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [line.rstrip("\n").split("\t") for line in f if line.strip()]


def read_env(path):
    out = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                if "=" in line:
                    k, v = line.rstrip("\n").split("=", 1)
                    out[k] = v
    return out


def pct_from_buckets(buckets, total, q):
    """Percentile from [(start, end, count)] buckets, linear inside a bucket."""
    if total == 0:
        return None
    rank = q * total
    seen = 0
    for start, end, count in buckets:
        if count and seen + count >= rank:
            frac = (rank - seen) / count
            return start + (end - start) * frac
        seen += count
    return buckets[-1][1] if buckets else None


def fmt_ms(v):
    return "n/a" if v is None else f"{v * 1000:.1f} ms"


def fmt_s(v):
    return "n/a" if v is None else f"{v:.2f} s"


def fmt_mib(v):
    if v is None:
        return "n/a"
    return f"{v / 1048576:.1f} MiB" if v >= 1048576 else f"{v / 1024:.0f} KiB"


# --- load ---------------------------------------------------------------------------
def load_chunks(d):
    chunks = []
    for idx, t0, t1, rc in read_tsv(os.path.join(d, "load.tsv")):
        c = {"idx": int(idx), "t0": float(t0), "t1": float(t1), "rc": int(rc), "ok": False}
        path = os.path.join(d, "load", f"{int(idx):04d}.json")
        try:
            with open(path) as f:
                text = f.read()
            raw = json.loads(text[text.index("{"):])
            c["codes"] = {int(k): v for k, v in (raw.get("RetCodes") or {}).items()}
            h = raw.get("DurationHistogram") or {}
            c["hist"] = [(b["Start"], b["End"], b["Count"]) for b in h.get("Data") or []]
            c["max"] = h.get("Max")
            c["min"] = h.get("Min")
            c["p99"] = next((p["Value"] for p in h.get("Percentiles") or [] if p["Percentile"] == 99), None)
            c["ok"] = c["rc"] == 0
        except (OSError, ValueError, KeyError):
            c["codes"] = {}
            c["hist"] = []
        c["total"] = sum(c["codes"].values())
        c["errors"] = sum(n for code, n in c["codes"].items() if not 200 <= code < 300)
        chunks.append(c)
    return chunks


def merge_hist(chunks):
    """fortio buckets have fixed boundaries, except that the first one starts at the
    chunk's min and the last one ends at its max: key the first by its end and the
    others by their start."""
    acc = {}
    for c in chunks:
        for i, (s, e, n) in enumerate(c["hist"]):
            key = ("e", e) if i == 0 else ("s", s)
            s0, e0, n0 = acc.get(key, (s, e, 0))
            acc[key] = (min(s0, s), max(e0, e), n0 + n)
    buckets = sorted(acc.values(), key=lambda b: (b[1], b[0]))
    return buckets, sum(n for _, _, n in buckets)


# --- controller metrics -----------------------------------------------------------------
SERIES = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?\s+(\S+)$")


def parse_labels(s):
    return dict(re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"', s or ""))


def load_metrics(d):
    """[(ts, pod, node, {series_key: value})], series_key = (name, frozenset(labels))."""
    samples = []
    cur = None
    path = os.path.join(d, "metrics.txt")
    if not os.path.exists(path):
        return samples
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("# soak "):
                kv = dict(p.split("=", 1) for p in line[len("# soak "):].split())
                cur = (float(kv["ts"]), kv["pod"], kv.get("node", ""), {})
                samples.append(cur)
                continue
            m = SERIES.match(line)
            if not m or cur is None:
                continue
            labels = parse_labels(m.group(3))
            for k in ("otel_scope_name", "otel_scope_version", "otel_scope_schema_url", "job", "instance"):
                labels.pop(k, None)
            try:
                cur[3][(m.group(1), frozenset(labels.items()))] = float(m.group(4))
            except ValueError:
                pass
    return samples


def counter_deltas(samples):
    """Sum over controller pods of (last - baseline) per series; baseline is the first
    scrape for pods running at the start, 0 for pods started during the run."""
    if not samples:
        return {}
    t_first = samples[0][0]
    first, last = {}, {}
    for ts, pod, _, series in samples:
        if pod not in first:
            first[pod] = (ts, series)
        last[pod] = series
    out = defaultdict(float)
    for pod, series in last.items():
        ts0, base = first[pod]
        initial = ts0 <= t_first + 1
        for key, v in series.items():
            b = base.get(key, 0.0) if initial else 0.0
            out[key] += max(0.0, v - b)
    return out


def hist_quantiles(deltas, name):
    buckets = []
    for (n, labels), v in deltas.items():
        if n != name + "_bucket":
            continue
        le = dict(labels).get("le")
        buckets.append((math.inf if le in ("+Inf", "inf") else float(le), v))
    buckets.sort()
    total = buckets[-1][1] if buckets else 0
    count = sum(v for (n, _), v in deltas.items() if n == name + "_count")
    ssum = sum(v for (n, _), v in deltas.items() if n == name + "_sum")

    def q(p):
        if total <= 0:
            return None
        rank = p * total
        prev_le, prev_c = 0.0, 0.0
        for le, c in buckets:
            if c >= rank:
                if le == math.inf:
                    return prev_le
                if c == prev_c:
                    return le
                return prev_le + (le - prev_le) * (rank - prev_c) / (c - prev_c)
            prev_le, prev_c = le, c
        return prev_le

    return {
        "count": count,
        "mean": (ssum / count) if count else None,
        "p50": q(0.5),
        "p90": q(0.9),
        "p99": q(0.99),
        "buckets": [(le, c) for le, c in buckets],
    }


def sum_by(deltas, name, label):
    out = defaultdict(float)
    for (n, labels), v in deltas.items():
        if n == name:
            out[dict(labels).get(label, "")] += v
    return dict(out)


def leader_timeline(samples):
    by_ts = defaultdict(list)
    for ts, pod, _, series in samples:
        if series.get(("pingap_k8s_is_leader", frozenset())) == 1.0:
            by_ts[ts].append(pod)
        else:
            by_ts.setdefault(ts, [])
    changes, dual, none = 0, 0, 0
    prev = None
    for ts in sorted(by_ts):
        leaders = by_ts[ts]
        if len(leaders) > 1:
            dual += 1
        if not leaders:
            none += 1
            continue
        if prev is not None and leaders[0] != prev:
            changes += 1
        prev = leaders[0]
    return {"changes": changes, "samples": len(by_ts), "dual_leader_samples": dual, "no_leader_samples": none}


# --- main -------------------------------------------------------------------------------
def main():
    d = sys.argv[1]
    meta = read_env(os.path.join(d, "meta.env"))
    t_start = float(meta.get("start", "0"))
    max_ratio = float(os.environ.get("SOAK_MAX_ERROR_RATIO", "0.001"))
    max_planned = int(os.environ.get("SOAK_MAX_PLANNED_ERRORS", "0"))
    max_restarts = int(os.environ.get("SOAK_MAX_RESTARTS", "0"))
    max_missing = float(os.environ.get("SOAK_MAX_MISSING_CHUNKS", "0.05"))

    rel = lambda t: t - t_start  # noqa: E731

    def clock(t):
        s = int(rel(t))
        return f"T+{s // 60:02d}:{s % 60:02d}"

    # Events.
    events = []
    for row in read_tsv(os.path.join(d, "events.tsv")):
        t0, t1, kind, target, result = row[:5]
        events.append({"t0": float(t0), "t1": float(t1), "kind": kind, "target": target,
                       "result": result, "detail": row[5] if len(row) > 5 else ""})

    # Load.
    chunks = load_chunks(d)
    good = [c for c in chunks if c["ok"]]
    total = sum(c["total"] for c in chunks)
    errors = sum(c["errors"] for c in chunks)
    codes = defaultdict(int)
    for c in chunks:
        for code, n in c["codes"].items():
            codes[code] += n
    buckets, hist_total = merge_hist(chunks)
    lat = {
        "p50": pct_from_buckets(buckets, hist_total, 0.50),
        "p90": pct_from_buckets(buckets, hist_total, 0.90),
        "p99": pct_from_buckets(buckets, hist_total, 0.99),
        "p999": pct_from_buckets(buckets, hist_total, 0.999),
        "max": max((c["max"] for c in chunks if c.get("max") is not None), default=None),
    }
    missing = len(chunks) - len(good)
    load_seconds = sum(c["t1"] - c["t0"] for c in chunks)

    # Error attribution.
    for e in events:
        e["errors"] = 0
        e["requests"] = 0
        e["p99_max"] = None
        e["max_latency"] = None
    unattributed = 0
    crash_errors = 0
    error_chunks = []
    for c in chunks:
        over = [e for e in events if c["t0"] <= e["t1"] + SLACK and c["t1"] >= e["t0"]]
        for e in over:
            e["requests"] += c["total"]
            if c.get("p99") is not None:
                e["p99_max"] = max(e["p99_max"] or 0, c["p99"])
            if c.get("max") is not None:
                e["max_latency"] = max(e["max_latency"] or 0, c["max"])
        if not c["errors"]:
            continue
        crash = [e for e in over if e["kind"] == "node-crash"]
        owner = crash[0] if crash else (over[0] if over else None)
        if owner is None:
            unattributed += c["errors"]
        else:
            owner["errors"] += c["errors"]
        if crash:
            crash_errors += c["errors"]
        error_chunks.append((c, owner))
    planned_errors = errors - crash_errors

    # Controller metrics.
    samples = load_metrics(d)
    deltas = counter_deltas(samples)
    e2w = hist_quantiles(deltas, "pingap_k8s_event_to_write_seconds")
    writes = sum_by(deltas, "pingap_k8s_writes_total", "result")
    foreign = sum_by(deltas, "pingap_k8s_foreign_writes_total", "corrected")
    watch_errors = sum_by(deltas, "pingap_k8s_watch_errors_total", "error_type")
    leaders = leader_timeline(samples)

    # Pods and restarts.
    restarts = {}
    for row in read_tsv(os.path.join(d, "pods.tsv")):
        _, ns, pod, node, phase, ready, rs, term = row[:8]
        restarts[(ns, pod)] = max(restarts.get((ns, pod), 0), int(rs or 0))
    restarted = {k: v for k, v in restarts.items() if v > 0}

    # Memory and CPU.
    usage = defaultdict(list)  # (ns, pod, container) -> [(ts, ws, rss, cpu)]
    for row in read_tsv(os.path.join(d, "usage.tsv")):
        ts, node, ns, pod, cont, ws, rss, cpu = row[:8]
        usage[(ns, pod, cont)].append((float(ts), int(ws), int(rss), int(cpu)))

    def group(pred):
        return {k: v for k, v in usage.items() if pred(k)}

    ctrl_u = group(lambda k: k[0] == "pingap-k8s" and k[2] == "controller")
    pingap_u = group(lambda k: k[0] == "pingap-k8s" and "-pingap-" in k[1] and k[2] == "pingap")
    etcd_u = group(lambda k: k[0] == "pingap-k8s" and k[1].startswith("pingap-k8s-etcd-") and k[2] == "etcd")

    def usage_summary(u):
        if not u:
            return None
        rss = [s[2] for v in u.values() for s in v]
        ws = [s[1] for v in u.values() for s in v]
        cpu = [s[3] for v in u.values() for s in v]
        longest = max(u.items(), key=lambda kv: kv[1][-1][0] - kv[1][0][0])
        lv = longest[1]
        n = max(1, len(lv) // 10)
        head = sorted(s[2] for s in lv[:n])[len(lv[:n]) // 2]
        tail = sorted(s[2] for s in lv[-n:])[len(lv[-n:]) // 2]
        return {
            "rss_max": max(rss), "ws_max": max(ws), "cpu_max_millicores": max(cpu) / 1e6,
            "cpu_mean_millicores": sum(cpu) / len(cpu) / 1e6,
            "longest_pod": longest[0][1], "longest_minutes": (lv[-1][0] - lv[0][0]) / 60,
            "longest_rss_start": head, "longest_rss_end": tail,
        }

    ctrl_sum = usage_summary(ctrl_u)
    pingap_sum = usage_summary(pingap_u)
    etcd_sum = usage_summary(etcd_u)

    # Controller memory over time (10-minute bins, max RSS over the controller pods).
    bins = defaultdict(lambda: [0, 0, 0])
    for u, idx in ((ctrl_u, 0), (pingap_u, 1), (etcd_u, 2)):
        for v in u.values():
            for ts, ws, rss, cpu in v:
                b = int(rel(ts) // 600)
                bins[b][idx] = max(bins[b][idx], rss)

    # etcd.
    etcd_rows = read_tsv(os.path.join(d, "etcd.tsv"))
    db = [int(r[2]) for r in etcd_rows]
    in_use = [int(r[3]) for r in etcd_rows]
    revs = [int(r[7]) for r in etcd_rows]
    terms = [int(r[6]) for r in etcd_rows if r[6].isdigit()] or [0]
    etcd_leaders = []
    for r in etcd_rows:
        if not etcd_leaders or etcd_leaders[-1] != r[4]:
            etcd_leaders.append(r[4])

    # Failovers.
    failovers = []
    failovers_by_kind = defaultdict(list)
    converges = []
    for e in events:
        m = re.search(r"failover_s=([0-9.]+)", e["detail"])
        if m:
            failovers.append(float(m.group(1)))
            failovers_by_kind[e["kind"]].append(float(m.group(1)))
        m = re.search(r"converge_s=([0-9.]+)", e["detail"])
        if m:
            converges.append(float(m.group(1)))
    failed_events = [e for e in events if e["result"] != "ok"]

    # Verdict.
    ratio = errors / total if total else 1.0
    failures = []
    if total == 0:
        failures.append("no requests were made")
    if ratio > max_ratio:
        failures.append(f"error ratio {ratio:.5f} > SOAK_MAX_ERROR_RATIO {max_ratio}")
    if planned_errors > max_planned:
        failures.append(f"{planned_errors} failed requests outside node-crash windows > SOAK_MAX_PLANNED_ERRORS {max_planned}")
    if failed_events:
        failures.append(f"{len(failed_events)} event(s) failed (not settled, not converged, or no failover)")
    if meta.get("final_converge", "") in ("", "timeout"):
        failures.append("the echo addrs in etcd did not match the ready endpoints at the end")
    if sum(restarted.values()) > max_restarts:
        failures.append(f"{sum(restarted.values())} container restart(s) > SOAK_MAX_RESTARTS {max_restarts}")
    if chunks and missing / len(chunks) > max_missing:
        failures.append(f"{missing} of {len(chunks)} load chunks failed to run")

    # --- report -----------------------------------------------------------------------
    L = []
    w = L.append
    w("# Soak test report")
    w("")
    w(f"Result: **{'PASS' if not failures else 'FAIL'}**")
    for f in failures:
        w(f"- {f}")
    w("")
    w("## Setup")
    w("")
    w("| | |")
    w("| --- | --- |")
    for k in ("date", "controller_version", "controller_image", "pingap_image", "etcd_image",
              "kubernetes", "node_image", "workers", "host"):
        w(f"| {k} | {meta.get(k, '')} |")
    w(f"| load | {meta.get('qps')} qps, {meta.get('connections')} connections, "
      f"{int(meta.get('duration', 0)) // 60} min in {meta.get('chunk')} s chunks |")
    w(f"| events | {meta.get('events')} (interval {meta.get('interval')} s, node down {meta.get('node_down')} s) |")
    w("")
    w("## Requests")
    w("")
    w("| Metric | Value |")
    w("| --- | --- |")
    w(f"| requests | {total} ({total / load_seconds:.1f}/s over {load_seconds / 60:.1f} min) |" if load_seconds else f"| requests | {total} |")
    w(f"| failed (non-2xx or socket error) | {errors} ({ratio * 100:.4f}%) |")
    w(f"| failed outside node-crash windows | {planned_errors} |")
    w(f"| failed during node crashes | {crash_errors} |")
    w(f"| status codes | {', '.join(f'{k}: {v}' for k, v in sorted(codes.items()))} |")
    w(f"| latency p50 / p90 / p99 / p99.9 | {fmt_ms(lat['p50'])} / {fmt_ms(lat['p90'])} / {fmt_ms(lat['p99'])} / {fmt_ms(lat['p999'])} |")
    w(f"| latency max | {fmt_ms(lat['max'])} |")
    w(f"| load chunks | {len(chunks)} ({missing} failed to run) |")
    w("")
    w("## Events")
    w("")
    w("Failed requests and latency are those of the load chunks overlapping the event "
      f"(start to end + {int(SLACK)} s).")
    w("")
    w("| Start | Duration | Event | Target | Result | Failed req. | Chunk p99 max | Max latency | Detail |")
    w("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for e in events:
        detail = re.sub(r"pods: .*", "", e["detail"]).strip("; ")
        w(f"| {clock(e['t0'])} | {e['t1'] - e['t0']:.0f} s | {e['kind']} | {e['target']} | {e['result']} | "
          f"{e['errors']} | {fmt_ms(e['p99_max'])} | {fmt_ms(e['max_latency'])} | {detail} |")
    w("")
    by_kind = defaultdict(lambda: {"n": 0, "errors": 0, "converge": [], "dur": []})
    for e in events:
        k = by_kind[e["kind"]]
        k["n"] += 1
        k["errors"] += e["errors"]
        k["dur"].append(e["t1"] - e["t0"])
        m = re.search(r"converge_s=([0-9.]+)", e["detail"])
        if m:
            k["converge"].append(float(m.group(1)))
    w("| Event | Count | Failed requests | Max duration | Max converge after settle |")
    w("| --- | --- | --- | --- | --- |")
    for kind, k in by_kind.items():
        w(f"| {kind} | {k['n']} | {k['errors']} | {max(k['dur']):.0f} s | "
          f"{(str(max(k['converge'])) + ' s') if k['converge'] else 'n/a'} |")
    w(f"| (no event) | | {unattributed} | | |")
    w("")
    if error_chunks:
        w("Load chunks with failed requests:")
        w("")
        w("| Chunk | Failed | Codes | Event |")
        w("| --- | --- | --- | --- |")
        for c, owner in error_chunks:
            bad = {k: v for k, v in c["codes"].items() if not 200 <= k < 300}
            w(f"| {clock(c['t0'])} to {clock(c['t1'])} | {c['errors']} | {bad} | "
              f"{(owner['kind'] + ' ' + owner['target']) if owner else '-'} |")
        w("")
    w("## Controller")
    w("")
    w("| Metric | Value |")
    w("| --- | --- |")
    w(f"| event-to-write (n={int(e2w['count'])}) p50 / p90 / p99 | {fmt_s(e2w['p50'])} / {fmt_s(e2w['p90'])} / {fmt_s(e2w['p99'])} (mean {fmt_s(e2w['mean'])}) |")
    w(f"| writes by result | {', '.join(f'{k}: {int(v)}' for k, v in sorted(writes.items())) or 'none'} "
      "(error and conflict are retried) |")
    w(f"| foreign writes: changed addrs / kept them | {int(foreign.get('true', 0))} / {int(foreign.get('false', 0))} "
      "(a follower sees the leader's writes as foreign) |")
    w(f"| watch errors | {', '.join(f'{k}: {int(v)}' for k, v in sorted(watch_errors.items())) or 'none'} |")
    w(f"| leader changes (sampled) | {leaders['changes']} over {leaders['samples']} samples; "
      f"{leaders['dual_leader_samples']} samples with two leaders, {leaders['no_leader_samples']} with none |")
    for kind, v in sorted(failovers_by_kind.items()):
        what = "pod deleted (resign)" if kind == "leader-delete" else "node paused (lease expiry)"
        w(f"| leader failover, {what} | max {max(v):.1f} s, mean {sum(v) / len(v):.1f} s (n={len(v)}) |")
    if converges:
        w(f"| addrs converged after settle | max {max(converges):.1f} s (n={len(converges)}) |")
    w(f"| final addrs check | {meta.get('final_converge', 'n/a')} s |")
    w("")
    w("Counters are summed over the controller pods' lifetimes (scraped every few seconds; "
      "increments in the last interval before a pod is deleted are not seen).")
    w("")
    w("## Resources")
    w("")
    w("From the kubelet stats summary (RSS and working set per container; CPU as millicores).")
    w("")
    w("| Container | RSS max | Working set max | CPU mean / max | Longest-lived pod: RSS start -> end |")
    w("| --- | --- | --- | --- | --- |")
    for name, s in (("controller", ctrl_sum), ("pingap", pingap_sum), ("etcd", etcd_sum)):
        if s:
            w(f"| {name} | {fmt_mib(s['rss_max'])} | {fmt_mib(s['ws_max'])} | "
              f"{s['cpu_mean_millicores']:.1f} / {s['cpu_max_millicores']:.1f} m | "
              f"{s['longest_pod']} ({s['longest_minutes']:.0f} min): {fmt_mib(s['longest_rss_start'])} -> {fmt_mib(s['longest_rss_end'])} |")
    w("")
    w("Max RSS per 10 minutes:")
    w("")
    w("| Window | controller | pingap | etcd |")
    w("| --- | --- | --- | --- |")
    for b in sorted(bins):
        c, p, e = bins[b]
        w(f"| {b * 10}-{b * 10 + 10} min | {fmt_mib(c)} | {fmt_mib(p)} | {fmt_mib(e)} |")
    w("")
    w("## etcd")
    w("")
    w("| Metric | Value |")
    w("| --- | --- |")
    if db:
        w(f"| db size first / max / last | {fmt_mib(db[0])} / {fmt_mib(max(db))} / {fmt_mib(db[-1])} |")
        w(f"| db size in use last | {fmt_mib(in_use[-1])} |")
        w(f"| revision first -> last | {revs[0]} -> {revs[-1]} (+{revs[-1] - revs[0]}) |")
        w(f"| raft term first -> last | {terms[0]} -> {terms[-1]} |")
        w(f"| leader changes (sampled) | {max(0, len(etcd_leaders) - 1)} |")
    w("")
    w("## Restarts")
    w("")
    if restarted:
        for (ns, pod), n in sorted(restarted.items()):
            w(f"- {ns}/{pod}: {n}")
    else:
        w("No container restarted.")
    w("")

    with open(os.path.join(d, "report.md"), "w") as f:
        f.write("\n".join(L))
    summary = {
        "pass": not failures, "failures": failures, "requests": total, "errors": errors,
        "error_ratio": ratio, "errors_outside_node_crash": planned_errors,
        "errors_node_crash": crash_errors, "errors_unattributed": unattributed,
        "latency_seconds": lat, "event_to_write_seconds": {k: e2w[k] for k in ("count", "mean", "p50", "p90", "p99")},
        "writes": writes, "leader": leaders, "failover_seconds": dict(failovers_by_kind),
        "controller": ctrl_sum, "pingap": pingap_sum, "etcd": etcd_sum,
        "etcd_db_bytes": {"first": db[0], "max": max(db), "last": db[-1]} if db else None,
        "restarts": {f"{ns}/{p}": n for (ns, p), n in restarted.items()},
        "events": [{k: e[k] for k in ("kind", "target", "result", "errors", "detail")} | {"start_s": rel(e["t0"]), "duration_s": e["t1"] - e["t0"]} for e in events],
    }
    with open(os.path.join(d, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"soak: {'PASS' if not failures else 'FAIL'}: {total} requests, {errors} failed "
          f"({planned_errors} outside node crashes), p99 {fmt_ms(lat['p99'])}; report {os.path.join(d, 'report.md')}")
    for f in failures:
        print(f"soak: {f}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
