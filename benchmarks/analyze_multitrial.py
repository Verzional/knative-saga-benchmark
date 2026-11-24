#!/usr/bin/env python3
"""
Statistical Aggregator for Multi-Trial (N=10) Saga Benchmarks.
Computes Mean, Sample SD, 95% Confidence Interval (Student-t), Median, IQR,
two-sided Welch's t-test / Mann-Whitney U p-values, namespace-equalized
resource footprints, and FaaS Cloud Unit Economics.
"""
import csv
import glob
import json
import math
import os
import statistics

T_CRIT_95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 14: 2.145, 19: 2.093, 29: 2.045,
}


def get_t_crit(n):
    return T_CRIT_95.get(max(1, n - 1), 1.96)


def normal_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def welch_t_pvalue(a, b):
    if len(a) < 2 or len(b) < 2:
        return None
    try:
        from scipy import stats
        p = float(stats.ttest_ind(a, b, equal_var=False).pvalue)
        return 1.0 if math.isnan(p) else p
    except Exception:
        ma, mb = statistics.mean(a), statistics.mean(b)
        va, vb = statistics.variance(a), statistics.variance(b)
        se = math.sqrt(va / len(a) + vb / len(b))
        if se == 0:
            return 1.0 if ma == mb else 0.0
        return 2.0 * (1.0 - normal_cdf(abs(ma - mb) / se))


def mann_whitney_pvalue(a, b):
    if len(a) < 2 or len(b) < 2:
        return None
    try:
        from scipy import stats
        return float(stats.mannwhitneyu(a, b, alternative='two-sided').pvalue)
    except Exception:
        n1, n2 = len(a), len(b)
        u1 = sum(1.0 if x > y else (0.5 if x == y else 0.0) for x in a for y in b)
        mu = n1 * n2 / 2.0
        sigma = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12.0)
        if sigma == 0:
            return 1.0
        return 2.0 * (1.0 - normal_cdf(abs(u1 - mu) / sigma))


def summarize(vals):
    clean = [v for v in vals if v is not None]
    n = len(clean)
    if n == 0:
        return None
    m = statistics.mean(clean)
    sd = statistics.stdev(clean) if n > 1 else 0.0
    ci = get_t_crit(n) * (sd / math.sqrt(n))
    s = sorted(clean)
    med = statistics.median(s)
    q25 = statistics.median(s[: n // 2]) if n >= 2 else s[0]
    q75 = statistics.median(s[(n + 1) // 2 :]) if n >= 2 else s[-1]
    return {
        "n": n,
        "mean": round(m, 3),
        "sd": round(sd, 4),
        "ci95_half": round(ci, 3),
        "ci95_low": round(m - ci, 3),
        "ci95_high": round(m + ci, 3),
        "median": round(med, 3),
        "q25": round(q25, 3),
        "q75": round(q75, 3),
        "iqr": round(q75 - q25, 3),
        "min": round(min(clean), 3),
        "max": round(max(clean), 3),
    }


def extract_trial_metrics(filepath, scen):
    with open(filepath) as f:
        d = json.load(f)
    agg = d.get("aggregate", {})
    c = agg.get("counters", {})
    s = agg.get("summaries", {})

    total = c.get("http.requests", 0)
    ok_200 = c.get("http.codes.200", 0)
    ok_202 = c.get("http.codes.202", 0)
    ok_2xx = ok_200 + ok_202
    ok_422 = c.get("http.codes.422", 0)
    err_503 = c.get("http.codes.503", 0)
    timeouts = c.get("errors.ERR_SOCKET_TIMEOUT", 0)
    resets = c.get("errors.ECONNRESET", 0) + c.get("errors.EPIPE", 0)

    # Strictly use 4xx response times for rollback and 2xx response times otherwise
    # (never pool HTTP 503 failure latencies from crashed trials into 2xx latency!)
    if scen == "rollback":
        rt = (s.get("http.response_time.422") or s.get("http.response_time.4xx")) if ok_422 > 0 else None
    else:
        rt = s.get("http.response_time.2xx") if ok_2xx > 0 else None

    if rt:
        mean_ms = rt.get("mean")
        p50 = rt.get("p50", rt.get("median"))
        p95 = rt.get("p95")
        p99 = rt.get("p99")
        max_ms = rt.get("max")
        min_ms = rt.get("min")
        idisp = (p99 / p50) if p50 else None
        delta_cold = (max_ms - min_ms) if (max_ms is not None and min_ms is not None) else None
    else:
        mean_ms = p50 = p95 = p99 = max_ms = min_ms = idisp = delta_cold = None

    return {
        "total_reqs": total,
        "http_200": ok_200,
        "http_202": ok_202,
        "total_2xx": ok_2xx,
        "http_422": ok_422,
        "http_503": err_503,
        "success_pct": (ok_200 / total * 100.0) if total else 0.0,
        "async_202_pct": (ok_202 / total * 100.0) if total else 0.0,
        "total_2xx_pct": (ok_2xx / total * 100.0) if total else 0.0,
        "rollback_pct": (ok_422 / total * 100.0) if total else 0.0,
        "http_503_pct": (err_503 / total * 100.0) if total else 0.0,
        "timeout_pct": (timeouts / total * 100.0) if total else 0.0,
        "reset_pct": (resets / total * 100.0) if total else 0.0,
        "mean_ms": mean_ms,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "max_ms": max_ms,
        "min_ms": min_ms,
        "idisp": idisp,
        "delta_cold_ms": delta_cold,
    }


def load_resources(base):
    res = {}
    for arch in ("temporal", "kafka"):
        cp = f"{base}/{arch}/resource_snapshots.csv"
        if not os.path.exists(cp):
            continue
        by_phase = {}
        with open(cp) as f:
            for row in csv.DictReader(f):
                ph = row["phase"]
                by_phase.setdefault(ph, []).append({
                    "ctrl_cpu": float(row[f"{arch}_cpu"]),
                    "ctrl_ram_mb": float(row[f"{arch}_ram_mb"]),
                    "default_cpu": float(row["default_cpu"]),
                    "default_ram_mb": float(row["default_ram_mb"]),
                    "knative_cpu": float(row["knative_cpu"]),
                    "knative_ram_mb": float(row["knative_ram_mb"]),
                    "total_cpu": float(row[f"{arch}_cpu"]) + float(row["default_cpu"]),
                    "total_ram_mb": float(row[f"{arch}_ram_mb"]) + float(row["default_ram_mb"]),
                    "full_stack_cpu": float(row[f"{arch}_cpu"]) + float(row["default_cpu"]) + float(row["knative_cpu"]),
                    "full_stack_ram_mb": float(row[f"{arch}_ram_mb"]) + float(row["default_ram_mb"]) + float(row["knative_ram_mb"]),
                })
        res[arch] = {
            ph: {k: summarize([r[k] for r in rows]) for k in rows[0]}
            for ph, rows in by_phase.items()
        }
    return res


def main():
    base = "results/trials"
    scenarios = ["normal", "coldstart", "rollback", "burst"]
    report = {}

    for scen in scenarios:
        t_files = sorted(glob.glob(f"{base}/temporal/{scen}_trial_*.json"))
        k_files = sorted(glob.glob(f"{base}/kafka/{scen}_trial_*.json"))
        t_rows = [extract_trial_metrics(p, scen) for p in t_files]
        k_rows = [extract_trial_metrics(p, scen) for p in k_files]

        scen_res = {"temporal": {}, "kafka": {}, "p_values": {}}
        if t_rows:
            for key in t_rows[0]:
                scen_res["temporal"][key] = summarize([r[key] for r in t_rows])
        if k_rows:
            for key in k_rows[0]:
                scen_res["kafka"][key] = summarize([r[key] for r in k_rows])
        if t_rows and k_rows:
            for key in t_rows[0]:
                a = [r[key] for r in t_rows if r[key] is not None]
                b = [r[key] for r in k_rows if r[key] is not None]
                scen_res["p_values"][key] = {
                    "welch_t_p": welch_t_pvalue(a, b),
                    "mann_whitney_p": mann_whitney_pvalue(a, b),
                }
        report[scen] = scen_res

    report["resources"] = load_resources(base)

    # Healthy Temporal subset (excluding Trial 7 control-plane outage: 0 HTTP 200, 99.79% HTTP 503, 0.025 CPU)
    t_norm_all = [(i, extract_trial_metrics(f"{base}/temporal/normal_trial_{i}.json", "normal")) for i in range(1, 11) if os.path.exists(f"{base}/temporal/normal_trial_{i}.json")]
    t_norm_h = [m for i, m in t_norm_all if m["http_200"] > 0]
    k_norm_all = [extract_trial_metrics(f"{base}/kafka/normal_trial_{i}.json", "normal") for i in range(1, 11) if os.path.exists(f"{base}/kafka/normal_trial_{i}.json")]
    if t_norm_h and k_norm_all:
        h_summary = {k: summarize([r[k] for r in t_norm_h]) for k in t_norm_h[0]}
        h_summary["p_values"] = {
            k: {
                "welch_t_p": welch_t_pvalue([r[k] for r in t_norm_h if r[k] is not None], [r[k] for r in k_norm_all if r[k] is not None]),
                "mann_whitney_p": mann_whitney_pvalue([r[k] for r in t_norm_h if r[k] is not None], [r[k] for r in k_norm_all if r[k] is not None]),
            }
            for k in t_norm_h[0]
        }
        report["normal_healthy_temporal"] = h_summary

    # Clean Kafka Cold Start subset (idle default_ram_mb == 0.0 and no 202 fallback)
    k_idle_ram = {}
    k_csv = f"{base}/kafka/resource_snapshots.csv"
    if os.path.exists(k_csv):
        with open(k_csv) as f:
            for row in csv.DictReader(f):
                if row["phase"] == "idle":
                    k_idle_ram[int(row["trial"])] = float(row["default_ram_mb"])
    k_cs_clean = [
        extract_trial_metrics(f"{base}/kafka/coldstart_trial_{i}.json", "coldstart")
        for i in range(1, 11)
        if os.path.exists(f"{base}/kafka/coldstart_trial_{i}.json") and k_idle_ram.get(i, 1.0) == 0.0
    ]
    if k_cs_clean:
        report["coldstart_clean_kafka"] = {k: summarize([r[k] for r in k_cs_clean]) for k in k_cs_clean[0]}

    # Healthy N=9 Temporal normal_peak resource stats (excluding crashed Trial 7)
    t_csv = f"{base}/temporal/resource_snapshots.csv"
    if os.path.exists(t_csv):
        h_rows = []
        with open(t_csv) as f:
            for row in csv.DictReader(f):
                if row["phase"] == "normal_peak" and int(row["trial"]) != 7:
                    h_rows.append({
                        "ctrl_cpu": float(row["temporal_cpu"]),
                        "ctrl_ram_mb": float(row["temporal_ram_mb"]),
                        "default_cpu": float(row["default_cpu"]),
                        "default_ram_mb": float(row["default_ram_mb"]),
                        "knative_cpu": float(row["knative_cpu"]),
                        "knative_ram_mb": float(row["knative_ram_mb"]),
                        "total_cpu": float(row["temporal_cpu"]) + float(row["default_cpu"]),
                        "total_ram_mb": float(row["temporal_ram_mb"]) + float(row["default_ram_mb"]),
                        "full_stack_cpu": float(row["temporal_cpu"]) + float(row["default_cpu"]) + float(row["knative_cpu"]),
                        "full_stack_ram_mb": float(row["temporal_ram_mb"]) + float(row["default_ram_mb"]) + float(row["knative_ram_mb"]),
                    })
        if h_rows:
            report.setdefault("resources", {}).setdefault("temporal", {})["normal_peak_healthy"] = {
                k: summarize([r[k] for r in h_rows]) for k in h_rows[0]
            }

    out_json = f"{base}/multitrial_summary.json"
    with open(out_json, "w") as f:
        json.dump(report, f, indent=2)
    print(f"[✓] Saved full statistical summary to {out_json}")
    for scen in scenarios:
        tn = (report[scen]["temporal"].get("mean_ms") or {}).get("n", 0) if report[scen]["temporal"] else 0
        kn = (report[scen]["kafka"].get("mean_ms") or {}).get("n", 0) if report[scen]["kafka"] else 0
        print(f"  - {scen:9s}: Temporal N={tn}, Kafka N={kn}")


if __name__ == "__main__":
    main()
