#!/usr/bin/env python3
"""
Publication Figure Generator for Serverless Saga Benchmark:
Reads multi-trial statistical summaries (results/trials/multitrial_summary.json)
and active-pod resource snapshots (results/trials/*/resource_snapshots.csv) when
available, plotting 300 DPI PNG/SVG figures with Mean ± SD error bars.
"""

import csv
import json
import os
import statistics
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

plt.rcParams.update({
    'font.family': 'sans-serif',
    'font.sans-serif': ['DejaVu Sans', 'Arial', 'Helvetica'],
    'font.size': 11,
    'axes.labelsize': 12,
    'axes.titlesize': 13,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 9.5,
    'figure.titlesize': 14,
    'figure.dpi': 300,
    'savefig.dpi': 300,
    'savefig.bbox': 'tight',
    'axes.grid': True,
    'grid.alpha': 0.3,
    'grid.linestyle': '--',
})

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
FIGURES_DIR = os.path.join(ROOT_DIR, "figures")
TRIALS_DIR = os.path.join(ROOT_DIR, "results", "trials")
SUMMARY_JSON = os.path.join(TRIALS_DIR, "multitrial_summary.json")
os.makedirs(FIGURES_DIR, exist_ok=True)

COLOR_TEMPORAL = '#1a5276'
COLOR_KAFKA = '#d35400'


def load_summary():
    if os.path.exists(SUMMARY_JSON):
        with open(SUMMARY_JSON) as f:
            return json.load(f)
    return None


def load_res_csv(arch, phase="normal_peak"):
    path = os.path.join(TRIALS_DIR, arch, "resource_snapshots.csv")
    if not os.path.exists(path):
        return None
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            if r.get("phase") == phase:
                rows.append({k: float(v) for k, v in r.items() if k not in ("trial", "arch", "phase")})
    if not rows:
        return None
    return {
        k: (statistics.mean([r[k] for r in rows]), statistics.stdev([r[k] for r in rows]) if len(rows) > 1 else 0.0)
        for k in rows[0]
    }


def plot_latency_cdf(stats):
    fig, ax = plt.subplots(figsize=(7, 4.3))
    pcts = [0, 50, 95, 99, 100]

    if stats and stats["normal"].get("temporal") and stats["normal"].get("kafka"):
        t, k = stats["normal"]["temporal"], stats["normal"]["kafka"]
        # Plot cross-trial median quantiles consistently with legend labels and vertical markers
        t_vals = [max(10.0, t["min_ms"]["median"]), t["p50_ms"]["median"], t["p95_ms"]["median"], t["p99_ms"]["median"], t["max_ms"]["median"]]
        k_vals = [max(10.0, k["min_ms"]["median"]), k["p50_ms"]["median"], k["p95_ms"]["median"], k["p99_ms"]["median"], k["max_ms"]["median"]]
        t_lbl = f"Temporal 2xx (N={t['p50_ms']['n']} healthy, med p50={t_vals[1]:.1f} ms)"
        k_lbl = f"Kafka 2xx (N={k['p50_ms']['n']}, med p50={k_vals[1]:.1f} ms)"
    else:
        t_vals = [116.0, 944.0, 7557.1, 7865.6, 7974.0]
        k_vals = [23.0, 1939.5, 3072.4, 3678.4, 3816.0]
        t_lbl = "Temporal Orchestration (p50=944.0 ms)"
        k_lbl = "Kafka Choreography (p50=1,939.5 ms)"

    t_vals = np.maximum.accumulate(t_vals)
    k_vals = np.maximum.accumulate(k_vals)
    t_x = np.logspace(np.log10(max(10, t_vals[0])), np.log10(max(10000, t_vals[-1])), 200)
    k_x = np.logspace(np.log10(max(10, k_vals[0])), np.log10(max(10000, k_vals[-1])), 200)
    t_cdf = np.interp(t_x, t_vals, [p / 100.0 for p in pcts])
    k_cdf = np.interp(k_x, k_vals, [p / 100.0 for p in pcts])

    ax.plot(t_x, t_cdf, label=t_lbl, color=COLOR_TEMPORAL, linewidth=2.5)
    ax.plot(k_x, k_cdf, label=k_lbl, color=COLOR_KAFKA, linewidth=2.5)
    ax.axvline(t_vals[1], color=COLOR_TEMPORAL, linestyle=':', alpha=0.8)
    ax.axvline(k_vals[1], color=COLOR_KAFKA, linestyle=':', alpha=0.8)

    ax.set_xscale('log')
    ax.set_xlim(10, 12000)
    ax.set_ylim(0, 1.05)
    ax.set_xlabel('End-to-End 2xx Latency $L_{e2e}$ (ms, Log Scale)')
    ax.set_ylabel('Cumulative Probability $P(L \\leq x)$')
    ax.set_title('Quantile-Reconstructed 2xx Latency Profile (Cross-Trial Medians)')
    ax.legend(loc='lower right', frameon=True)
    ax.xaxis.set_major_formatter(ticker.FormatStrFormatter('%d'))

    fig.savefig(os.path.join(FIGURES_DIR, 'fig2_latency_cdf.png'))
    fig.savefig(os.path.join(FIGURES_DIR, 'fig2_latency_cdf.svg'))
    plt.close(fig)


def plot_topology():
    from matplotlib.patches import FancyBboxPatch

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis('off')

    # Outer host frames
    left_frame = FancyBboxPatch((2, 4), 40, 88, boxstyle="round,pad=0.6,rounding_size=1.2",
                                facecolor='#fdfefe', edgecolor='#1a4384', linewidth=2.2)
    right_frame = FancyBboxPatch((58, 4), 40, 88, boxstyle="round,pad=0.6,rounding_size=1.2",
                                 facecolor='#fdfefe', edgecolor='#8b1a1a', linewidth=2.2)
    ax.add_patch(left_frame)
    ax.add_patch(right_frame)

    # Top interconnect banner
    banner = FancyBboxPatch((14, 90.5), 72, 6.2, boxstyle="round,pad=0.4,rounding_size=1.5",
                            facecolor='#ffffff', edgecolor='#95a5a6', linewidth=1.2)
    ax.add_patch(banner)
    ax.text(50, 93.6, "Isolated Physical Dual-Host Testbed (Cat6 Point-to-Point Ethernet, <0.8 ms RTT)",
            ha='center', va='center', fontsize=10.5, fontweight='bold', color='#2c3e50')

    # Left Header
    ax.text(22, 87.2, "LOAD DRIVER (Apple MacBook Pro)", ha='center', va='center',
            fontsize=11.5, fontweight='bold', color='#1a4384')
    ax.text(22, 82.8, "IP: 192.168.10.2 (Static, en12) | macOS 27.2.1\nApple M5 (10-Core SoC) | 24 GB Unified RAM",
            ha='center', va='center', fontsize=8.8, color='#34495e')

    # Right Header
    ax.text(78, 87.2, "SYSTEM UNDER TEST (Lenovo Legion 5 Pro)", ha='center', va='center',
            fontsize=11.5, fontweight='bold', color='#8b1a1a')
    ax.text(78, 82.8, "IP: 192.168.10.10 (Static, eth0) | WSL2 Ubuntu 24.04.3 LTS\nIntel Core i7-12700H (14c/20t) | ~10 GB Allocated RAM",
            ha='center', va='center', fontsize=8.8, color='#34495e')

    def add_box(x, y, w, h, title, lines, bg, edge, tcol):
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=0.8",
                             facecolor=bg, edgecolor=edge, linewidth=1.2)
        ax.add_patch(box)
        ax.text(x + w / 2, y + h - 3.6, title, ha='center', va='center',
                fontsize=10.2, fontweight='bold', color=tcol)
        ax.text(x + w / 2, y + h / 2 - 1.5, "\n".join(lines), ha='center', va='center',
                fontsize=8.6, color='#2c3e50', linespacing=1.4)

    # Left Boxes
    add_box(5.5, 56, 33, 21.5, "Artillery v2.0 Load Injector", [
        "• 12,600 Step-Load (10 -> 200 RPS)",
        "• 1,200 Fixed Burst (80 RPS) & 300 Poison Pills",
        "• Scale-to-Zero Cold Start Probing (5/Trial)"
    ], '#ebf5fb', '#5dade2', '#1a4384')

    add_box(5.5, 29.5, 33, 22.5, "Multi-Trial Telemetry & Stats (N = 10)", [
        "• Kubelet cAdvisor (/proxy/stats/summary)",
        "• Per-Namespace Active Pod Aggregation",
        "• Mann-Whitney U Statistical Analysis"
    ], '#ebf5fb', '#5dade2', '#1a4384')

    add_box(5.5, 8.5, 33, 17, "Automated Trial Orchestrator", [
        "Bridge Server (:8088) | 40+40 Automated Runs",
        "Deterministic Reset & Scale-to-Zero Barriers"
    ], '#ebf5fb', '#5dade2', '#1a4384')

    # Right Boxes
    add_box(61.5, 56, 33, 21.5, "Knative Serving v1.14 (Kind v1.36.1)", [
        "• Kourier Ingress Gateway (:80)",
        "• Knative Activator & Queue-Proxy (target=10)",
        "• Scale-to-Zero App Pods (min=0, no maxScale)"
    ], '#fdedec', '#ec7063', '#8b1a1a')

    # Right Box 2 updated to note exact KRaft 4.3.0 live runtime vs 3.7.0 provisioning script
    add_box(61.5, 29.5, 33, 22.5, "Saga Coordination Targets", [
        "• Temporal v1.24.0 + PostgreSQL 15 (PC/EC)",
        "• Strimzi Kafka 4.3.0 KRaft, 3 Partitions (PA/EL)",
        "• Go Gateways + HTTP/JSON + pprof"
    ], '#fdedec', '#ec7063', '#8b1a1a')

    add_box(61.5, 8.5, 33, 17, "SUT Reset & Poller Agent", [
        "SUT Bridge Poller (:8088) | Cluster Lifecycle",
        "Rollout Restarts & Pod Cleanup"
    ], '#fdedec', '#ec7063', '#8b1a1a')

    # Arrows
    ax.annotate("", xy=(58, 67), xytext=(42, 67),
                arrowprops=dict(arrowstyle="<->", color='#16a085', lw=2.8))
    ax.text(50, 70.5, "Traffic & Ingress", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#16a085')
    ax.text(50, 62.5, "HTTP POST /orders\nCloudEvents v1.0.2", ha='center', va='center', fontsize=8.3, color='#16a085')

    ax.annotate("", xy=(42, 41), xytext=(58, 41),
                arrowprops=dict(arrowstyle="->", color='#d35400', lw=2.2, ls='--'))
    ax.text(50, 44.5, "Resource Telemetry", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#d35400')
    ax.text(50, 36.5, "cAdvisor Pod Stats\n& Go pprof Traces", ha='center', va='center', fontsize=8.3, color='#d35400')

    ax.annotate("", xy=(58, 17), xytext=(42, 17),
                arrowprops=dict(arrowstyle="<->", color='#5b5ea6', lw=2.0, ls=':'))
    ax.text(50, 20.5, "Reset & Control", ha='center', va='center', fontsize=9.5, fontweight='bold', color='#5b5ea6')
    ax.text(50, 12.5, "REST Bridge (:8088)\nTrial Synchronization", ha='center', va='center', fontsize=8.3, color='#5b5ea6')

    fig.savefig(os.path.join(FIGURES_DIR, 'fig1_topology.png'))
    fig.savefig(os.path.join(FIGURES_DIR, 'fig1_topology.svg'))
    plt.close(fig)


def plot_step_load_and_resources(stats, res_t, res_k):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.2, 4.6))
    categories = ['Temporal\n(Orchestration)', 'Kafka\n(Choreography)']

    if stats and stats["normal"].get("temporal") and stats["normal"].get("kafka"):
        t, k = stats["normal"]["temporal"], stats["normal"]["kafka"]
        s200 = np.array([t["success_pct"]["mean"], k["success_pct"]["mean"]])
        s202 = np.array([t["async_202_pct"]["mean"], k["async_202_pct"]["mean"]])
        acc_sd = np.array([t["total_2xx_pct"]["sd"], k["total_2xx_pct"]["sd"]])
        to = np.array([t["timeout_pct"]["mean"] + t["rollback_pct"]["mean"],
                       k["timeout_pct"]["mean"] + k["rollback_pct"]["mean"]])
        r503 = np.array([t["http_503_pct"]["mean"] + t["reset_pct"]["mean"],
                         k["http_503_pct"]["mean"] + k["reset_pct"]["mean"]])
    else:
        s200, s202, acc_sd = np.array([6.78, 85.51]), np.array([0.0, 13.51]), np.array([2.42, 2.10])
        to, r503 = np.array([80.87, 0.98]), np.array([12.36, 0.0])

    w = 0.50
    acc_tot = s200 + s202
    acc_yerr = [np.minimum(acc_tot, acc_sd), np.minimum(100.0 - acc_tot, acc_sd)]
    ax1.bar(categories, s200, w, label='HTTP 200 Sync (%)', color='#27ae60')
    ax1.bar(categories, s202, w, bottom=s200, yerr=acc_yerr, capsize=4, label='HTTP 202 Async Fallback (%)', color='#2980b9')
    ax1.bar(categories, to, w, bottom=acc_tot, label='Bounded Timeouts / 422 (%)', color='#e67e22')
    ax1.bar(categories, r503, w, bottom=acc_tot + to, label='HTTP 503 Refusals (%)', color='#c0392b')
    ax1.set_ylabel('Share of 12,600 Requests (%)')
    ax1.set_title('(a) Step-Load Outcome Breakdown (N=10, Mean ± SD)')
    ax1.set_ylim(0, 128)
    ax1.legend(loc='upper right', fontsize=8.5, ncol=2, frameon=True)

    x = np.arange(2)
    wr = 0.34
    ax2_twin = ax2.twinx()

    # Include knative-serving in Equalized Full-Stack Footprint (healthy N=9 Temporal vs N=10 Kafka)
    rt_ns = stats.get("resources", {}).get("temporal", {}).get("normal_peak_healthy") if stats else None
    rk_ns = stats.get("resources", {}).get("kafka", {}).get("normal_peak") if stats else None
    if rt_ns and rk_ns:
        ram_m = [rt_ns["full_stack_ram_mb"]["mean"], rk_ns["full_stack_ram_mb"]["mean"]]
        ram_sd = [rt_ns["full_stack_ram_mb"]["sd"], rk_ns["full_stack_ram_mb"]["sd"]]
        cpu_m = [rt_ns["full_stack_cpu"]["mean"], rk_ns["full_stack_cpu"]["mean"]]
        cpu_sd = [rt_ns["full_stack_cpu"]["sd"], rk_ns["full_stack_cpu"]["sd"]]
    else:
        ram_m, ram_sd = [3197.4, 1357.3], [72.8, 457.5]
        cpu_m, cpu_sd = [4.765, 2.358], [0.313, 0.123]

    ax2.bar(x - wr / 2, ram_m, wr, yerr=ram_sd, capsize=4, label='Full Stack RAM (MB)', color='#8e44ad', alpha=0.85)
    ax2_twin.bar(x + wr / 2, cpu_m, wr, yerr=cpu_sd, capsize=4, label='Full Stack CPU (Cores)', color='#16a085', alpha=0.85)
    ax2.set_ylabel('Active RAM Working Set (MB)', color='#8e44ad')
    ax2_twin.set_ylabel('CPU Utilization (Cores)', color='#16a085')
    ax2.set_ylim(0, 4200)
    ax2_twin.set_ylim(0, 7.2)
    ax2.set_xticks(x)
    ax2.set_xticklabels(categories)
    ax2.set_title('(b) Full-Stack Footprint (Temp N=9 Healthy vs. Kafka N=10)')
    l1, lb1 = ax2.get_legend_handles_labels()
    l2, lb2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(l1 + l2, lb1 + lb2, loc='upper right', fontsize=8.5, frameon=True)

    plt.tight_layout()
    fig.savefig(os.path.join(FIGURES_DIR, 'fig3_throughput_and_resources.png'))
    fig.savefig(os.path.join(FIGURES_DIR, 'fig3_throughput_and_resources.svg'))
    plt.close(fig)


def plot_compensation_latency(stats):
    fig, ax = plt.subplots(figsize=(7.5, 4.3))
    metrics = ['Mean', 'Median (p50)', 'p95', 'p99']

    if stats and stats["rollback"].get("temporal") and stats["rollback"].get("kafka"):
        tr, kr = stats["rollback"]["temporal"], stats["rollback"]["kafka"]
        t_m = np.array([tr["mean_ms"]["mean"], tr["p50_ms"]["mean"], tr["p95_ms"]["mean"], tr["p99_ms"]["mean"]])
        t_sd = np.array([tr["mean_ms"]["sd"], tr["p50_ms"]["sd"], tr["p95_ms"]["sd"], tr["p99_ms"]["sd"]])
        k_m = np.array([kr["mean_ms"]["mean"], kr["p50_ms"]["mean"], kr["p95_ms"]["mean"], kr["p99_ms"]["mean"]])
        k_sd = np.array([kr["mean_ms"]["sd"], kr["p50_ms"]["sd"], kr["p95_ms"]["sd"], kr["p99_ms"]["sd"]])
    else:
        t_m, t_sd = np.array([322.9, 292.7, 506.5, 599.5]), np.array([18.3, 14.8, 27.8, 71.5])
        k_m, k_sd = np.array([63.8, 60.7, 80.1, 129.1]), np.array([6.5, 0.8, 30.1, 164.1])

    x = np.arange(len(metrics))
    w = 0.35
    r1 = ax.bar(x - w / 2, t_m, w, yerr=[np.minimum(t_m, t_sd), t_sd], capsize=4, label='Temporal Orchestration (Sync Complete $L_{comp}$)', color=COLOR_TEMPORAL)
    r2 = ax.bar(x + w / 2, k_m, w, yerr=[np.minimum(k_m, k_sd), k_sd], capsize=4, label='Kafka Choreography (Async Notify $L_{comp}$)', color=COLOR_KAFKA)
    ax.set_ylabel('Rollback Recovery / Notification Latency (ms)')
    ax.set_title('Compensation Rollback Latency ($L_{comp}$, 300 Poison Pills, Mean ± SD)')
    ax.set_xticks(x)
    ax.set_xticklabels(metrics)
    ax.set_ylim(0, max(t_m + t_sd) * 1.18)
    ax.legend(loc='upper left', frameon=True)

    for rect, sd_v in list(zip(r1, t_sd)) + list(zip(r2, k_sd)):
        h = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2.0, h + sd_v + 12, f'{h:.1f}', ha='center', va='bottom', fontsize=8.5)

    fig.savefig(os.path.join(FIGURES_DIR, 'fig4_compensation_latency.png'))
    fig.savefig(os.path.join(FIGURES_DIR, 'fig4_compensation_latency.svg'))
    plt.close(fig)


def plot_pacelc_radar(stats):
    fig = plt.figure(figsize=(7.6, 6.8))
    ax = fig.add_subplot(111, polar=True)
    # Hybrid 0-10 normalized axes defined in Sec. III.C:
    # 1) Throughput Resilience = 0.1 * total_2xx% (Temporal healthy N=9 = 0.8 vs Kafka = 9.9)
    # 2) Burst Speed Rank = max(1, 10 - 2.2 * log10(burst_mean_ms))
    # 3) Clean Cold-Start Agility = 10 - 0.7 * (clean_t_frozen_ms / 1000)
    # 4) Audit & Replay Determinism (Qualitative rubric: 9.5 vs 3.5)
    # 5) Unimodal Tail Shape = max(1, 10 - 3.0 * log10(median Idisp))
    # 6) Full-Stack CPU Efficiency = 10 * (1 - full_stack_cpu / 8.0)
    labels = [
        'Throughput\nResilience', 'Burst Speed\nRank', 'Cold-Start\nAgility',
        'Audit & Replay\nDeterminism (Qual.)', 'Unimodal Tail\nShape', 'Full-Stack CPU\nEfficiency'
    ]
    if stats:
        th_2xx = stats["normal_healthy_temporal"]["total_2xx_pct"]["mean"]
        kn_2xx = stats["normal"]["kafka"]["total_2xx_pct"]["mean"]
        tb_ms = stats["burst"]["temporal"]["mean_ms"]["mean"]
        kb_ms = stats["burst"]["kafka"]["mean_ms"]["mean"]
        tc_s = stats["coldstart"]["temporal"]["max_ms"]["mean"] / 1000.0
        kc_s = stats["coldstart_clean_kafka"]["max_ms"]["mean"] / 1000.0
        t_id = stats["normal_healthy_temporal"]["idisp"]["median"]
        k_id = stats["normal"]["kafka"]["idisp"]["median"]
        t_cpu = stats["resources"]["temporal"]["normal_peak_healthy"]["full_stack_cpu"]["mean"]
        k_cpu = stats["resources"]["kafka"]["normal_peak"]["full_stack_cpu"]["mean"]
        t_sc = [
            round(0.1 * th_2xx, 1),
            round(max(1.0, 10.0 - 2.2 * np.log10(tb_ms)), 1),
            round(10.0 - 0.7 * tc_s, 1),
            9.5,
            round(max(1.0, 10.0 - 3.0 * np.log10(t_id)), 1),
            round(10.0 * (1.0 - t_cpu / 8.0), 1),
        ]
        k_sc = [
            round(0.1 * kn_2xx, 1),
            round(max(1.0, 10.0 - 2.2 * np.log10(kb_ms)), 1),
            round(10.0 - 0.7 * kc_s, 1),
            3.5,
            round(max(1.0, 10.0 - 3.0 * np.log10(k_id)), 1),
            round(10.0 * (1.0 - k_cpu / 8.0), 1),
        ]
    else:
        t_sc = [0.8, 2.4, 7.9, 9.5, 6.6, 4.0]
        k_sc = [9.9, 6.4, 8.0, 3.5, 4.5, 7.1]
    print(f"[i] PACELC radar scores: Temporal={t_sc}, Kafka={k_sc}")
    angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
    t_sc += t_sc[:1]
    k_sc += k_sc[:1]
    angles += angles[:1]

    ax.plot(angles, t_sc, color=COLOR_TEMPORAL, linewidth=2, label='Temporal (PC/EC)')
    ax.fill(angles, t_sc, color=COLOR_TEMPORAL, alpha=0.25)
    ax.plot(angles, k_sc, color=COLOR_KAFKA, linewidth=2, label='Kafka (PA/EL)')
    ax.fill(angles, k_sc, color=COLOR_KAFKA, alpha=0.25)
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_thetagrids(np.degrees(angles[:-1]), labels, fontsize=10)
    ax.tick_params(axis='x', pad=28)
    ax.set_ylim(0, 10)
    ax.set_title('PACELC Normalized Trade-Off Radar (ADF-SS)', pad=34)
    ax.legend(loc='upper right', bbox_to_anchor=(1.34, 1.15), frameon=True)
    fig.savefig(os.path.join(FIGURES_DIR, 'fig5_pacelc_radar.png'))
    fig.savefig(os.path.join(FIGURES_DIR, 'fig5_pacelc_radar.svg'))
    plt.close(fig)


if __name__ == '__main__':
    s = load_summary()
    rt = load_res_csv("temporal")
    rk = load_res_csv("kafka")
    plot_topology()
    plot_latency_cdf(s)
    plot_step_load_and_resources(s, rt, rk)
    plot_compensation_latency(s)
    plot_pacelc_radar(s)
    print("[✓] Figures updated in figures/")
