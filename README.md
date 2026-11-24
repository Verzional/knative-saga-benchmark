# Serverless Saga Benchmark: Orchestration (Temporal) vs. Choreography (Kafka KRaft)

[![Platform: Knative Serving](https://img.shields.io/badge/Serverless-Knative_v1.14-green.svg)](https://knative.dev/)
[![Engine: Kubernetes Kind](https://img.shields.io/badge/Kubernetes-Kind_v1.36.1-blue.svg)](https://kind.sigs.k8s.io/)
[![Orchestrator: Temporal.io](https://img.shields.io/badge/Orchestrator-Temporal.io_v1.24.0-black.svg)](https://temporal.io/)
[![Event Broker: Apache Kafka](https://img.shields.io/badge/Broker-Apache_Kafka_4.3.0_(KRaft)-red.svg)](https://strimzi.io/)

Codebase, Kubernetes/Knative provisioning configs, Artillery benchmark suites, `N = 10` multi-trial telemetry datasets, runtime `pprof` traces, statistical aggregators, and 300 DPI plotting scripts evaluating centralized durable **Orchestration** (Temporal Server `v1.24.0` + PostgreSQL `15-alpine`) against decentralized event-driven **Choreography** (Strimzi Apache Kafka KRaft) on Knative Serving (`minScale: 0`).

---

## Architecture Overview

1. **Temporal Orchestration (`PC/EC`):** Go ingress HTTP gateway (`services/temporal/order_service/main.go`) forwards synchronous workflow execution (`CreateOrderSaga` on `SAGA_ORDER_QUEUE`, 8 s HTTP timeout) to Temporal Server `v1.24.0` + PostgreSQL `15-alpine` (`max_connections=500`, `SQL_MAX_CONNS=200`). Reference workflow/activity stubs live in `services/temporal/workflow.go`.
2. **Kafka Choreography (`PA/EL`):** Go ingress HTTP gateway (`services/kafka/order_service/main.go`) publishes `OrderEvent` payloads (`ce_specversion`, `ce_type`, `traceparent`) to Kafka (`LeastBytes`, `RequiredAcks=RequireOne`, Snappy, `BatchSize=100`, `BatchTimeout=10ms`), maintains a pod-local `pendingAcks` `sync.Map` for 5 s synchronous correlation, and falls back to HTTP `202 Accepted` (`PROCESSING_ASYNC`) on cross-pod misses.
3. **Knative Autoscaling Config (`services/*/kservice.yaml`):** `minScale: 0`, `maxScale: 10`, KPA `target: 10`, `scaleToZeroGracePeriod: 30s`, `50m/500m` CPU, `64Mi/256Mi` RAM (`GOMEMLIMIT=220MiB`).

---

## Dual-Host Testbed & Multi-Trial Telemetry (`N = 10`)

Benchmarks executed over direct Cat6 Ethernet (`<0.8 ms` RTT, `192.168.10.0/24`):
- **SUT (`192.168.10.10`):** Lenovo Legion 5 Pro 16IAH7, Intel Core i7-12700H (14c/20t), ~10 GB RAM allocated to WSL2 Ubuntu 24.04.3 LTS, Kind (`Kubernetes v1.36.1`), Knative Serving `v1.14` + Kourier ingress.
- **Load Driver (`192.168.10.2`):** Apple MacBook Pro (`Mac17,2`), Apple M5 (10c), 24 GB Unified Memory, `macOS 27.2.1`, Artillery v2.0.

Key results aggregated in [`results/trials/multitrial_summary.json`](results/trials/multitrial_summary.json) (two-sided Mann-Whitney *U* tests):

| Scenario / Metric | Temporal Orchestration | Kafka Choreography | MW *U* *p*-value |
| :--- | :--- | :--- | :--- |
| **Step-Load (10–200 RPS, 12,600 reqs):** Total 2xx | `6.78% ± 2.42%` (`7.53% ± 0.47%` healthy `N = 9`) | `99.02% ± 2.10%` (`85.51%` 200 + `13.51%` 202) | `0.0001` / `0.0002` |
| **Step-Load Failure Breakdown:** Timeout / 503 / 422 | `80.73%` / `12.36%` / `0.14%` (Trial 7 crash: `99.79%` 503) | `0.98%` / `0.00%` / `0.00%` | — |
| **Step-Load 2xx Latency:** Mean / *p*50 [Median] | `1,664.1 ± 124.5 ms` / `614.5 ± 132.1 ms` [`584.2 ms`] (`N = 9`) | `908.4 ± 1,514.6 ms` / `645.8 ± 1,904.0 ms` [`43.4 ms`] | `0.0304` / `0.0030` |
| **Fixed Burst (80 RPS, 1,200 reqs):** Completion / Mean | `9.20% ± 3.35%` (`2,872.5 ± 710.5 ms`) | `100.00% ± 0.00%` (`42.9 ± 0.2 ms`) | `< 0.0001` / `0.0002` |
| **Poison-Pill Rollback (300 pills, 5–10 RPS):** *p*50 *L*comp | `292.7 ± 14.8 ms` (completed rollback) | `60.7 ± 0.8 ms` (client alert notification) | `0.0001` |
| **Scale-to-Zero Cold Starts:** Clean *t*frozen / *Δt*cold | `2,934.7 ms` / `2,786.9 ms` (`N = 10`) | `2,800.0 ms` / `2,753.3 ms` (`N = 3` clean) | `0.3075` (pooled) |
| **Full-Stack Footprint (4 namespaces):** CPU / RAM | `4.402` (`4.765` `N = 9`) cores / `3,041.4` (`3,197.4`) MB | `2.358` cores / `1,357.3` MB (`50.5%` CPU saved vs. `N = 9`) | — |
| **Amortized Cloud Run Cost:** Total / 1k 2xx Tx | `$0.0258` (`$0.0246` `N = 9`) | `$0.0011` (`95.6%` / `95.4%` saved); `$0.0013` sync-200 | — |

---

## Repository Structure

```text
├── benchmarks/
│   ├── analyze_multitrial.py          # N=10 stats, IQR, MW U tests, healthy/clean subset aggregator
│   ├── extract_metrics.py             # Single-run metric extractor
│   ├── run_multitrial.sh              # Automated 10-trial orchestrator across all scenarios
│   ├── run_profiling_burst.sh         # Burst pprof capture helper
│   ├── load-test-normal.yaml          # 10 -> 200 RPS 150s step-load (12,600 requests)
│   ├── burst-profile.yaml             # 80 RPS fixed burst (1,200 requests)
│   ├── load-test-rollback.yaml        # 300 poison pills (5-10 RPS over 40s)
│   ├── load-test-coldstart.yaml       # Scale-to-zero activation probe (5 requests)
│   └── ...
├── bridge/
│   ├── agent_bridge.py                # Dual-host HTTP REST bridge (:8088)
│   ├── bridge_cli.sh                  # Bridge inspection CLI
│   └── legion_worker.py               # SUT polling worker
├── figures/                           # 300 DPI PNG & SVG benchmark figures (fig1..fig5)
├── infra/
│   ├── kind-config.yaml               # Multi-node Kind reference template
│   ├── install-knative.sh             # Knative Serving v1.14 + Kourier setup
│   ├── setup-temporal.sh              # Temporal v1.24.0 + PostgreSQL 15 setup
│   ├── setup-kafka.sh                 # Strimzi Kafka setup script
│   ├── reset-cluster.sh               # Persistence reset script between runs
│   └── grafana/                       # Grafana provisioning & benchmark dashboard JSON
├── results/
│   ├── trials/                        # 80 raw Artillery JSONs + resource_snapshots.csv + multitrial_summary.json
│   └── profiles/                      # Captured Go pprof profiles (cpu_live_burst, cpu_peak, heap, goroutine)
├── scripts/
│   └── generate_figures.py            # Reproducible 300 DPI PNG/SVG plot generator
└── services/
    ├── common/cloudevents.go          # Shared CloudEvent structs
    ├── temporal/                      # Temporal Order Gateway (HTTP -> gRPC) & reference workflow.go
    └── kafka/                         # Kafka Order Gateway (pprof :6060, pendingAcks, OrderEvent)
```

---

## Reproducing Analysis & Figures

```bash
# Recompute multi-trial statistics & Mann-Whitney U p-values
python3 benchmarks/analyze_multitrial.py

# Regenerate all 300 DPI PNG/SVG figures into figures/
python3 scripts/generate_figures.py
```
