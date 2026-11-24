#!/usr/bin/env python3
"""
Metrics Extractor for Prometheus Telemetry
Queries Prometheus API across the network and exports p95, p99, Cold Start,
Compensation Latency, and RAM/CPU metrics for Orchestration vs Choreography.
"""
import urllib.request
import urllib.parse
import json
import csv
import sys
import os
import time

PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

QUERIES = {
    # 1. End-to-End Latency
    "p50_e2e_latency_ms": "histogram_quantile(0.50, sum(rate(knative_revision_app_request_latencies_bucket[1m])) by (le))",
    "p95_e2e_latency_ms": "histogram_quantile(0.95, sum(rate(knative_revision_app_request_latencies_bucket[1m])) by (le))",
    "p99_e2e_latency_ms": "histogram_quantile(0.99, sum(rate(knative_revision_app_request_latencies_bucket[1m])) by (le))",
    
    # 2. Throughput & Error Rates
    "total_rps": "sum(rate(knative_revision_app_request_count[1m]))",
    "http_503_error_rps": "sum(rate(knative_revision_app_request_count{response_code=\"503\"}[1m]))",
    
    # 3. Resource Utilization (Legion Node Server)
    "temporal_ram_mb": "sum(container_memory_working_set_bytes{namespace=\"temporal\"}) / 1024 / 1024",
    "temporal_cpu_cores": "sum(rate(container_cpu_usage_seconds_total{namespace=\"temporal\"}[1m]))",
    "kafka_ram_mb": "sum(container_memory_working_set_bytes{namespace=\"kafka\"}) / 1024 / 1024",
    "kafka_cpu_cores": "sum(rate(container_cpu_usage_seconds_total{namespace=\"kafka\"}[1m]))",
    
    # 4. Microservices namespace
    "services_ram_mb": "sum(container_memory_working_set_bytes{namespace=\"default\"}) / 1024 / 1024",
    "services_cpu_cores": "sum(rate(container_cpu_usage_seconds_total{namespace=\"default\"}[1m]))",
}

def query_prometheus(query):
    url = f"{PROMETHEUS_URL}/api/v1/query?query={urllib.parse.quote(query)}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if data.get("status") == "success":
                result = data.get("data", {}).get("result", [])
                if result:
                    return float(result[0].get("value", [0, 0])[1])
            return 0.0
    except Exception as e:
        sys.stderr.write(f"Query error for [{query}]: {e}\n")
        return None

def collect_snapshot(architecture="TEMPORAL", scenario="NORMAL"):
    timestamp = time.time()
    snapshot = {
        "timestamp": timestamp,
        "architecture": architecture,
        "scenario": scenario
    }
    for metric_name, query_str in QUERIES.items():
        val = query_prometheus(query_str)
        snapshot[metric_name] = val
    return snapshot

def save_csv(data_list, output_file="results/metrics_summary.csv"):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    if not data_list:
        return
    keys = data_list[0].keys()
    file_exists = os.path.isfile(output_file)
    with open(output_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerows(data_list)
    print(f"[+] Metrics appended to {output_file}")

if __name__ == '__main__':
    arch = sys.argv[1] if len(sys.argv) > 1 else "TEMPORAL"
    scen = sys.argv[2] if len(sys.argv) > 2 else "NORMAL"
    print(f"[*] Scraping Prometheus snapshot for {arch} ({scen})...")
    metrics = collect_snapshot(arch, scen)
    print(json.dumps(metrics, indent=2))
    save_csv([metrics])
