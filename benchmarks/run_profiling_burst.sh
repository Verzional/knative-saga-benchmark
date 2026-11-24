#!/bin/bash
set -e
echo "=== Starting simultaneous 10s CPU profiling and Artillery burst ==="
curl -s -H "Host: order.default.192.168.10.10.nip.io" "http://192.168.10.10:80/debug/pprof/profile?seconds=10" > results/profiles/cpu_peak.pprof &
CURL_PID=$!

npx --yes artillery quick --count 100 -n 15 \
  -H "Host: order.default.192.168.10.10.nip.io" \
  -H "Content-Type: application/json" \
  http://192.168.10.10:80/health || true

wait $CURL_PID
echo "=== CPU Profile Captured ==="
ls -lh results/profiles/cpu_peak.pprof
