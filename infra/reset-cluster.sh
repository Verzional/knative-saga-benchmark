#!/bin/bash
# Hard-reset script to guarantee metric isolation between test suites
set -e

TARGET_ARCH="$1" # "TEMPORAL" or "KAFKA" or "ALL"

echo "=== [Hard Reset] Initiating clean state purge for $TARGET_ARCH ==="

if [ "$TARGET_ARCH" = "TEMPORAL" ] || [ "$TARGET_ARCH" = "ALL" ]; then
  echo "[*] Purging Temporal namespace and stateful storage..."
  kubectl delete namespace temporal --ignore-not-found=true --timeout=60s || true
  kubectl delete kservice order-service payment-service inventory-service -n default --ignore-not-found=true || true
fi

if [ "$TARGET_ARCH" = "KAFKA" ] || [ "$TARGET_ARCH" = "ALL" ]; then
  echo "[*] Purging Kafka namespace and stateful storage..."
  kubectl delete namespace kafka --ignore-not-found=true --timeout=60s || true
  kubectl delete kservice order-service payment-service inventory-service -n default --ignore-not-found=true || true
fi

# Force garbage collection / scale down in default namespace
kubectl delete pods --all -n default --force --grace-period=0 || true

echo "=== [Hard Reset] Cleanup completed successfully ==="
