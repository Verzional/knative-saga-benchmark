#!/usr/bin/env bash
# Automated N-Trial Benchmark Runner (Full Isolation Between Heavy Scenarios & Trials)
# Orchestrates Legion resets & active-pod kubelet snapshots via Antigravity Bridge.
# Usage: bash benchmarks/run_multitrial.sh <temporal|kafka> [N=10] [START_TRIAL=1]

set -euo pipefail

ARCH="${1:-}"
TRIALS="${2:-10}"
START_TRIAL="${3:-1}"
RUN_ID=$(date +%s)
TARGET="http://192.168.10.10:80"
BRIDGE="http://127.0.0.1:8088"
OUT_DIR="results/trials/${ARCH}"

if [[ "$ARCH" != "temporal" && "$ARCH" != "kafka" ]]; then
  echo "Usage: $0 <temporal|kafka> [N=10] [START_TRIAL=1]"
  exit 1
fi

mkdir -p "$OUT_DIR"

echo "=========================================================="
echo " Clean Multi-Trial Benchmark Runner: ARCH=$ARCH | N=$TRIALS (start=$START_TRIAL)"
echo " Target: $TARGET | Bridge: $BRIDGE"
echo " Output: $OUT_DIR"
echo "=========================================================="

CSV_FILE="${OUT_DIR}/resource_snapshots.csv"
if [[ ! -f "$CSV_FILE" ]]; then
  echo "trial,arch,phase,timestamp,temporal_cpu,temporal_ram_mb,kafka_cpu,kafka_ram_mb,default_cpu,default_ram_mb,knative_cpu,knative_ram_mb" > "$CSV_FILE"
fi

run_bridge_py() {
  local task_id="$1"
  local py_code="$2"
  local max_wait="${3:-180}"

  python3 - "$BRIDGE" "$task_id" "$py_code" "$max_wait" << 'PYEOF'
import sys, json, time, base64, urllib.request

bridge, tid, code, max_wait = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
b64 = base64.b64encode(code.encode()).decode()
cmd = f'python3 -c "import base64; exec(base64.b64decode(\'{b64}\'))"'
req = urllib.request.Request(
    f"{bridge}/api/tasks/dispatch",
    data=json.dumps({"task_id": tid, "action": "EXEC", "commands": [cmd]}).encode(),
    headers={"Content-Type": "application/json"},
)
urllib.request.urlopen(req, timeout=5)

deadline = time.time() + max_wait
while time.time() < deadline:
    with urllib.request.urlopen(f"{bridge}/api/messages", timeout=5) as r:
        msgs = json.loads(r.read().decode()).get("messages", [])
    hits = [m for m in msgs if m.get("task_id") == tid and m.get("event_type") == "TASK_COMPLETE"]
    if hits:
        last = hits[-1]
        print(last.get("output", "").strip())
        sys.exit(0 if last.get("status") == "SUCCESS" else 1)
    time.sleep(2)
print(f"TIMEOUT waiting for {tid}", file=sys.stderr)
sys.exit(1)
PYEOF
}

reset_cluster_state() {
  local tag="$1"
  echo "[*] Resetting $ARCH state on Legion ($tag)..."
  if [[ "$ARCH" == "temporal" ]]; then
    run_bridge_py "reset-${RUN_ID}-${ARCH}-${tag}" '
import subprocess, time
r=lambda a:subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl"]+a,check=True)
r(["rollout","restart","deployment/postgres","deployment/temporal-server","-n","temporal"])
r(["rollout","status","deployment/postgres","-n","temporal","--timeout=120s"])
r(["rollout","status","deployment/temporal-server","-n","temporal","--timeout=120s"])
for _ in range(45):
    p=subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl","exec","-n","temporal","deploy/temporal-server","--","temporal","operator","namespace","describe","default"],capture_output=True)
    if p.returncode==0: break
    time.sleep(2)
subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl","delete","pods","--all","-n","default","--force","--grace-period=0"],check=False)
for _ in range(10):
    out=subprocess.check_output(["docker","exec","saga-benchmark-control-plane","kubectl","get","pods","-n","default","--field-selector=status.phase=Running","--no-headers"]).decode().strip()
    if not out: break
    time.sleep(2)
time.sleep(5)
print("TEMPORAL_RESET_READY_NS_VERIFIED")
' 300
  else
    run_bridge_py "reset-${RUN_ID}-${ARCH}-${tag}" '
import subprocess, json, time
r=lambda a:subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl"]+a,check=True)
subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl","delete","pod","saga-kafka-dual-role-pool-0","-n","kafka","--ignore-not-found=true","--timeout=90s"],check=False)
sps=json.loads(subprocess.check_output(["docker","exec","saga-benchmark-control-plane","kubectl","get","strimzipodset","-n","kafka","-o","json"]))
pod_spec=sps["items"][0]["spec"]["pods"][0]
pod_spec["metadata"]["namespace"]="kafka"
pod_spec["metadata"].pop("resourceVersion",None)
pod_spec["metadata"].pop("uid",None)
subprocess.run(["docker","exec","-i","saga-benchmark-control-plane","kubectl","apply","-f","-"],input=json.dumps(pod_spec).encode(),check=True)
r(["wait","--for=condition=ready","pod/saga-kafka-dual-role-pool-0","-n","kafka","--timeout=180s"])
for tp in ("order-events","payment-events","inventory-events"):
    subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl","exec","-n","kafka","saga-kafka-dual-role-pool-0","--","/opt/kafka/bin/kafka-topics.sh","--bootstrap-server","localhost:9092","--create","--if-not-exists","--topic",tp,"--partitions","3","--replication-factor","1"],check=False)
subprocess.run(["docker","exec","saga-benchmark-control-plane","kubectl","delete","pods","--all","-n","default","--force","--grace-period=0"],check=False)
for _ in range(10):
    out=subprocess.check_output(["docker","exec","saga-benchmark-control-plane","kubectl","get","pods","-n","default","--field-selector=status.phase=Running","--no-headers"]).decode().strip()
    if not out: break
    time.sleep(2)
time.sleep(8)
print("KAFKA_RESET_READY_ZERO_PODS")
' 300
  fi
}

capture_resource_snapshot() {
  local trial="$1"
  local phase="$2"
  local out
  # Compact script (<550 bytes) so bridge 2000-char log limit never truncates CSV_ROW
  out=$(run_bridge_py "snap-${RUN_ID}-${ARCH}-${trial}-${phase}" '
import subprocess,json,time
g=lambda a:subprocess.check_output(["docker","exec","saga-benchmark-control-plane","kubectl","get"]+a)
act={(i["metadata"]["namespace"],i["metadata"]["name"])for i in json.loads(g(["pods","-A","-o","json"]))["items"] if i["status"]["phase"]=="Running"}
agg={k:[0.,0.]for k in("temporal","kafka","default","knative-serving")}
for p in json.loads(g(["--raw","/api/v1/nodes/saga-benchmark-control-plane/proxy/stats/summary"]))["pods"]:
 n,m=p["podRef"]["namespace"],p["podRef"]["name"]
 if n in agg and(n,m)in act:agg[n][0]+=p.get("cpu",{}).get("usageNanoCores",0)/1e9;agg[n][1]+=p.get("memory",{}).get("workingSetBytes",0)/1048576
print("CSV_ROW:"+",".join([f"{time.time():.1f}"]+[f"{v[0]:.4f},{v[1]:.2f}"for v in agg.values()]))
' 30 || true)
  local row
  row=$(echo "$out" | grep "^CSV_ROW:" | sed 's/^CSV_ROW://' || true)
  if [[ -n "$row" ]]; then
    echo "${trial},${ARCH},${phase},${row}" >> "$CSV_FILE"
    echo "[*] Captured resource snapshot ($phase): $row"
  else
    echo "[!] Resource snapshot missed ($phase): $out"
  fi
}

for i in $(seq "$START_TRIAL" "$TRIALS"); do
  echo ""
  echo "----------------------------------------------------------"
  echo " [$ARCH] CLEAN TRIAL $i / $TRIALS"
  echo "----------------------------------------------------------"

  # 1. Reset state & verify 0 default pods -> Cold Start + Rollback + Burst
  reset_cluster_state "${i}-pre"
  capture_resource_snapshot "$i" "idle"

  echo "[*] ($i/$TRIALS) Running Cold Start probe..."
  npx --yes artillery run --target "$TARGET" -o "${OUT_DIR}/coldstart_trial_${i}.json" benchmarks/load-test-coldstart.yaml

  echo "[*] ($i/$TRIALS) Waiting 10s before Rollback..."
  sleep 10
  echo "[*] ($i/$TRIALS) Running Rollback Path (300 poison pills)..."
  npx --yes artillery run --target "$TARGET" -o "${OUT_DIR}/rollback_trial_${i}.json" benchmarks/load-test-rollback.yaml

  echo "[*] ($i/$TRIALS) Waiting 10s before Burst..."
  sleep 10
  echo "[*] ($i/$TRIALS) Running Burst Profile (80 RPS)..."
  npx --yes artillery run --target "$TARGET" -o "${OUT_DIR}/burst_trial_${i}.json" benchmarks/burst-profile.yaml

  # 2. Reset again before heavy Normal Step-Load so Burst backlog never bleeds into Normal!
  reset_cluster_state "${i}-norm"
  echo "[*] ($i/$TRIALS) Waiting 10s for clean Normal Step-Load start..."
  sleep 10
  echo "[*] ($i/$TRIALS) Running Normal Step-Load (150s)..."
  (
    sleep 130
    capture_resource_snapshot "$i" "normal_peak"
  ) &
  SNAP_PID=$!
  npx --yes artillery run --target "$TARGET" -o "${OUT_DIR}/normal_trial_${i}.json" benchmarks/load-test-normal.yaml
  wait "$SNAP_PID" || true

  echo "[✓] ($i/$TRIALS) Clean Trial $i complete!"
done

echo ""
echo "=========================================================="
echo " All $TRIALS clean isolated trials for [$ARCH] completed!"
echo " Results saved in: $OUT_DIR"
echo "=========================================================="
