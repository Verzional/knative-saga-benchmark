#!/bin/bash
# Antigravity Inter-Agent Bridge CLI
# Usage:
#   ./bridge_cli.sh health [--target http://...]
#   ./bridge_cli.sh status [--target http://...]
#   ./bridge_cli.sh tasks [--target http://...]
#   ./bridge_cli.sh report-status --state <STATE> [--target http://...]
#   ./bridge_cli.sh dispatch --action <ACTION> --payload '<JSON>' [--target http://...]
#   ./bridge_cli.sh dispatch-sync [--target http://...]
#   ./bridge_cli.sh poll [--target http://...]
#   ./bridge_cli.sh poll-and-execute [--target http://...]
#   ./bridge_cli.sh sync [--target http://...]
#   ./bridge_cli.sh wait --state <EXPECTED_STATE> [--timeout 300] [--target http://...]

TARGET="http://127.0.0.1:8088"
LEGION_TARGET="http://192.168.10.10:8088"

ACTION=""
STATE=""
PAYLOAD="{}"
TIMEOUT=300

while [[ $# -gt 0 ]]; do
  case $1 in
    health|status|tasks|report-status|dispatch|dispatch-sync|poll|poll-and-execute|sync|wait)
      CMD="$1"
      shift
      ;;
    --target)
      TARGET="$2"
      shift 2
      ;;
    --to-legion)
      TARGET="$LEGION_TARGET"
      shift
      ;;
    --action)
      ACTION="$2"
      shift 2
      ;;
    --state)
      STATE="$2"
      shift 2
      ;;
    --payload)
      PAYLOAD="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

case "$CMD" in
  health)
    curl -s "${TARGET}/health"
    echo ""
    ;;
  status)
    curl -s "${TARGET}/api/status"
    echo ""
    ;;
  tasks)
    curl -s "${TARGET}/api/tasks"
    echo ""
    ;;
  report-status)
    curl -s -X POST "${TARGET}/api/status" \
      -H "Content-Type: application/json" \
      -d "{\"macbook_state\": \"$STATE\", \"last_action\": \"$ACTION\"}"
    echo ""
    ;;
  dispatch)
    curl -s -X POST "${TARGET}/api/tasks/dispatch" \
      -H "Content-Type: application/json" \
      -d "{\"action\": \"$ACTION\", \"payload\": $PAYLOAD}"
    echo ""
    ;;
  dispatch-sync)
    SYNC_TASK='{"action": "SYNC_BUNDLE", "description": "Synchronize entire repository workspace from MacBook", "payload": {"archive": "files.tar.gz", "verify": "results/trials/multitrial_summary.json"}}'
    curl -s -X POST "${TARGET}/api/tasks/dispatch" \
      -H "Content-Type: application/json" \
      -d "$SYNC_TASK"
    echo ""
    ;;
  poll)
    curl -s "${TARGET}/api/tasks/poll"
    echo ""
    ;;
  poll-and-execute)
    python3 bridge/legion_worker.py --bridge "${TARGET}" --once
    ;;
  sync)
    echo "[*] Connecting to ${TARGET} to download files.tar.gz..."
    curl -# "${TARGET}/files.tar.gz" -o files.tar.gz
    echo "[*] Extracting files..."
    tar -xzf files.tar.gz
    rm -f files.tar.gz
    echo "[✓] Synchronized files successfully."
    ;;
  wait)
    echo "Waiting for target ${TARGET} state to become '${STATE}' (timeout ${TIMEOUT}s)..."
    ELAPSED=0
    while [ $ELAPSED -lt $TIMEOUT ]; do
      CURRENT_STATE=$(curl -s "${TARGET}/api/status" | python3 -c "import sys, json; print(json.load(sys.stdin).get('legion_state', ''))" 2>/dev/null || echo "")
      if [ "$CURRENT_STATE" = "$STATE" ]; then
        echo "State reached: $STATE"
        exit 0
      fi
      sleep 3
      ELAPSED=$((ELAPSED + 3))
    done
    echo "Timeout reached while waiting for state: $STATE"
    exit 1
    ;;
  *)
    echo "Commands: health | status | tasks | report-status | dispatch | dispatch-sync | poll | poll-and-execute | sync | wait"
    exit 1
    ;;
esac
