#!/usr/bin/env python3
"""
Autonomous Legion Agent Worker for Antigravity Bridge
Polls the bridge server on MacBook (http://192.168.10.2:8088) for synchronization,
system configuration, and benchmark tasks. Executes them autonomously and reports
execution results and system health back to the bridge.

Usage:
  python3 bridge/legion_worker.py [--bridge http://192.168.10.2:8088] [--once | --daemon]
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.error

DEFAULT_BRIDGE_URL = "http://192.168.10.2:8088"

def http_get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "Antigravity-Legion-Worker/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))

def http_post(url, data, timeout=10):
    encoded = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Antigravity-Legion-Worker/1.0"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8'))

def download_file(url, target_path, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "Antigravity-Legion-Worker/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(target_path, 'wb') as out_f:
        shutil.copyfileobj(resp, out_f)
    return os.path.getsize(target_path)

def report_status(bridge_url, legion_state, details=None):
    payload = {
        "device": "legion",
        "legion_state": legion_state,
        "last_seen": time.time()
    }
    if details:
        payload["legion_details"] = details
    try:
        http_post(f"{bridge_url}/api/status", payload)
    except Exception as e:
        print(f"[!] Warning: Failed to report status to bridge: {e}")

def log_message(bridge_url, event_type, message, task_id=None, status="INFO", output=""):
    payload = {
        "source": "legion",
        "timestamp": time.time(),
        "event_type": event_type,
        "status": status,
        "message": message,
        "task_id": task_id,
        "output": output[:2000]
    }
    try:
        http_post(f"{bridge_url}/api/messages", payload)
    except Exception as e:
        print(f"[!] Warning: Failed to log message to bridge: {e}")

def execute_shell_commands(commands, work_dir="."):
    logs = []
    success = True
    for cmd in commands:
        print(f"[+] Executing: {cmd}")
        t0 = time.time()
        res = subprocess.run(cmd, shell=True, cwd=work_dir, capture_output=True, text=True)
        dur = round(time.time() - t0, 2)
        out = (res.stdout + res.stderr).strip()
        log_entry = f"$ {cmd} (code {res.returncode}, {dur}s)\n{out}"
        logs.append(log_entry)
        print(out if out else f"[✓ Done in {dur}s]")
        if res.returncode != 0:
            print(f"[!] Command failed with exit code {res.returncode}")
            success = False
            break
    return success, "\n\n".join(logs)

def handle_task(task, bridge_url):
    action = task.get("action", "").upper()
    task_id = task.get("task_id", f"task-{int(time.time())}")
    payload = task.get("payload", {})
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}

    print(f"\n==================================================")
    print(f"[*] Processing Task: {action} (ID: {task_id})")
    print(f"==================================================")

    report_status(bridge_url, "EXECUTING", {"current_task": action, "task_id": task_id})
    log_message(bridge_url, "TASK_START", f"Started executing task {action}", task_id=task_id)

    success = False
    output_log = ""

    if action in ["SYNC", "SYNC_BUNDLE", "SYNC_ALL"]:
        archive_name = "files.tar.gz"
        tar_url = f"{bridge_url}/files.tar.gz"
        print(f"[*] Fetching sync bundle from {tar_url}...")
        try:
            bytes_received = download_file(tar_url, archive_name)
            print(f"[✓] Downloaded {bytes_received} bytes.")
            cmds = [
                f"tar -xzf {archive_name}",
                f"rm -f {archive_name}",
                "ls -lh results/trials/multitrial_summary.json 2>/dev/null || echo 'Checked results'"
            ]
            success, output_log = execute_shell_commands(cmds)
            if success:
                output_log = f"Downloaded {bytes_received} bytes and extracted successfully.\n" + output_log
        except Exception as e:
            success = False
            output_log = f"Failed to download/extract bundle: {str(e)}"
            print(f"[!] Error: {output_log}")

    elif action in ["GIT_PULL", "PULL"]:
        cmds = ["git status", "git pull --rebase origin main || git pull origin main"]
        success, output_log = execute_shell_commands(cmds)

    elif action in ["EXEC", "SHELL", "RUN_COMMANDS"]:
        commands = payload.get("commands", task.get("commands", []))
        if isinstance(commands, str):
            commands = [commands]
        success, output_log = execute_shell_commands(commands)

    elif action in ["RESET_CLUSTER"]:
        cmds = ["bash infra/reset-cluster.sh"]
        success, output_log = execute_shell_commands(cmds)

    else:
        # Default fallback: check if payload has custom commands
        commands = payload.get("commands", task.get("commands", []))
        if commands:
            if isinstance(commands, str):
                commands = [commands]
            success, output_log = execute_shell_commands(commands)
        else:
            success = False
            output_log = f"Unknown action '{action}' with no executable commands provided."
            print(f"[!] {output_log}")

    final_status = "SUCCESS" if success else "FAILED"
    print(f"[*] Task {task_id} completed with status: {final_status}")
    log_message(
        bridge_url,
        "TASK_COMPLETE",
        f"Task {action} finished with {final_status}",
        task_id=task_id,
        status=final_status,
        output=output_log
    )
    report_status(bridge_url, "IDLE", {"last_completed_task": action, "status": final_status})
    return success

def poll_once(bridge_url):
    try:
        res = http_get(f"{bridge_url}/api/tasks/poll")
        if res.get("has_task") and res.get("task"):
            handle_task(res["task"], bridge_url)
            return True
        else:
            return False
    except urllib.error.URLError as e:
        print(f"[!] Bridge unreachable at {bridge_url}: {e}")
        return False
    except Exception as e:
        print(f"[!] Error while polling bridge: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Antigravity Autonomous Legion Worker")
    parser.add_argument("--bridge", default=DEFAULT_BRIDGE_URL, help=f"Bridge URL (default: {DEFAULT_BRIDGE_URL})")
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in background polling loop")
    parser.add_argument("--interval", type=int, default=3, help="Polling interval in seconds (default: 3)")
    args = parser.parse_args()

    bridge_url = args.bridge.rstrip('/')

    print(f"[*] Antigravity Legion Worker starting...")
    print(f"[*] Target Bridge: {bridge_url}")

    # Register initial presence
    report_status(bridge_url, "ONLINE", {"mode": "daemon" if args.daemon else "once"})
    log_message(bridge_url, "WORKER_ONLINE", "Legion worker initialized and listening for bridge tasks.")

    if args.once or not args.daemon:
        print("[*] Polling bridge for pending tasks...")
        found = poll_once(bridge_url)
        if not found:
            print("[*] No pending tasks in queue. Legion is up to date.")
        sys.exit(0)

    print(f"[*] Running in DAEMON mode (polling every {args.interval}s). Press Ctrl+C to stop.")
    try:
        while True:
            poll_once(bridge_url)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n[*] Legion worker shutting down gracefully...")
        report_status(bridge_url, "OFFLINE")
        log_message(bridge_url, "WORKER_OFFLINE", "Legion worker terminated by user.")

if __name__ == '__main__':
    main()
