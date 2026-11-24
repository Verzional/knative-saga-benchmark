#!/usr/bin/env python3
"""
Bidirectional Agent Communication Bridge for Antigravity (MacBook <-> Legion)
Enables autonomous coordination, task dispatching, status checks, and data synchronization.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIRECTIVE_PATH = os.path.join(BASE_DIR, "docs", "LEGION_AGENT_DIRECTIVE.md")

STATE = {
    "device": "macbook",
    "status": "ONLINE",
    "target_peer": "http://192.168.10.10:8088",
    "macbook_state": "IDLE",
    "legion_state": "UNKNOWN",
    "active_architecture": "NONE",
    "last_updated": time.time(),
    "tasks_queue": [],
    "task_history": [],
    "messages": []
}

SYNC_SCRIPT_TEMPLATE = """#!/usr/bin/env bash
set -e
BRIDGE_HOST="192.168.10.2:8088"
echo "=========================================================="
echo "[*] Antigravity Bridge Client: Synchronizing with MacBook"
echo "=========================================================="

curl -s -X POST "http://${BRIDGE_HOST}/api/status" \\
  -H "Content-Type: application/json" \\
  -d '{"legion_state": "SYNCING", "device": "legion"}' >/dev/null 2>&1 || true

echo "[+] Downloading files.tar.gz from http://${BRIDGE_HOST}/files.tar.gz..."
curl -# "http://${BRIDGE_HOST}/files.tar.gz" -o files.tar.gz

echo "[+] Extracting files into working directory..."
tar -xzf files.tar.gz
rm -f files.tar.gz

echo "[+] Verifying updated files..."
if [ -f "results/trials/multitrial_summary.json" ]; then
    echo "[✓] results/trials/multitrial_summary.json found ($(wc -c < results/trials/multitrial_summary.json) bytes)"
fi

curl -s -X POST "http://${BRIDGE_HOST}/api/status" \\
  -H "Content-Type: application/json" \\
  -d '{"legion_state": "SYNCED", "device": "legion", "last_synced": '$(date +%s)'}' >/dev/null 2>&1 || true

curl -s -X POST "http://${BRIDGE_HOST}/api/messages" \\
  -H "Content-Type: application/json" \\
  -d '{"source": "legion", "event_type": "SYNC_SUCCESS", "message": "Legion synchronized via sync.sh"}' >/dev/null 2>&1 || true

echo "=========================================================="
echo "[✓] Legion SUT is fully synchronized with MacBook!"
echo "=========================================================="
"""

class BridgeHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, data):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, HEAD')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))

    def do_OPTIONS(self):
        self._send_json(200, {"status": "ok"})

    def do_HEAD(self):
        if self.path in ['/api/sync', '/files.tar.gz']:
            if os.path.exists('files.tar.gz'):
                self.send_response(200)
                self.send_header('Content-Type', 'application/gzip')
                self.send_header('Content-Length', str(os.path.getsize('files.tar.gz')))
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self._send_json(200, {"status": "ok", "device": STATE["device"], "timestamp": time.time()})
        elif self.path == '/api/status':
            self._send_json(200, STATE)
        elif self.path == '/api/tasks':
            self._send_json(200, {"tasks": STATE["tasks_queue"], "count": len(STATE["tasks_queue"])})
        elif self.path.startswith('/api/tasks/poll'):
            if STATE["tasks_queue"]:
                task = STATE["tasks_queue"].pop(0)
                task["polled_at"] = time.time()
                STATE["task_history"].append(task)
                self._send_json(200, {"has_task": True, "task": task})
            else:
                self._send_json(200, {"has_task": False, "task": None})
        elif self.path in ['/api/directive', '/directive']:
            if os.path.exists(DIRECTIVE_PATH):
                self.send_response(200)
                self.send_header('Content-Type', 'text/markdown; charset=utf-8')
                with open(DIRECTIVE_PATH, 'rb') as f:
                    content = f.read()
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                self.wfile.write(content)
            else:
                self._send_json(404, {"error": "Directive file not found"})
        elif self.path == '/sync.sh' or self.path == '/api/sync/exec':
            self.send_response(200)
            self.send_header('Content-Type', 'text/x-shellscript')
            self.send_header('Content-Length', str(len(SYNC_SCRIPT_TEMPLATE)))
            self.end_headers()
            self.wfile.write(SYNC_SCRIPT_TEMPLATE.encode('utf-8'))
        elif self.path in ['/api/sync', '/files.tar.gz']:
            if os.path.exists('files.tar.gz'):
                self.send_response(200)
                self.send_header('Content-Type', 'application/gzip')
                self.send_header('Content-Disposition', 'attachment; filename="files.tar.gz"')
                with open('files.tar.gz', 'rb') as tf:
                    data = tf.read()
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self._send_json(404, {'error': 'files.tar.gz not found'})
        elif self.path == '/api/messages':
            self._send_json(200, {"messages": STATE["messages"]})
        else:
            self._send_json(404, {"error": "Endpoint not found"})

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length) if content_length > 0 else b'{}'
        try:
            payload = json.loads(body.decode('utf-8'))
        except Exception as e:
            self._send_json(400, {"error": f"Invalid JSON: {str(e)}"})
            return

        if self.path == '/api/status':
            for k, v in payload.items():
                STATE[k] = v
            STATE["last_updated"] = time.time()
            self._send_json(200, {"status": "updated", "state": STATE})
        elif self.path == '/api/tasks/clear':
            STATE["tasks_queue"] = []
            self._send_json(200, {"status": "cleared", "queue_len": 0})
        elif self.path == '/api/tasks/dispatch':
            payload["dispatch_time"] = time.time()
            if "task_id" not in payload:
                payload["task_id"] = f"task-{int(time.time())}"
            if "status" not in payload:
                payload["status"] = "QUEUED"
            STATE["tasks_queue"].append(payload)
            self._send_json(200, {
                "status": "queued",
                "task_id": payload["task_id"],
                "queue_len": len(STATE["tasks_queue"])
            })
        elif self.path == '/api/messages':
            payload["timestamp"] = time.time()
            STATE["messages"].append(payload)
            self._send_json(200, {"status": "message_logged"})
        else:
            self._send_json(404, {"error": "Endpoint not found"})

    def log_message(self, format, *args):
        sys.stderr.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {args[0]} {args[1]} {args[2]}\n")

def run(port=8088):
    server = HTTPServer(('0.0.0.0', port), BridgeHandler)
    print(f"[*] Antigravity Communication Bridge running on port {port}...")
    server.serve_forever()

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8088
    run(port)
