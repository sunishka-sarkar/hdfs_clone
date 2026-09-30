# === START OF FILE: frontend/datanode_dashboard.py ===
import logging
import threading
from flask import Flask, jsonify, render_template_string
from waitress import serve
import datetime

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Cluster DataNode Monitor</title>
<style>
  body { font-family: Arial, sans-serif; background: #f5f6fa; color: #333; margin: 0; padding: 20px; }
  h1 { color: #007bff; }
  .node { background: #fff; border-radius: 6px; padding: 12px 16px; margin-bottom: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
  .LIVE { border-left: 5px solid #28a745; }
  .DEAD { border-left: 5px solid #dc3545; }
  .STALE { border-left: 5px solid #ffc107; }
  .chunk { background: #e9ecef; padding: 4px 8px; border-radius: 3px; margin-right: 5px; display: inline-block; }
  footer { text-align: center; font-size: 0.8em; color: #777; margin-top: 20px; }
</style>
</head>
<body>
<h1>Active DataNodes</h1>
<div id="nodes"></div>
<footer>Auto-refreshing every 5 seconds</footer>
<script>
async function fetchData() {
  try {
    const res = await fetch('/api/datanodes');
    const data = await res.json();
    const container = document.getElementById('nodes');
    container.innerHTML = '';
    Object.entries(data.datanodes).forEach(([dn, info]) => {
      const div = document.createElement('div');
      div.className = 'node ' + info.status;
      const hb = info.last_heartbeat || '—';
      const chunks = (info.chunks || []).map(c => `<span class='chunk'>${c}</span>`).join('');
      div.innerHTML = `<b>${dn}</b> (${info.ip}) - <b>${info.status}</b><br>
                       Last heartbeat: ${hb}<br>
                       Chunks: ${chunks || 'None'}`;
      container.appendChild(div);
    });
  } catch (err) {
    console.error('Error fetching datanode info:', err);
  }
}
fetchData();
setInterval(fetchData, 5000);
</script>
</body>
</html>
"""

class DatanodeDashboard:
    def __init__(self, namenode_proxy, host="127.0.0.1", port=5050):
        self.namenode = namenode_proxy
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self.logger = logging.getLogger("hdfs_clone.datanode_dashboard")
        self.thread = None

        self.app.add_url_rule("/", "index", self.index)
        self.app.add_url_rule("/api/datanodes", "api_datanodes", self.api_datanodes)

    def index(self):
        return render_template_string(HTML_TEMPLATE)

    def api_datanodes(self):
        data = {"datanodes": {}}
        for dn_id, info in getattr(self.namenode, "datanodes", {}).items():
            entry = {
                "status": info.get("status", "UNKNOWN"),
                "ip": info.get("ip", ""),
                "last_heartbeat": info.get("last_heartbeat", ""),
                "chunks": info.get("chunks", [])
            }
            if isinstance(entry["last_heartbeat"], (int, float)):
                entry["last_heartbeat"] = datetime.datetime.fromtimestamp(
                    entry["last_heartbeat"]
                ).strftime("%H:%M:%S")
            data["datanodes"][dn_id] = entry
        return jsonify(data)

    def _run_server(self):
        self.logger.info(f"Starting Datanode dashboard on http://{self.host}:{self.port}")
        serve(self.app, host=self.host, port=self.port, _quiet=True)

    def start(self):
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()
        print(f"--- Datanode Dashboard is live at http://{self.host}:{self.port} ---")

# === END OF FILE: frontend/datanode_dashboard.py ===
