# === START OF FILE: namenode/dashboard.py (FIXED + IMPROVED) ===

import logging
import threading
import requests
from flask import Flask, render_template_string, jsonify, request
from waitress import serve
import datetime

# --- HTML Template ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HDFS Clone Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background-color: #f0f2f5; color: #1c1e21; margin: 0; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; background: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h1, h2 { color: #0d6efd; border-bottom: 2px solid #dee2e6; padding-bottom: 10px; }
        .section { margin-bottom: 30px; }
        .datanodes { display: flex; flex-wrap: wrap; gap: 15px; }
        .datanode { padding: 10px 15px; border-radius: 5px; font-weight: 500; color: white; }
        .status-LIVE { background-color: #198754; }
        .status-STALE { background-color: #ffc107; color: black; }
        .status-DEAD { background-color: #dc3545; }
        .file-map { display: grid; gap: 20px; }
        .file-entry { border: 1px solid #ccc; border-radius: 5px; padding: 15px; }
        .file-path { font-size: 1.2em; font-weight: bold; margin-bottom: 10px; word-break: break-all; }
        .blocks-container { display: flex; flex-direction: column; gap: 10px; }
        .block-entry { background-color: #e9ecef; padding: 10px; border-radius: 5px; }
        .block-id { font-family: monospace; }
        .locations { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
        .location { padding: 3px 8px; border-radius: 4px; font-size: 0.9em; }
        footer { text-align: center; margin-top: 20px; font-size: 0.9em; color: #6c757d; }
    </style>
</head>
<body>
    <div class="container">
        <h1>HDFS Clone Dashboard</h1>
        <div class="section">
            <h2>Datanode Status</h2>
            <div id="datanode-status" class="datanodes"></div>
        </div>
        <div class="section">
            <h2>File & Block Map</h2>
            <div id="file-map" class="file-map"></div>
        </div>
    </div>
    <footer>Dashboard auto-refreshes every 5 seconds.</footer>
    <script>
        function renderDashboard(data) {
            const dnStatusContainer = document.getElementById('datanode-status');
            dnStatusContainer.innerHTML = '';
            const datanodes = data.datanodes || {};
            if (Object.keys(datanodes).length === 0) {
                dnStatusContainer.innerHTML = '<p style="color: gray;">No DataNodes connected.</p>';
            }
            for (const [dnId, info] of Object.entries(datanodes)) {
                const dnDiv = document.createElement('div');
                dnDiv.className = `datanode status-${info.status}`;
                const lastSeen = info.last_heartbeat || '—';
                const ip = info.ip || '';
                dnDiv.textContent = `${dnId} (${info.status}) ${ip ? '- ' + ip : ''} ${lastSeen !== '—' ? '• Last: ' + lastSeen : ''}`;
                dnStatusContainer.appendChild(dnDiv);
            }

            const fileMapContainer = document.getElementById('file-map');
            fileMapContainer.innerHTML = '';
            if (data.files.length === 0) {
                fileMapContainer.innerHTML = '<p>No files found in the filesystem.</p>';
            }
            data.files.forEach(file => {
                const fileDiv = document.createElement('div');
                fileDiv.className = 'file-entry';
                let blocksHtml = '<div class="blocks-container">';
                if (file.blocks.length === 0) {
                    blocksHtml += '<span>No blocks allocated for this file.</span>';
                }
                file.blocks.forEach(block => {
                    let locationsHtml = '';
                    block.locations.forEach(loc => {
                        const status = data.datanodes[loc]?.status || 'UNKNOWN';
                        locationsHtml += `<span class="location status-${status}">${loc}</span>`;
                    });
                    blocksHtml += `<div class="block-entry"><span class="block-id">${block.block_id}</span><div class="locations">${locationsHtml}</div></div>`;
                });
                blocksHtml += '</div>';
                fileDiv.innerHTML = `<div class="file-path">${file.path}</div>${blocksHtml}`;
                fileMapContainer.appendChild(fileDiv);
            });
        }

        async function fetchData() {
            try {
                const response = await fetch('/api/filesystem-state');
                if (!response.ok) throw new Error('Network response was not ok');
                const data = await response.json();
                renderDashboard(data);
            } catch (error) {
                console.error('Failed to fetch dashboard data:', error);
                document.getElementById('file-map').innerHTML = '<p style="color: red;">Error fetching data from NameNode.</p>';
            }
        }

        fetchData();
        setInterval(fetchData, 5000);
    </script>
</body>
</html>
"""

class Dashboard:
    """A Flask web server that provides a dashboard for the HDFS clone."""

    def __init__(self, namenode, host='127.0.0.1', port=5001):
        self.namenode = namenode
        self.host = host
        self.port = port
        self.logger = logging.getLogger("hdfs_clone.dashboard")
        self.flask_app = Flask(__name__)
        self.thread = None
        
        self.flask_app.add_url_rule('/', 'index', self.index)
        self.flask_app.add_url_rule('/api/filesystem-state', 'get_state', self.get_filesystem_state)

    def index(self):
        return render_template_string(HTML_TEMPLATE)

    def get_filesystem_state(self):
        """Return live filesystem + datanode status JSON for dashboard frontend."""
        with self.namenode.namespace_manager.lock, self.namenode.block_manager.lock:
            # --- FILES ---
            files_data = []
            for path, meta in sorted(self.namenode.namespace_manager.namespace.get('files', {}).items()):
                blocks = [
                    {"block_id": b_id, "locations": sorted(self.namenode.block_manager.get_block_locations(b_id))}
                    for b_id in meta.get('blocks', [])
                ]
                files_data.append({
                    "path": path,
                    "size": meta.get('file_size', 0),
                    "replication": meta.get('replication', 0),
                    "blocks": blocks
                })

            # --- DATANODES ---
            datanodes_source = getattr(self.namenode, "datanodes", {})
            datanodes_data = {}
            for dn_id, info in datanodes_source.items():
                datanodes_data[dn_id] = {
                    "status": info.get("status", "UNKNOWN"),
                    "ip": info.get("ip", ""),  # Optional
                    "last_heartbeat": info.get("last_heartbeat", None)
                }
                # Convert timestamp if needed
                if datanodes_data[dn_id]["last_heartbeat"]:
                    try:
                        ts = datanodes_data[dn_id]["last_heartbeat"]
                        if isinstance(ts, (int, float)):
                            datanodes_data[dn_id]["last_heartbeat"] = datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")
                    except Exception:
                        datanodes_data[dn_id]["last_heartbeat"] = "—"

        return jsonify({"files": files_data, "datanodes": datanodes_data})

    def _run_server(self):
        """Runs the waitress server, which is silent by default."""
        self.logger.info(f"Starting dashboard server on http://{self.host}:{self.port}")
        serve(self.flask_app, host=self.host, port=self.port, _quiet=True)

    def start(self):
        """Starts the Flask/Waitress server in a background thread."""
        self.logger.info("Starting dashboard thread...")
        self.thread = threading.Thread(target=self._run_server, name="DashboardThread")
        self.thread.daemon = True
        self.thread.start()
        
        self.logger.info("=" * 60)
        self.logger.info("DASHBOARD IS LIVE!")
        self.logger.info(f"  * Local URL:  http://{self.host}:{self.port}")
        self.logger.info("=" * 60)
        print("\n--- Dashboard is running at http://%s:%s ---" % (self.host, self.port))

    def stop(self):
        """Stops the dashboard server (no-op, daemon thread exits automatically)."""
        self.logger.info("Dashboard thread will terminate on application exit.")

# === END OF FILE: namenode/dashboard.py (FIXED + IMPROVED) ===
