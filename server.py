# === server.py (final stable version: first client becomes NameNode) ===
import socket
import threading
import json
import logging
import time
import sys
import os
from typing import Dict

# Ensure project root in path
sys.path.append(os.path.dirname(os.path.abspath(_file_)))

from common.utils import setup_logging, load_config
from common.discovery import broadcast_presence, SERVER_TCP_PORT
from namenode.namenode import NameNode
from datanode.datanode import DataNode
from secondary.checkpoint import SecondaryNameNode


def get_local_ip():
    """Get the local LAN IP (not 127.0.0.1)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return socket.gethostbyname(socket.gethostname())


def run_server():
    os.makedirs("logs", exist_ok=True)
    setup_logging("logs/server_coordinator.log", level=logging.INFO, force_reconfigure=True)
    logger = logging.getLogger("hdfs_clone.coordinator")

    username = input("Enter a name for this server machine (e.g., Server-GUI): ").strip()

    stop_broadcast = threading.Event()
    broadcast_thread = threading.Thread(target=broadcast_presence, args=(stop_broadcast,), daemon=True)
    broadcast_thread.start()

    connected_clients: Dict[str, Dict] = {}
    connection_order = []  # to track who joined first
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", SERVER_TCP_PORT))
    server_socket.listen(5)

    expected_clients = 3
    print(f"Server '{username}' broadcasting as 'Team 88' — waiting for {expected_clients} clients...")

    while len(connected_clients) < expected_clients:
        conn, addr = server_socket.accept()
        client_ip = addr[0]
        try:
            client_user = conn.recv(1024).decode("utf-8")
            if client_ip not in connected_clients:
                connected_clients[client_ip] = {"conn": conn, "user": client_user}
                connection_order.append(client_ip)  # record who joined first
                print(f"--> Client connected: '{client_user}' from {client_ip} ({len(connected_clients)}/{expected_clients})")
        except Exception as e:
            logger.warning(f"Error with client {client_ip}: {e}")
            conn.close()

    stop_broadcast.set()
    broadcast_thread.join(timeout=2)
    server_socket.close()

    print("\nAll clients connected. Assigning roles...")

    # === Assign roles based on join order ===
    namenode_ip = connection_order[0]
    datanode_ips = connection_order[1:]
    my_ip = get_local_ip()

    cluster_map = {
        "namenode_ip": namenode_ip,
        "secondary_namenode_ip": my_ip,
        "datanode_ips": {f"dn{i+1}": ip for i, ip in enumerate(datanode_ips)},
    }

    print("\n--- ROLE ASSIGNMENT COMPLETE ---")
    print(f"NameNode:           '{connected_clients[namenode_ip]['user']}' at {namenode_ip}")
    print(f"Secondary NameNode: '{username}' at {my_ip}")
    for i, ip in enumerate(datanode_ips):
        print(f"DataNode dn{i+1}:   '{connected_clients[ip]['user']}' at {ip}")
    print("-------------------------------------------")

    # === Start local services ===
    print("\nStarting local Secondary NameNode and Dashboard...")
    config = load_config()
    namenode = NameNode(config, my_ip)
    secondary_nn = SecondaryNameNode(config, namenode)

    try:
        namenode.start()
        secondary_nn.start()
    except Exception as e:
        logger.exception("Failed to start NameNode services: %s", e)
        print("❌ Error starting NameNode services:", e)
        return

    # === Send cluster map and roles to clients ===
    for ip, client in connected_clients.items():
        role = "NameNode" if ip == namenode_ip else "DataNode"
        datanode_id = next((k for k, v in cluster_map["datanode_ips"].items() if v == ip), None)
        payload = {
            "role": role,
            "cluster_map": cluster_map,
            "datanode_id": datanode_id,
        }
        try:
            client["conn"].sendall(json.dumps(payload).encode("utf-8"))
        except Exception as e:
            logger.warning(f"Failed to send payload to {ip}: {e}")
        finally:
            try:
                client["conn"].shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            client["conn"].close()

    # === Print dashboard link ===
    dashboard_port = config.get("dashboard", {}).get("port", 5001)
    print("\n=======================================================")
    print(f"  FRONTEND IS LIVE at http://{my_ip}:{dashboard_port}")
    print("=======================================================")

    # === Heartbeat log stream (console view) ===
    def stream_heartbeat_logs(log_file="logs/server_coordinator.log"):
        print("\n--- Live Heartbeat Log Stream ---")
        try:
            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                f.seek(0, os.SEEK_END)
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(1)
                        continue
                    if "Heartbeat" in line or "heartbeat" in line:
                        print(line.strip())
        except FileNotFoundError:
            print(f"No log file found at {log_file}")

    threading.Thread(target=stream_heartbeat_logs, daemon=True).start()

    print("\nCluster running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        try:
            secondary_nn.stop()
        except Exception:
            pass
        try:
            namenode.stop()
        except Exception:
            pass
        print("✅ Cluster shut down cleanly.")


if _name_ == "_main_":
    run_server()