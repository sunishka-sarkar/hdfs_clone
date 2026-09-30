# === client.py (final version) ===
import socket
import json
import logging
import time
import sys
import os
from xmlrpc.client import ServerProxy

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common.utils import setup_logging, load_config
from common.discovery import listen_for_server, SERVER_TCP_PORT
from namenode.namenode import NameNode
from datanode.datanode import DataNode


def run_client():
    username = input("Enter a name for this client machine: ").strip()
    setup_logging("logs/client_startup.log", level=logging.INFO, force_reconfigure=True)
    logger = logging.getLogger("hdfs_clone.client_node")

    print("Listening for broadcast from coordinator...")
    server_ip = listen_for_server()
    print(f"Found coordinator at {server_ip}. Connecting...")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(30)

    try:
        client_socket.connect((server_ip, SERVER_TCP_PORT))
        client_socket.sendall(username.encode("utf-8"))
        print(f"Connected to coordinator '{server_ip}'. Waiting for role assignment...")

        # === Wait for ACK handshake ===
        try:
            ack = client_socket.recv(3)
            if ack == b"ACK":
                print("[INFO] Coordinator acknowledged connection.")
            else:
                print("[WARN] No ACK from coordinator, continuing anyway.")
        except socket.timeout:
            print("[WARN] No ACK within timeout, continuing anyway.")

        # === Wait for cluster assignment payload ===
        data_buffer = b""
        client_socket.settimeout(90)  # allow time for RPC to start
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            data_buffer += chunk
            try:
                payload = json.loads(data_buffer.decode("utf-8"))
                break
            except json.JSONDecodeError:
                continue

        if not data_buffer:
            print("❌ No cluster payload received. Exiting.")
            return

        payload = json.loads(data_buffer.decode("utf-8"))
    except Exception as e:
        print(f"❌ Connection/coordination error: {e}")
        return
    finally:
        try:
            client_socket.close()
        except Exception:
            pass

    role = payload.get("role")
    cluster_map = payload.get("cluster_map", {})
    datanode_id = payload.get("datanode_id")

    print(f"\n✅ ROLE ASSIGNED: {role}")
    print(f"Cluster Map:\n{json.dumps(cluster_map, indent=2)}")

    config = load_config()
    namenode_ip = cluster_map.get("namenode_ip")
    namenode_port = config["namenode"].get("port", 9000)
    rpc_url = f"http://{namenode_ip}:{namenode_port}"

    my_ip = socket.gethostbyname(socket.gethostname())
    print(f"[INFO] Local IP: {my_ip}")

    # === Start local service ===
    if role == "NameNode":
        print(f"🚀 Launching NameNode service at {my_ip} ...")
        node_service = NameNode(config, my_ip)
        node_service.start()
        print("[INFO] NameNode RPC started.")
    elif role == "DataNode":
        print(f"🚀 Launching DataNode {datanode_id} (connecting to {rpc_url})")
        namenode_rpc = ServerProxy(rpc_url, allow_none=True)
        node_service = DataNode(datanode_id, config, namenode_rpc, {})
        node_service.start()
    else:
        print("❌ Unknown role. Exiting.")
        return

    print(f"{role} service running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping {role}...")
        try:
            node_service.stop()
        except Exception:
            pass


if __name__ == "__main__":
    run_client()
