# === START OF FILE: common/discovery.py ===

import socket
import logging
import time
import threading

BROADCAST_PORT = 60000
BROADCAST_MAGIC_MESSAGE = "TEAM_88_HDFS_SERVER_HERE"
SERVER_TCP_PORT = 60001

def broadcast_presence(stop_event: threading.Event):
    """Periodically broadcasts a UDP message for clients to discover the server."""
    logger = logging.getLogger("hdfs_clone.discovery")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    logger.info(f"Starting UDP broadcast on port {BROADCAST_PORT}...")
    while not stop_event.is_set():
        sock.sendto(BROADCAST_MAGIC_MESSAGE.encode('utf-8'), ('<broadcast>', BROADCAST_PORT))
        logger.debug("Sent discovery broadcast.")
        # --- CHANGE: Increased frequency from 5 to 2 seconds ---
        time.sleep(2)
    sock.close()
    logger.info("UDP broadcast stopped.")

def listen_for_server() -> str:
    """Listens for the server's broadcast to find its IP address."""
    logger = logging.getLogger("hdfs_clone.discovery")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('', BROADCAST_PORT))
    logger.info("Listening for server broadcast ('Team 88')...")
    print("Searching for 'Team 88' server on the local network...")
    while True:
        data, addr = sock.recvfrom(1024)
        if data.decode('utf-8') == BROADCAST_MAGIC_MESSAGE:
            server_ip = addr[0]
            logger.info(f"Discovered server at IP: {server_ip}")
            print(f"Found server at {server_ip}!")
            sock.close()
            return server_ip

# === END OF FILE: common/discovery.py ===