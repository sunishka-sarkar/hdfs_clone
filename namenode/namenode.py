# === START OF FILE: namenode/namenode.py ===

import logging
import threading
import time
from typing import Dict, Any, List
from xmlrpc.server import SimpleXMLRPCServer

from .fsimage import FsImageManager
from .editlog import EditLogManager
from .block_manager import BlockManager
from .namespace import NamespaceManager
from .replication_manager import ReplicationManager
from .dashboard import Dashboard
from common.constants import HEARTBEAT_INTERVAL
from common.exceptions import SafeModeException

class NameNode:
    """The master server that manages the filesystem namespace and data blocks."""

    def __init__(self, config: Dict[str, Any], host_ip: str):
        self.config = config
        self.host_ip = host_ip # The actual IP of this machine
        self.logger = logging.getLogger("hdfs_clone.namenode")
        self.metadata_logger = logging.getLogger("hdfs_clone.metadata") # Dedicated metadata logger
        
        self.in_safe_mode = True
        self.reported_blocks = set()
        self.safemode_threshold = self.config['namenode'].get('safemode_threshold', 0.999)
        self.fsimage_manager = FsImageManager(self.config["namenode"]["data_dir"])
        self.editlog_manager = EditLogManager(self.config["namenode"]["data_dir"])
        self.block_manager = BlockManager()
        self.block_manager.load_topology()
        self.namespace_manager = NamespaceManager(self.fsimage_manager, self.editlog_manager, self.block_manager, self, self.config)
        self.replication_manager = ReplicationManager(self, self.namespace_manager, self.block_manager)
        self.dashboard = Dashboard(self) 
        self._stop_event = threading.Event()
        self.health_check_thread = None
        
        # --- RPC Server Setup ---
        self.rpc_server = None
        self.rpc_thread = None
        rpc_port = self.config['namenode'].get('port', 9000)
        # Listen on all interfaces (0.0.0.0) so other machines can connect
        self.rpc_server = SimpleXMLRPCServer(('0.0.0.0', rpc_port), logRequests=False, allow_none=True)
        self.rpc_server.register_instance(self)
        self.logger.info(f"RPC server will listen on port {rpc_port}")

    def start(self):
        """Starts the NameNode services."""
        self.logger.info("Starting NameNode..."); self.logger.warning("="*50); self.logger.warning("NameNode is in SAFE MODE. Writes disabled."); self.logger.warning("="*50)
        self.in_safe_mode = True
        self.namespace_manager.load()
        self.health_check_thread = threading.Thread(target=self._datanode_health_checker, name="DataNodeHealthChecker"); self.health_check_thread.start()
        self.replication_manager.start()
        
        self.rpc_thread = threading.Thread(target=self.rpc_server.serve_forever, name="RPCServerThread"); self.rpc_thread.daemon = True; self.rpc_thread.start()

        self.dashboard.start()
        self.logger.info("NameNode started successfully.")

    def stop(self):
        """Stops the NameNode services gracefully."""
        self.logger.info("Stopping NameNode..."); self._stop_event.set()
        
        if self.rpc_server: self.rpc_server.shutdown(); self.rpc_server.server_close()
        
        self.dashboard.stop()
        self.replication_manager.stop()
        if self.health_check_thread: self.health_check_thread.join()
        self.logger.info("NameNode stopped.")

    # All public methods below are automatically exposed via RPC
    # We will add metadata logging to all write operations
    def mkdir(self, path: str, permissions: str, user: str, group: str):
        result = self.namespace_manager.mkdir(path, permissions, user, group)
        self.metadata_logger.info(f"MKDIR by {user}:{group} on {path}")
        return result
    def create(self, path: str, replication: int, block_size: int, permissions: str, user: str, group: str):
        result = self.namespace_manager.create(path, replication, block_size, permissions, user, group)
        self.metadata_logger.info(f"CREATE by {user}:{group} on {path}")
        return result
    def delete(self, path: str, user: str, group: str, recursive: bool):
        result = self.namespace_manager.delete(path, user, group, recursive)
        self.metadata_logger.info(f"DELETE by {user}:{group} on {path}")
        return result
    def rename(self, src_path: str, dst_path: str, user: str, group: str):
        result = self.namespace_manager.rename(src_path, dst_path, user, group)
        self.metadata_logger.info(f"RENAME by {user}:{group} from {src_path} to {dst_path}")
        return result
    def chown(self, path: str, owner: str, group: str, user: str):
        result = self.namespace_manager.set_owner(path, owner, group, user)
        self.metadata_logger.info(f"CHOWN by {user} on {path} to {owner}:{group}")
        return result
    def chmod(self, path: str, mode: str, user: str):
        result = self.namespace_manager.set_permissions(path, mode, user)
        self.metadata_logger.info(f"CHMOD by {user} on {path} to {mode}")
        return result
    
    # ... (rest of the methods are unchanged, but will be called via RPC) ...
    def _datanode_health_checker(self):
        while not self._stop_event.is_set():
            try: self.block_manager.check_datanode_health()
            except Exception as e: self.logger.error(f"Error in health checker: {e}", exc_info=True)
            self._stop_event.wait(HEARTBEAT_INTERVAL * 2)
    def _check_safe_mode_exit(self):
        if not self.in_safe_mode: return
        total_blocks = self.namespace_manager.get_total_block_count()
        if total_blocks == 0: self.leave_safe_mode(); return
        reported_ratio = len(self.reported_blocks) / total_blocks
        self.logger.info(f"Safe Mode: {len(self.reported_blocks)}/{total_blocks} blocks reported ({reported_ratio:.2%})")
        if reported_ratio >= self.safemode_threshold: self.leave_safe_mode()
    def leave_safe_mode(self):
        if not self.in_safe_mode: return
        self.in_safe_mode = False; self.logger.info("="*50); self.logger.info("NameNode LEAVING SAFE MODE. Writes enabled."); self.logger.info("="*50)
    def register_datanode(self, datanode_id: str): return self.block_manager.register_datanode(datanode_id)
    def send_heartbeat(self, datanode_id: str):
        self.block_manager.process_heartbeat(datanode_id)
        if self.in_safe_mode: return []
        return self.replication_manager.get_datanode_commands(datanode_id)
        # === File Upload / Download APIs (New) ===
    def upload_file(self, remote_path: str, data_hex: str) -> str:
        """Uploads a file from client. Splits into blocks, stores on DataNodes."""
        import math, os, binascii
        data = bytes.fromhex(data_hex)
        block_size = self.config["namenode"].get("block_size", 4 * 1024 * 1024)  # default 4MB
        total_blocks = math.ceil(len(data) / block_size)
        datanode_report = self.block_manager.get_datanode_report()

        if not datanode_report:
            return "❌ No DataNodes available"

        block_locations = {}
        for i in range(total_blocks):
            block_id = f"blk_{int(time.time())}_{i}"
            start = i * block_size
            end = start + block_size
            block_data = data[start:end]

            # choose first N DataNodes for replication
            dn_ids = list(datanode_report.keys())[: self.config["namenode"].get("replication", 2)]
            for dn_id in dn_ids:
                dn_ip = datanode_report[dn_id]["ip"]
                dn_port = self.config["datanode"].get("rpc_port", 9100)
                from xmlrpc.client import ServerProxy
                try:
                    dn_proxy = ServerProxy(f"http://{dn_ip}:{dn_port}", allow_none=True)
                    dn_proxy.store_block(block_id, binascii.hexlify(block_data).decode())
                    self.block_manager.add_block_location(block_id, dn_id)
                except Exception as e:
                    self.logger.error(f"Failed to send block {block_id} to {dn_id}: {e}")
                    continue

            block_locations[block_id] = dn_ids
            self.logger.info(f"Uploaded block {block_id} -> {dn_ids}")

        self.namespace_manager.create(remote_path, replication=2, block_size=block_size,
                                      permissions="rw-r--r--", user="system", group="system")
        self.namespace_manager.complete_file(remote_path, len(data), user="system", group="system")

        return f"✅ File '{remote_path}' uploaded with {total_blocks} block(s)."

    def download_file(self, remote_path: str) -> str:
        """Downloads a file from the cluster, reassembling blocks."""
        import binascii
        try:
            block_map = self.namespace_manager.get_block_locations_for_file(remote_path)
        except Exception as e:
            return f"❌ Error: {e}"

        datanode_report = self.block_manager.get_datanode_report()
        file_bytes = b""

        for blk_id, dn_list in block_map.items():
            found = False
            for dn_id in dn_list:
                dn_info = datanode_report.get(dn_id)
                if not dn_info:
                    continue
                dn_ip = dn_info["ip"]
                dn_port = self.config["datanode"].get("rpc_port", 9100)
                from xmlrpc.client import ServerProxy
                try:
                    dn_proxy = ServerProxy(f"http://{dn_ip}:{dn_port}", allow_none=True)
                    blk_hex = dn_proxy.fetch_block(blk_id)
                    if blk_hex:
                        file_bytes += bytes.fromhex(blk_hex)
                        found = True
                        break
                except Exception as e:
                    self.logger.warning(f"Failed fetching block {blk_id} from {dn_id}: {e}")
                    continue
            if not found:
                self.logger.error(f"❌ Missing block {blk_id} for {remote_path}")

        return binascii.hexlify(file_bytes).decode()

    def block_report(self, datanode_id: str, blocks: List[str]):
        self.block_manager.process_block_report(datanode_id, blocks)
        if self.in_safe_mode: self.reported_blocks.update(blocks); self._check_safe_mode_exit()
    def block_received(self, datanode_id: str, block_id: str): self.block_manager.add_block_location(block_id, datanode_id)
    def add_block(self, path: str, user: str, group: str): return self.namespace_manager.add_block_to_file(path, user, group)
    def get_block_locations(self, path: str, user: str, group: str): return self.namespace_manager.get_block_locations_for_file(path, user, group)
    def get_file_info(self, path: str, user: str, group: str): return self.namespace_manager.get_file_info(path, user, group)
    def list_status(self, path: str, user: str, group: str): return self.namespace_manager.list_status(path, user, group)
    def complete_file(self, path: str, file_size: int, user: str, group: str): return self.namespace_manager.complete_file(path, file_size, user, group)
    def set_quota(self, path: str, name_quota: int, space_quota: int, user: str): return self.namespace_manager.set_quota(path, name_quota, space_quota, user)
    def get_content_summary(self, path: str, user: str, group: str): return self.namespace_manager.get_content_summary(path, user, group)
    def get_datanode_report(self): return self.block_manager.get_datanode_report()
    def get_safe_mode_status(self): return "ON" if self.in_safe_mode else "OFF"

# === END OF FILE: namenode/namenode.py ===