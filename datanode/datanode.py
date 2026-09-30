# datanode/datanode.py
"""
Contains the main DataNode class, the worker nodes that store data blocks.
Now capable of executing replication commands from the NameNode.
"""
import logging
import threading
import time
from typing import Dict, Any, TYPE_CHECKING

from .block_storage import BlockStorage
# --- FIX: Added missing import for the constant ---
from common.constants import HEARTBEAT_INTERVAL
from common.exceptions import HDFSCloneException

# Use a forward reference for type hinting to avoid circular imports
if TYPE_CHECKING:
    from namenode.namenode import NameNode

class DataNode:
    """A worker node that stores and serves data blocks."""

    def __init__(self, datanode_id: str, config: Dict[str, Any], namenode: 'NameNode', datanodes_map: Dict[str, 'DataNode']):
        self.datanode_id = datanode_id
        self.config = config
        self.namenode = namenode
        self.datanodes_map = datanodes_map # Gives DN access to the full map to find other DNs
        
        self.logger = logging.getLogger(f"hdfs_clone.datanode.{self.datanode_id}")
        
        datanode_base_dir = self.config["datanode"]["base_dir"]
        self.storage = BlockStorage(self.datanode_id, datanode_base_dir)

        self._stop_event = threading.Event()
        self.heartbeat_thread = None

    def start(self):
        """Starts the DataNode services and RPC server for block uploads/downloads."""
        from xmlrpc.server import SimpleXMLRPCServer
        self.logger.info(f"Starting DataNode '{self.datanode_id}'...")

        # Register DataNode with the NameNode
        self.namenode.register_datanode(self.datanode_id)
        self._send_block_report()

        # Start DataNode RPC Server
        rpc_port = self.config["datanode"].get("rpc_port", 9100)
        self.rpc_server = SimpleXMLRPCServer(("0.0.0.0", rpc_port), allow_none=True, logRequests=False)
        self.rpc_server.register_instance(self)
        threading.Thread(target=self.rpc_server.serve_forever, daemon=True).start()
        self.logger.info(f"DataNode RPC server started on port {rpc_port}")

        # Start heartbeat loop
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_sender, name=f"HeartbeatSender-{self.datanode_id}"
        )
        self.heartbeat_thread.start()

        self.logger.info(f"DataNode '{self.datanode_id}' fully operational.")


    def stop(self):
        """Stops the DataNode services gracefully."""
        self.logger.info(f"Stopping DataNode '{self.datanode_id}'...")
        self._stop_event.set()
        if self.heartbeat_thread:
            self.heartbeat_thread.join()
        self.logger.info(f"DataNode '{self.datanode_id}' stopped.")

    def _heartbeat_sender(self):
        """Periodically sends heartbeats to the NameNode and receives commands."""
        self.logger.info("Heartbeat sender thread started.")
        while not self._stop_event.is_set():
            try:
                commands = self.namenode.send_heartbeat(self.datanode_id)
                if commands:
                    self.logger.info(f"Received {len(commands)} command(s) from NameNode.")
                    for cmd in commands:
                        # Execute commands in a new thread to not block the heartbeat loop
                        cmd_thread = threading.Thread(target=self.execute_command, args=(cmd,))
                        cmd_thread.start()
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}", exc_info=True)
            
            self._stop_event.wait(HEARTBEAT_INTERVAL)
        self.logger.info("Heartbeat sender thread stopped.")

    def _send_block_report(self):
        """Sends a full report of all blocks stored locally to the NameNode."""
        try:
            blocks = self.storage.get_block_report()
            self.namenode.block_report(self.datanode_id, blocks)
            self.logger.info(f"Sent block report with {len(blocks)} blocks.")
        except Exception as e:
            self.logger.error(f"Error sending block report: {e}", exc_info=True)
            
    def execute_command(self, command: Dict):
        """Executes a command received from the NameNode."""
        action = command.get("action")
        self.logger.info(f"Executing command: {action}")
        if action == "replicate":
            self.replicate_block(command["block_id"], command["source_dn_id"])
        elif action == "delete":
            # This would be for deleting blocks as part of rm -rf or balancing
            self.storage.delete_block(command["block_id"])
        else:
            self.logger.warning(f"Unknown command received: {action}")

    def replicate_block(self, block_id: str, source_dn_id: str):
        """Reads a block from a source DataNode and writes it to local storage."""
        self.logger.info(f"Starting replication of block '{block_id}' from source '{source_dn_id}'")
        try:
            source_datanode = self.datanodes_map.get(source_dn_id)
            if not source_datanode:
                raise HDFSCloneException(f"Source DataNode '{source_dn_id}' not found in local map.")
                
            block_data = source_datanode.read_block(block_id)
            self.storage.write_block(block_id, block_data)
            
            # Report successful replication to NameNode
            self.namenode.block_received(self.datanode_id, block_id)
            self.logger.info(f"Successfully replicated block '{block_id}'")

        except Exception as e:
            self.logger.error(f"Failed to replicate block '{block_id}': {e}", exc_info=True)
            # A real system would report this replication failure back to the NameNode.

    # --- Block Operations API ---
    def write_block(self, block_id: str, data: bytes):
        """Writes a block to local storage."""
        self.storage.write_block(block_id, data)

    def read_block(self, block_id: str) -> bytes:
        """Reads a block from local storage."""
        return self.storage.read_block(block_id)
    
        # === RPC Methods for Storage ===
    def store_block(self, block_id: str, data_hex: str) -> bool:
        """RPC-exposed: store a block (used by NameNode during upload)."""
        import binascii
        try:
            data = bytes.fromhex(data_hex)
            self.storage.write_block(block_id, data)
            self.namenode.block_received(self.datanode_id, block_id)
            self.logger.info(f"Stored block {block_id} ({len(data)} bytes)")
            return True
        except Exception as e:
            self.logger.error(f"Failed to store block {block_id}: {e}", exc_info=True)
            return False

    def fetch_block(self, block_id: str) -> str:
        """RPC-exposed: return a block as hex string."""
        import binascii
        try:
            data = self.storage.read_block(block_id)
            return binascii.hexlify(data).decode()
        except Exception as e:
            self.logger.error(f"Failed to fetch block {block_id}: {e}")
            return ""
