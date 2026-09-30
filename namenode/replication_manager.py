# namenode/replication_manager.py
"""
Manages the detection and scheduling of block re-replication.
"""
import logging
import threading
import time
from collections import deque

from common.constants import REPLICATION_MANAGER_INTERVAL

class ReplicationManager:
    """Monitors block replication levels and schedules replication tasks."""

    def __init__(self, namenode, namespace_manager, block_manager):
        self.namenode = namenode
        self.namespace_manager = namespace_manager
        self.block_manager = block_manager
        self.logger = logging.getLogger("hdfs_clone.replication_manager")

        # Commands for DataNodes are queued here
        self.replication_tasks = deque()
        
        self._stop_event = threading.Event()
        self.thread = None

    def start(self):
        """Starts the replication monitor thread."""
        self.logger.info("Starting Replication Manager...")
        self.thread = threading.Thread(target=self._monitor, name="ReplicationManagerThread")
        self.thread.start()

    def stop(self):
        """Stops the replication monitor thread."""
        self.logger.info("Stopping Replication Manager...")
        self._stop_event.set()
        if self.thread:
            self.thread.join()
        self.logger.info("Replication Manager stopped.")

    def _monitor(self):
        """Periodically checks for replication needs."""
        while not self._stop_event.is_set():
            try:
                # Do not check for replication while in safe mode
                if not self.namenode.in_safe_mode:
                    self.check_replication_levels()
            except Exception as e:
                self.logger.error(f"Error in replication monitor: {e}", exc_info=True)
            
            self._stop_event.wait(REPLICATION_MANAGER_INTERVAL)

    def check_replication_levels(self):
        """Scans blocks and creates tasks for under-replicated ones."""
        self.logger.info("Running replication level check...")
        under_replicated_count = 0
        
        # Get a consistent snapshot of the block and file maps
        block_to_path = self.namespace_manager.get_block_to_path_map()
        all_blocks = self.block_manager.get_all_block_locations()

        for block_id, locations in all_blocks.items():
            path = block_to_path.get(block_id)
            if not path:
                self.logger.warning(f"Found an orphan block '{block_id}'. Skipping.")
                continue

            file_meta = self.namespace_manager.get_file_metadata_internal(path)
            if not file_meta:
                self.logger.warning(f"Could not find file metadata for path '{path}' of block '{block_id}'. Skipping.")
                continue

            target_replication = file_meta['replication']
            live_replicas = [dn for dn in locations if self.block_manager.is_datanode_live(dn)]
            
            if len(live_replicas) < target_replication:
                under_replicated_count += 1
                self._schedule_replication(block_id, live_replicas, target_replication - len(live_replicas))
        
        if under_replicated_count > 0:
            self.logger.info(f"Replication check finished. Found {under_replicated_count} under-replicated blocks.")
        else:
            self.logger.info("Replication check finished. All blocks are sufficiently replicated.")

    def _schedule_replication(self, block_id, live_replicas, replicas_needed):
        """Creates replication tasks for a single under-replicated block."""
        if not live_replicas:
            self.logger.error(f"Block '{block_id}' has no live replicas (total data loss!). Cannot schedule replication.")
            return

        # Choose a random live replica as the source
        source_dn = live_replicas[0]
        
        try:
            target_dns = self.block_manager.select_datanodes_for_block(replicas_needed, existing_locations=set(live_replicas))
            for target_dn in target_dns:
                task = {
                    "action": "replicate",
                    "block_id": block_id,
                    "source_dn_id": source_dn,
                    "target_dn_id": target_dn
                }
                self.replication_tasks.append(task)
                self.logger.info(f"Scheduled replication of block '{block_id}' from '{source_dn}' to '{target_dn}'")
        except Exception as e:
            self.logger.error(f"Failed to schedule replication for block '{block_id}': {e}", exc_info=True)

    def get_datanode_commands(self, datanode_id: str):
        """Gets pending commands for a specific DataNode."""
        commands = []
        # A simple approach: give the first available task where this DN is the target
        # A more advanced system would have per-DN queues.
        if self.replication_tasks and self.replication_tasks[0]['target_dn_id'] == datanode_id:
            commands.append(self.replication_tasks.popleft())
        return commands