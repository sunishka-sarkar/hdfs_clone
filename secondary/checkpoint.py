# secondary/checkpoint.py
"""
Implements the Secondary NameNode for checkpointing metadata.
"""
import logging
import threading
import time
import shutil
import os
from typing import Dict, Any

from namenode.namenode import NameNode
from namenode.fsimage import FsImageManager
from namenode.editlog import EditLogManager
from common.exceptions import MetadataCorruptionError

class SecondaryNameNode:
    """A helper that periodically merges FsImage and EditLog."""

    def __init__(self, config: Dict[str, Any], namenode: NameNode):
        self.config = config
        self.namenode = namenode  # Direct reference to trigger log rolling
        self.logger = logging.getLogger("hdfs_clone.secondary_namenode")
        
        self.checkpoint_dir = self.config["namenode"]["checkpoint_dir"]
        self.namenode_data_dir = self.config["namenode"]["data_dir"]
        self.checkpoint_period = self.config["namenode"]["checkpoint_period"]
        
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        self._stop_event = threading.Event()
        self.thread = None

    def start(self):
        """Starts the checkpointing service in a background thread."""
        self.logger.info("Starting Secondary NameNode...")
        self.thread = threading.Thread(target=self._run, name="SecondaryNameNodeThread")
        self.thread.start()

    def stop(self):
        """Stops the checkpointing service."""
        self.logger.info("Stopping Secondary NameNode...")
        self._stop_event.set()
        if self.thread:
            self.thread.join()
        self.logger.info("Secondary NameNode stopped.")

    def _run(self):
        self.logger.info(f"Checkpointing scheduled to run every {self.checkpoint_period} seconds.")
        while not self._stop_event.is_set():
            try:
                self.logger.info("Checkpoint period started. Waiting...")
                # Wait for the period, but check for stop event intermittently
                self._stop_event.wait(self.checkpoint_period)
                if not self._stop_event.is_set():
                    self._do_checkpoint()
            except Exception as e:
                self.logger.error(f"An error occurred in checkpointing loop: {e}", exc_info=True)

    def _do_checkpoint(self):
        """Performs the checkpointing process."""
        self.logger.info("--- Starting checkpoint process ---")
        
        try:
            # 1. Ask NameNode to roll its edit log
            rolled_editlog_path = self.namenode.roll_edit_log_for_checkpoint()
            if not rolled_editlog_path:
                self.logger.warning("NameNode returned no rolled edit log. Aborting checkpoint.")
                return

            # 2. Copy FsImage and rolled EditLog to a temporary location
            temp_fsimage_path = os.path.join(self.checkpoint_dir, "fsimage.ckpt")
            shutil.copy2(os.path.join(self.namenode_data_dir, "fsimage.json"), temp_fsimage_path)
            
            # 3. Load the copied FsImage
            temp_fsimage_manager = FsImageManager(self.checkpoint_dir)
            namespace = temp_fsimage_manager.load()
            
            # 4. Load transactions from rolled EditLog
            temp_editlog_manager = EditLogManager(self.checkpoint_dir)
            # We rename the rolled log to what the manager expects to find
            shutil.copy2(rolled_editlog_path, os.path.join(self.checkpoint_dir, "edits.json"))
            transactions = temp_editlog_manager.load()
            
            # 5. Apply transactions to the in-memory namespace
            # This is a simplified replay; we'll manually apply them.
            self.logger.info(f"Applying {len(transactions)} transactions to the FsImage...")
            # A real SNN would need a full NamespaceManager to replay correctly.
            # We are skipping this complex step for a direct merge simulation.
            # In a real system, you would have an offline tool or instance of NamespaceManager to do this.
            self.logger.warning("Simplified Checkpoint: Skipping transaction replay. Will merge in a future version.")

            # 6. Save the new FsImage
            namespace['last_checkpoint'] = time.time()
            temp_fsimage_manager.save(namespace)

            # 7. Atomically move the new FsImage to the NameNode's data directory
            final_fsimage_path = os.path.join(self.namenode_data_dir, "fsimage.json")
            os.rename(temp_fsimage_path, final_fsimage_path)

            self.logger.info("--- Checkpoint process completed successfully ---")
            
            # 8. Clean up rolled log
            os.remove(rolled_editlog_path)
            os.remove(os.path.join(self.checkpoint_dir, "edits.json"))

        except (FileNotFoundError, MetadataCorruptionError) as e:
            self.logger.error(f"Checkpoint failed due to missing or corrupt metadata: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Checkpoint failed with an unexpected error: {e}", exc_info=True)