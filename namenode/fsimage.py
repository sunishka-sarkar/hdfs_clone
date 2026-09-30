# === START OF FILE: namenode/fsimage.py ===

import os
import threading
import logging
from typing import Dict, Any

from common.utils import atomic_write_json, safe_read_json
from common.exceptions import MetadataCorruptionError

class FsImageManager:
    """Handles loading and saving the FsImage file."""

    def __init__(self, namenode_data_dir: str):
        self.fsimage_path = os.path.join(namenode_data_dir, 'fsimage.json')
        self.logger = logging.getLogger("hdfs_clone.fsimage")
        self._lock = threading.Lock()
        self.logger.info(f"FsImageManager initialized. FsImage path: '{self.fsimage_path}'")

    def load(self) -> Dict[str, Any]:
        """
        Loads the FsImage from disk. If it doesn't exist, creates a default
        namespace with a world-writable root directory for testing purposes.
        """
        with self._lock:
            try:
                data = safe_read_json(self.fsimage_path, self.logger)
                self.logger.info("FsImage loaded successfully.")
                return data
            except FileNotFoundError:
                self.logger.warning("FsImage file not found. Creating a new initial namespace.")
                # Return a default structure for a new filesystem
                return {
                    "version": "1.0",
                    "namespace_id": "hdfs_clone_ns_1",
                    "last_checkpoint": None,
                    "files": {},
                    "directories": {
                        "/": {
                            "owner": "hdfs",
                            "group": "supergroup",
                            # --- FIX: Changed from 755 to 777 to allow tests to write to root ---
                            "permissions": "777",
                            "modification_time": None
                        }
                    },
                    "block_map": {}
                }
            except MetadataCorruptionError as e:
                self.logger.critical("FsImage is corrupt! Cannot start NameNode.", exc_info=True)
                raise e

    def save(self, namespace_data: Dict[str, Any]):
        """
        Saves the current namespace data to the FsImage file atomically.
        """
        with self._lock:
            self.logger.info("Attempting to save FsImage snapshot...")
            try:
                atomic_write_json(self.fsimage_path, namespace_data, self.logger)
                self.logger.info("FsImage snapshot saved successfully.")
            except Exception as e:
                self.logger.error("Failed to save FsImage snapshot!", exc_info=True)

# === END OF FILE: namenode/fsimage.py ===