# datanode/block_storage.py
"""
Manages the physical storage of blocks on the local filesystem.
Includes checksum handling for data integrity.
"""
import os
import logging
import zlib
import json
from typing import List, Tuple

from common.exceptions import HDFSCloneException

class BlockStorage:
    """Manages blocks on a DataNode's local disk."""
    
    METADATA_SUFFIX = ".meta"

    def __init__(self, datanode_id: str, data_dir: str):
        self.datanode_id = datanode_id
        # Use a subdirectory for each datanode to keep storage isolated
        self.data_dir = os.path.join(data_dir, self.datanode_id)
        self.logger = logging.getLogger(f"hdfs_clone.datanode.{self.datanode_id}.storage")
        
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            self.logger.info(f"Block storage initialized at '{self.data_dir}'")
        except OSError as e:
            self.logger.critical(f"Failed to create data directory '{self.data_dir}': {e}", exc_info=True)
            raise HDFSCloneException(f"Cannot initialize block storage for {self.datanode_id}")

    def _get_block_paths(self, block_id: str) -> Tuple[str, str]:
        """Returns the paths for a block and its metadata."""
        return (
            os.path.join(self.data_dir, block_id),
            os.path.join(self.data_dir, f"{block_id}{self.METADATA_SUFFIX}")
        )

    def write_block(self, block_id: str, data: bytes):
        """
        Writes a block and its metadata (checksum) to local disk.
        """
        # --- FIX: Changed self_get_block_paths to self._get_block_paths ---
        block_path, meta_path = self._get_block_paths(block_id)
        self.logger.info(f"Writing block '{block_id}' ({len(data)} bytes) to '{block_path}'")
        
        try:
            # Write data
            with open(block_path, 'wb') as f:
                f.write(data)
            
            # Calculate checksum and write metadata
            checksum = zlib.crc32(data)
            metadata = {
                "block_id": block_id,
                "size": len(data),
                "checksum": checksum,
                "datanode_id": self.datanode_id
            }
            with open(meta_path, 'w') as f:
                json.dump(metadata, f)

            self.logger.info(f"Successfully wrote block '{block_id}' with checksum {checksum}.")
        except (IOError, OSError) as e:
            self.logger.error(f"Failed to write block '{block_id}': {e}", exc_info=True)
            # Attempt cleanup
            if os.path.exists(block_path): os.remove(block_path)
            if os.path.exists(meta_path): os.remove(meta_path)
            raise HDFSCloneException(f"Disk I/O error writing block {block_id}") from e

    def read_block(self, block_id: str) -> bytes:
        """
        Reads a block from local disk and verifies its checksum.
        """
        block_path, meta_path = self._get_block_paths(block_id)
        self.logger.info(f"Reading block '{block_id}' from '{block_path}'")

        if not os.path.exists(block_path) or not os.path.exists(meta_path):
            self.logger.error(f"Block '{block_id}' or its metadata not found.")
            raise FileNotFoundError(f"Block {block_id} not found on {self.datanode_id}")
            
        try:
            # Read metadata
            with open(meta_path, 'r') as f:
                metadata = json.load(f)
            
            # Read data
            with open(block_path, 'rb') as f:
                data = f.read()

            # Verify checksum
            expected_checksum = metadata.get('checksum')
            actual_checksum = zlib.crc32(data)

            if expected_checksum != actual_checksum:
                self.logger.critical(f"Checksum mismatch for block '{block_id}'! "
                                     f"Expected: {expected_checksum}, Got: {actual_checksum}. Block is corrupt.")
                raise HDFSCloneException(f"Block {block_id} is corrupt on {self.datanode_id}")
            
            self.logger.info(f"Successfully read and verified block '{block_id}'.")
            return data
        except (IOError, OSError, json.JSONDecodeError) as e:
            self.logger.error(f"Failed to read block '{block_id}': {e}", exc_info=True)
            raise HDFSCloneException(f"Error reading block {block_id}") from e

    def delete_block(self, block_id: str):
        """Deletes a block and its metadata from local disk."""
        block_path, meta_path = self._get_block_paths(block_id)
        self.logger.info(f"Deleting block '{block_id}'")

        for path in [block_path, meta_path]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError as e:
                self.logger.error(f"Failed to delete file '{path}': {e}", exc_info=True)
                # Continue trying to delete the other file

    def get_block_report(self) -> List[str]:
        """Returns a list of all block IDs stored on this DataNode."""
        self.logger.debug("Generating block report.")
        block_ids = []
        try:
            for filename in os.listdir(self.data_dir):
                if not filename.endswith(self.METADATA_SUFFIX):
                    block_ids.append(filename)
            self.logger.info(f"Block report generated with {len(block_ids)} blocks.")
            return block_ids
        except OSError as e:
            self.logger.error(f"Failed to generate block report: {e}", exc_info=True)
            return []