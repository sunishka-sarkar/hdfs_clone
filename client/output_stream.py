# === START OF FILE: client/output_stream.py ===

import logging
from typing import Dict

from namenode.namenode import NameNode
from datanode.datanode import DataNode
from common.exceptions import HDFSCloneException

class FSDataOutputStream:
    """A stream for writing data to an HDFS file."""

    def __init__(self, namenode: NameNode, datanodes_map: Dict[str, DataNode], 
                 path: str, block_size: int, user: str, group: str):
        self.namenode = namenode
        self.datanodes_map = datanodes_map
        self.path = path
        self.block_size = block_size
        self.user = user
        self.group = group
        self.logger = logging.getLogger("hdfs_clone.output_stream")
        self.buffer = bytearray()
        self.bytes_written = 0
        self.is_closed = False

    def write(self, data: bytes):
        if self.is_closed: raise HDFSCloneException("Stream is closed.")
        self.buffer.extend(data)
        while len(self.buffer) >= self.block_size: self._flush_block()

    def _flush_block(self):
        if not self.buffer: return
        data_to_write = self.buffer[:self.block_size]
        try:
            block_info = self.namenode.add_block(self.path, self.user, self.group)
            block_id, datanode_ids = block_info["block_id"], block_info["locations"]
            for dn_id in datanode_ids: self.datanodes_map[dn_id].write_block(block_id, data_to_write)
            self.bytes_written += len(data_to_write)
            self.buffer = self.buffer[len(data_to_write):]
        except Exception as e:
            self.logger.error(f"Failed to write block for '{self.path}': {e}", exc_info=True)
            raise HDFSCloneException(f"Block write failed for {self.path}") from e

    def flush(self):
        if self.is_closed: raise HDFSCloneException("Stream is closed.")
        if self.buffer: self._flush_block()

    def close(self):
        if self.is_closed: return
        self.flush()
        self.namenode.complete_file(self.path, self.bytes_written, self.user, self.group)
        self.is_closed = True; self.buffer.clear()
        
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close()

# === END OF FILE: client/output_stream.py ===