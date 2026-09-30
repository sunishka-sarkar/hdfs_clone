# === START OF FILE: client/input_stream.py ===

import logging
import random
from typing import Dict

from namenode.namenode import NameNode
from datanode.datanode import DataNode
from common.exceptions import *

class FSDataInputStream:
    """A stream for reading data from an HDFS file."""

    def __init__(self, namenode: NameNode, datanodes_map: Dict[str, DataNode], path: str, user: str, group: str):
        self.namenode = namenode
        self.datanodes_map = datanodes_map
        self.path = path
        self.user = user
        self.group = group
        self.logger = logging.getLogger("hdfs_clone.input_stream")
        file_info = self.namenode.get_file_info(path, self.user, self.group)
        self.file_size = file_info["file_size"]
        self.block_size = file_info["block_size"]
        self.block_locations = self.namenode.get_block_locations(path, self.user, self.group)
        self.pos = 0; self.current_block_data = None; self.current_block_index = -1; self.is_closed = False

    def _fetch_block(self, block_index: int):
        if block_index >= len(self.block_locations): self.current_block_data = b''; return
        block_info = self.block_locations[block_index]
        block_id, datanode_ids = block_info["block_id"], block_info["locations"]
        random.shuffle(datanode_ids)
        last_exception = None
        for dn_id in datanode_ids:
            try:
                datanode = self.datanodes_map[dn_id]
                self.current_block_data = datanode.read_block(block_id)
                self.current_block_index = block_index
                self.logger.info(f"Read block '{block_id}' from DataNode '{dn_id}'")
                return
            except Exception as e:
                self.logger.warning(f"Failed to read block '{block_id}' from '{dn_id}': {e}", exc_info=False)
                last_exception = e
        raise HDFSCloneException(f"Could not read block {block_id} from any replicas.") from last_exception

    def read(self, size: int = -1) -> bytes:
        if self.is_closed: raise HDFSCloneException("Stream is closed.")
        if self.pos >= self.file_size: return b''
        if size < 0 or size > self.file_size - self.pos: size = self.file_size - self.pos
        result = bytearray()
        while len(result) < size:
            block_index = self.pos // self.block_size
            offset = self.pos % self.block_size
            if block_index != self.current_block_index: self._fetch_block(block_index)
            chunk = self.current_block_data[offset:offset + (size - len(result))]
            result.extend(chunk)
            self.pos += len(chunk)
        return bytes(result)

    def close(self):
        if self.is_closed: return
        self.is_closed = True; self.current_block_data = None
        
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): self.close()

# === END OF FILE: client/input_stream.py ===