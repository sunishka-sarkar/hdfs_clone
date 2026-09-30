# === START OF FILE: client/hdfs_client.py ===

import logging
import os
from typing import Dict, Any, List

from .output_stream import FSDataOutputStream
from .input_stream import FSDataInputStream
from namenode.namenode import NameNode
from datanode.datanode import DataNode
from common import constants
from common.exceptions import *

class HDFSClient:
    """The main client for interacting with the HDFS cluster."""
    
    def __init__(self, namenode: NameNode, datanodes: Dict[str, DataNode]):
        self.namenode = namenode
        self.datanodes = datanodes
        self.logger = logging.getLogger("hdfs_clone.client")
        self.logger.info("HDFSClient initialized.")

    def mkdir(self, path: str, user: str, group: str, permissions: str = constants.DEFAULT_DIR_PERMISSIONS):
        return self.namenode.mkdir(path, permissions, user, group)

    def create(self, path: str, user: str, group: str, replication: int = constants.DEFAULT_REPLICATION_FACTOR, block_size: int = constants.DEFAULT_BLOCK_SIZE) -> FSDataOutputStream:
        permissions = constants.DEFAULT_FILE_PERMISSIONS
        self.namenode.create(path, replication, block_size, permissions, user, group)
        return FSDataOutputStream(self.namenode, self.datanodes, path, block_size, user, group)

    def open(self, path: str, user: str, group: str) -> FSDataInputStream:
        return FSDataInputStream(self.namenode, self.datanodes, path, user, group)

    def list_status(self, path: str, user: str, group: str):
        return self.namenode.list_status(path, user, group)

    def get_file_info(self, path: str, user: str, group: str):
        return self.namenode.get_file_info(path, user, group)

    def delete(self, path: str, user: str, group: str, recursive: bool = False):
        return self.namenode.delete(path, user, group, recursive)

    def rename(self, src: str, dst: str, user: str, group: str):
        return self.namenode.rename(src, dst, user, group)

    def chmod(self, path: str, mode: str, user: str):
        return self.namenode.chmod(path, mode, user)

    def chown(self, path: str, owner: str, group: str, user: str):
        return self.namenode.chown(path, owner, group, user)

    def set_quota(self, path: str, name_quota: int = None, space_quota: int = None, user: str = 'cli'):
        return self.namenode.set_quota(path, name_quota, space_quota, user)

    def count(self, path: str, user: str, group: str):
        return self.namenode.get_content_summary(path, user, group)

    def get_datanode_report(self):
        return self.namenode.get_datanode_report()

    def get_safe_mode_status(self):
        return self.namenode.get_safe_mode_status()

    def copy_from_local(self, local_src: str, hdfs_dst: str, user: str, group: str):
        """Uploads a local file to HDFS."""
        self.logger.info(f"Uploading local file '{local_src}' to HDFS '{hdfs_dst}'")
        if not os.path.exists(local_src):
            raise FileNotFoundException(f"Local file not found: {local_src}")
        
        with open(local_src, 'rb') as f_local, self.create(hdfs_dst, user=user, group=group) as f_hdfs:
            # --- FIX: Corrected indentation for the while loop ---
            while True:
                chunk = f_local.read(65536)
                if not chunk:
                    break
                f_hdfs.write(chunk)
        self.logger.info("Upload complete.")

    def copy_to_local(self, hdfs_src: str, local_dst: str, user: str, group: str):
        """Downloads an HDFS file to the local filesystem."""
        self.logger.info(f"Downloading HDFS file '{hdfs_src}' to local '{local_dst}'")
        with self.open(hdfs_src, user=user, group=group) as f_hdfs, open(local_dst, 'wb') as f_local:
            # This implementation reads the whole file at once, which is fine for this project.
            content = f_hdfs.read()
            f_local.write(content)
        self.logger.info("Download complete.")

# === END OF FILE: client/hdfs_client.py ===