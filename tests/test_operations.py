# === START OF FILE: tests/test_operations.py ===

import unittest
import os
import tempfile
import shutil
import logging

from common.config import load_config
from common.utils import setup_logging
from namenode.namenode import NameNode
from datanode.datanode import DataNode
from client.hdfs_client import HDFSClient
from common.exceptions import *

class TestIntegrationOperations(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Setup logging once for all tests in this class."""
        setup_logging("logs/hdfs_test.log", level=logging.DEBUG, force_reconfigure=True)

    def setUp(self):
        """Set up a fresh in-memory HDFS cluster for each test."""
        self.test_dir = tempfile.mkdtemp()
        
        self.config = load_config()
        self.config['namenode']['data_dir'] = os.path.join(self.test_dir, 'namenode')
        self.config['datanode']['base_dir'] = os.path.join(self.test_dir, 'datanodes')
        
        # Initialize components
        self.namenode = NameNode(self.config)
        self.datanodes_map = {}
        
        # --- FIX: Correctly initialize DataNodes with the datanodes_map ---
        # The map needs to be passed so each DataNode knows how to find its peers for replication.
        for i in range(1, self.config["datanode"]["num_datanodes"] + 1):
            dn_id = f"dn{i}"
            # Create the instance and pass the (currently being built) map to it
            self.datanodes_map[dn_id] = DataNode(dn_id, self.config, self.namenode, self.datanodes_map)
        
        self.client = HDFSClient(self.namenode, self.datanodes_map)

        # Start services
        self.namenode.start()
        for dn in self.datanodes_map.values():
            dn.start()

    def tearDown(self):
        """Stop the cluster and clean up the temporary directory."""
        # --- FIX: Add logger cleanup to prevent PermissionError on Windows ---
        logger = logging.getLogger("hdfs_clone")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

        for dn in self.datanodes_map.values():
            dn.stop()
        self.namenode.stop()
        shutil.rmtree(self.test_dir)

    def test_e2e_write_read_delete(self):
        """Test the full lifecycle: write a file, read it, verify, then delete it."""
        self.client.mkdir("/data", user="testuser", group="testgroup")
        
        file_path = "/data/test_file.txt"
        content = b"Hello, HDFS Clone!"
        self.client.copy_from_local_string(content, file_path, user="testuser", group="testgroup")

        file_info = self.namenode.get_file_info(file_path, user="testuser", group="testgroup")
        self.assertEqual(file_info['file_size'], len(content))
        self.assertEqual(len(file_info['blocks']), 1)
        
        read_content = self.client.copy_to_local_string(file_path, user="testuser", group="testgroup")
        self.assertEqual(content, read_content)

        self.client.delete(file_path, user="testuser", group="testgroup")
        
        with self.assertRaises(FileNotFoundException):
            self.namenode.get_file_info(file_path, user="testuser", group="testgroup")

    def test_multi_block_file(self):
        """Test that a file larger than the block size is handled correctly."""
        block_size = 1024
        content = b"A" * (block_size * 2 + 500)
        file_path = "/large_file.bin"
        
        with self.client.create(file_path, block_size=block_size, user="testuser", group="testgroup") as writer:
            writer.write(content)
        
        file_info = self.namenode.get_file_info(file_path, user="testuser", group="testgroup")
        self.assertEqual(file_info['file_size'], len(content))
        self.assertEqual(len(file_info['blocks']), 3)

        read_content = self.client.copy_to_local_string(file_path, user="testuser", group="testgroup")
        self.assertEqual(content, read_content)

    def test_read_from_replica_on_failure(self):
        """
        Crucial Fault-Tolerance Test: Verify the client can read a block from another
        replica if the first one fails.
        """
        file_path = "/fault_tolerance_test.txt"
        content = b"This data must survive a DataNode failure."
        
        with self.client.create(file_path, replication=2, user="testuser", group="testgroup") as writer:
            writer.write(content)

        file_info = self.namenode.get_file_info(file_path, user="testuser", group="testgroup")
        block_id = file_info['blocks'][0]
        locations = self.namenode.get_block_locations(file_path, user="testuser", group="testgroup")[0]['locations']
        self.assertGreaterEqual(len(locations), 2)

        failed_dn_id = locations[0]
        failed_dn = self.datanodes_map[failed_dn_id]
        
        original_read_block = failed_dn.read_block
        def new_read_block_that_fails(block_id_arg):
            logging.info(f"SIMULATED FAILURE on DataNode {failed_dn_id} for block {block_id_arg}")
            raise HDFSCloneException("Simulated disk read failure")
        
        failed_dn.read_block = new_read_block_that_fails

        try:
            read_content = self.client.copy_to_local_string(file_path, user="testuser", group="testgroup")
            self.assertEqual(content, read_content)
            logging.info("SUCCESS: File was read successfully from a replica.")
        finally:
            failed_dn.read_block = original_read_block

# Helper methods for testing
def copy_from_local_string_with_user(self, content: bytes, hdfs_dst: str, user: str, group: str):
    with self.create(hdfs_dst, user=user, group=group) as f_hdfs:
        f_hdfs.write(content)

def copy_to_local_string_with_user(self, hdfs_src: str, user: str, group: str) -> bytes:
    with self.open(hdfs_src, user=user, group=group) as f_hdfs:
        return f_hdfs.read()

# Monkey-patch the client for easier testing
HDFSClient.copy_from_local_string = copy_from_local_string_with_user
HDFSClient.copy_to_local_string = copy_to_local_string_with_user

if __name__ == '__main__':
    unittest.main()

# === END OF FILE: tests/test_operations.py ===