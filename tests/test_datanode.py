import unittest
import os
import tempfile
import shutil
import logging
import zlib

from datanode.block_storage import BlockStorage
from common.exceptions import HDFSCloneException
from common.utils import setup_logging

class TestBlockStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.logger = setup_logging("logs/hdfs_test.log")
        self.storage = BlockStorage("dn_test", self.test_dir)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_write_read_delete(self):
        """Test basic block I/O operations."""
        block_id = "blk_123"
        data = b"some block data"
        self.storage.write_block(block_id, data)
        
        # Verify read
        read_data = self.storage.read_block(block_id)
        self.assertEqual(data, read_data)
        
        # Verify block report
        report = self.storage.get_block_report()
        self.assertIn(block_id, report)
        
        # Verify delete
        self.storage.delete_block(block_id)
        with self.assertRaises(FileNotFoundError):
            self.storage.read_block(block_id)

    def test_checksum_mismatch(self):
        """Test that data corruption is detected via checksum mismatch."""
        block_id = "blk_corrupt"
        data = b"original data"
        self.storage.write_block(block_id, data)
        
        # Manually corrupt the block file
        block_path, _ = self.storage._get_block_paths(block_id)
        with open(block_path, "wb") as f:
            f.write(b"corrupted data")
            
        # Verify that reading now raises an exception
        with self.assertRaisesRegex(HDFSCloneException, "is corrupt"):
            self.storage.read_block(block_id)