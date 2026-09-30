# tests/test_metadata.py
"""
Unit tests for metadata management components like FsImage and EditLog.
"""
import unittest
import os
import json
import tempfile
import shutil
import logging

from common.utils import atomic_write_json, safe_read_json, setup_logging
from common.exceptions import AtomicWriteError, MetadataCorruptionError
from namenode.fsimage import FsImageManager
from namenode.editlog import EditLogManager

# --- NEW: Base class to handle common setup and teardown, including logger cleanup ---
class BaseMetadataTest(unittest.TestCase):
    """A base class for metadata tests to handle temporary directories and logging."""
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.logger = setup_logging(os.path.join(self.test_dir, 'test.log'), force_reconfigure=True)

    def tearDown(self):
        # --- FIX: Crucial step to release file handles before deleting the directory ---
        # Get the logger and remove its handlers
        logger = logging.getLogger("hdfs_clone")
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
        
        # Now we can safely remove the directory
        shutil.rmtree(self.test_dir)

class TestMetadataUtils(BaseMetadataTest):

    def setUp(self):
        super().setUp()
        # --- FIX: Re-added the missing attribute definition ---
        self.test_file = os.path.join(self.test_dir, 'test.json')

    def test_atomic_write_success(self):
        """Test that atomic_write_json successfully creates a file."""
        data = {"key": "value"}
        atomic_write_json(self.test_file, data, self.logger)
        self.assertTrue(os.path.exists(self.test_file))
        with open(self.test_file, 'r') as f:
            self.assertEqual(json.load(f), data)

    def test_safe_read_success(self):
        """Test that safe_read_json successfully reads a file."""
        data = {"key": "value"}
        with open(self.test_file, 'w') as f:
            json.dump(data, f)
        
        read_data = safe_read_json(self.test_file, self.logger)
        self.assertEqual(read_data, data)

    def test_safe_read_file_not_found(self):
        """Test that safe_read_json raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            safe_read_json(self.test_file, self.logger)

    def test_safe_read_corrupt_json(self):
        """Test that safe_read_json raises MetadataCorruptionError for bad JSON."""
        with open(self.test_file, 'w') as f:
            f.write("{'key': 'value'}")  # Invalid JSON with single quotes
        
        with self.assertRaises(MetadataCorruptionError):
            safe_read_json(self.test_file, self.logger)

class TestFsImageManager(BaseMetadataTest):
    
    def setUp(self):
        super().setUp()
        self.fsimage_manager = FsImageManager(self.test_dir)

    def test_load_non_existent(self):
        """Test loading when no fsimage exists; should return default."""
        namespace = self.fsimage_manager.load()
        self.assertIn("directories", namespace)
        self.assertIn("/", namespace["directories"])

    def test_save_and_load(self):
        """Test saving and then loading the fsimage."""
        namespace_data = {"files": {"/test.txt": {}}, "directories": {"/": {}}}
        self.fsimage_manager.save(namespace_data)
        
        loaded_namespace = self.fsimage_manager.load()
        self.assertEqual(loaded_namespace, namespace_data)
        
class TestEditLogManager(BaseMetadataTest):

    def setUp(self):
        super().setUp()
        self.editlog_manager = EditLogManager(self.test_dir)

    def test_log_and_load(self):
        """Test logging a transaction and loading it back."""
        self.editlog_manager.log_transaction("CREATE", path="/a.txt", replication=1)
        self.editlog_manager.log_transaction("DELETE", path="/b.txt")
        
        transactions = self.editlog_manager.load()
        self.assertEqual(len(transactions), 2)
        self.assertEqual(transactions[0]["operation"], "CREATE")
        self.assertEqual(transactions[0]["params"]["path"], "/a.txt")
        self.assertEqual(transactions[1]["txid"], 2)

    def test_roll_log(self):
        """Test that the edit log can be rolled."""
        self.editlog_manager.log_transaction("MKDIR", path="/data")
        
        rolled_path = self.editlog_manager.roll_log()
        self.assertTrue(os.path.exists(rolled_path))
        self.assertFalse(os.path.exists(self.editlog_manager.editlog_path))
        
        # Logging again should create a new edits.json
        self.editlog_manager.log_transaction("CREATE", path="/new.txt")
        self.assertTrue(os.path.exists(self.editlog_manager.editlog_path))
        transactions = self.editlog_manager.load()
        self.assertEqual(len(transactions), 1)
        self.assertEqual(transactions[0]['txid'], 1) # txid resets for the new log

if __name__ == '__main__':
    unittest.main()