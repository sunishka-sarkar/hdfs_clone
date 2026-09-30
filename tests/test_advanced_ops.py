# === START OF FILE: tests/test_advanced_ops.py ===

import unittest
import logging

from tests.test_operations import TestIntegrationOperations
from common.exceptions import *

# Helper method for testing
def copy_from_local_string_with_user(self, content: bytes, hdfs_dst: str, user: str, group: str):
    with self.create(hdfs_dst, user=user, group=group) as f_hdfs:
        f_hdfs.write(content)

class TestAdvancedOperations(TestIntegrationOperations):

    def setUp(self):
        super().setUp()
        # Patch the client for these tests
        self.client.copy_from_local_string = copy_from_local_string_with_user.__get__(self.client)

    def test_rename_file(self):
        """Test renaming a single file."""
        src_path = "/dir1/file.txt"
        dst_path = "/dir2/new_file.txt"
        content = b"rename test"

        self.client.mkdir("/dir1", user="testuser", group="testgroup")
        self.client.mkdir("/dir2", user="testuser", group="testgroup")
        self.client.copy_from_local_string(content, src_path, user="testuser", group="testgroup")
        
        self.assertIsNotNone(self.client.get_file_info(src_path, user="testuser", group="testgroup"))
        with self.assertRaises(FileNotFoundException):
            self.client.get_file_info(dst_path, user="testuser", group="testgroup")
            
        self.client.rename(src_path, dst_path, user="testuser", group="testgroup")

        with self.assertRaises(FileNotFoundException):
            self.client.get_file_info(src_path, user="testuser", group="testgroup")
        
        read_content = self.client.copy_to_local_string(dst_path, user="testuser", group="testgroup")
        self.assertEqual(content, read_content)

    def test_rename_directory(self):
        """Test renaming a directory and ensure its contents are moved."""
        self.client.mkdir("/data", user="testuser", group="testgroup")
        self.client.mkdir("/data/srcdir", user="testuser", group="testgroup")
        self.client.copy_from_local_string(b"child", "/data/srcdir/child.txt", user="testuser", group="testgroup")

        src_path = "/data/srcdir"
        dst_path = "/data/dstdir"

        self.client.rename(src_path, dst_path, user="testuser", group="testgroup")

        with self.assertRaises(FileNotFoundException):
            self.client.get_file_info(src_path, user="testuser", group="testgroup")
        with self.assertRaises(FileNotFoundException):
            self.client.get_file_info("/data/srcdir/child.txt", user="testuser", group="testgroup")
            
        self.assertIsNotNone(self.client.get_file_info(dst_path, user="testuser", group="testgroup"))
        read_content = self.client.copy_to_local_string("/data/dstdir/child.txt", user="testuser", group="testgroup")
        self.assertEqual(b"child", read_content)

    def test_rename_quota_update(self):
        """Verify that renaming correctly moves quota usage between parent directories."""
        self.client.mkdir("/src", user="hdfs", group="supergroup")
        self.client.mkdir("/dst", user="hdfs", group="supergroup")
        
        self.client.chmod("/src", "777", user="hdfs")
        # --- FIX: Add chmod for the destination directory as well ---
        self.client.chmod("/dst", "777", user="hdfs")
        
        self.client.copy_from_local_string(b"A"*100, "/src/file.txt", user="testuser", group="testgroup")
        
        src_summary_before = self.client.count("/src", user="hdfs", group="supergroup")
        dst_summary_before = self.client.count("/dst", user="hdfs", group="supergroup")
        self.assertEqual(src_summary_before['name_count'], 2)
        self.assertEqual(dst_summary_before['name_count'], 1)

        self.client.rename("/src/file.txt", "/dst/file.txt", user="testuser", group="testgroup")

        src_summary_after = self.client.count("/src", user="hdfs", group="supergroup")
        dst_summary_after = self.client.count("/dst", user="hdfs", group="supergroup")
        self.assertEqual(src_summary_after['name_count'], 1)
        self.assertEqual(dst_summary_after['name_count'], 2)

if __name__ == '__main__':
    unittest.main()

# === END OF FILE: tests/test_advanced_ops.py ===