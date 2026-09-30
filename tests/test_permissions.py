# tests/test_permissions.py
"""
Integration tests for the permission and ownership model.
"""
import unittest
import logging

# Inherit from our integration test setup
from tests.test_operations import TestIntegrationOperations 
# --- FIX: Import both exceptions used in this file ---
from common.exceptions import PermissionDeniedException, FileNotFoundException

# Helper methods to add user/group context to the test client's methods
def copy_from_local_string_with_user(self, content: bytes, hdfs_dst: str, user: str, group: str):
    with self.create(hdfs_dst, user=user, group=group) as f_hdfs:
        f_hdfs.write(content)

class TestPermissions(TestIntegrationOperations):

    def setUp(self):
        """Extend the base setup to patch the client for these tests."""
        super().setUp()
        # Monkey-patch the client for easier testing with user context
        # This replaces the old helper method with one that understands user/group
        self.client.copy_from_local_string = copy_from_local_string_with_user.__get__(self.client)

    def test_mkdir_permission_denied(self):
        """Verify a user cannot create a directory inside a folder they don't have write access to."""
        # Superuser creates a restricted directory
        self.client.mkdir("/restricted", user="hdfs", group="supergroup")
        self.client.chmod("/restricted", "755", user="hdfs") # Owner can write, others can only read/execute
        
        # Another user tries to create a directory inside it
        with self.assertRaisesRegex(PermissionDeniedException, "Permission denied for user 'testuser' on '/restricted'"):
            self.client.mkdir("/restricted/newdir", user="testuser", group="testgroup")
        
    def test_read_permission_denied(self):
        """Verify a user cannot read a file they don't have read access to."""
        path = "/secret.txt"
        self.client.copy_from_local_string(b"secret data", path, user="owner", group="secretgroup")
        self.client.chmod(path, "600", user="owner") # Owner can read/write, nobody else can do anything

        with self.assertRaises(PermissionDeniedException):
            self.client.open(path, user="attacker", group="users")

    def test_delete_permission(self):
        """Verify a user must have write access to the *parent* directory to delete a file."""
        self.client.mkdir("/data", user="hdfs", group="supergroup")
        self.client.chmod("/data", "777", user="hdfs") # Everyone can write to /data

        path = "/data/deletable.txt"
        self.client.copy_from_local_string(b"data", path, user="file_owner", group="group")
        self.client.chmod(path, "444", user="file_owner") # File itself is read-only

        # A different user should still be able to delete it because the parent (/data) is writable
        self.client.delete(path, user="another_user", group="another_group")
        
        # Verify deletion
        with self.assertRaises(FileNotFoundException):
            self.client.get_file_info(path, user="hdfs", group="supergroup")

    def test_chown_chmod_success_by_owner(self):
        """Verify owner can change ownership and permissions."""
        path = "/testfile"
        self.client.copy_from_local_string(b"data", path, user="owner", group="users")
        
        # Owner changes permissions
        self.client.chmod(path, "700", user="owner")
        meta = self.namenode.get_file_info(path, user="hdfs", group="supergroup")
        self.assertEqual(meta['permissions'], "700")

        # Owner changes ownership
        self.client.chown(path, "newowner", "newgroup", user="owner")
        meta = self.namenode.get_file_info(path, user="hdfs", group="supergroup")
        self.assertEqual(meta['owner'], "newowner")
        self.assertEqual(meta['group'], "newgroup")
        
    def test_chown_permission_denied_by_non_owner(self):
        """Verify a non-owner cannot change ownership."""
        path = "/testfile2"
        self.client.copy_from_local_string(b"data", path, user="owner", group="users")

        with self.assertRaisesRegex(PermissionDeniedException, "User 'nonowner' is not the owner"):
            self.client.chown(path, "attacker", "attackers", user="nonowner")
            
    def test_chmod_permission_denied_by_non_owner(self):
        """Verify a non-owner cannot change permissions."""
        path = "/testfile3"
        self.client.copy_from_local_string(b"data", path, user="owner", group="users")

        with self.assertRaisesRegex(PermissionDeniedException, "User 'nonowner' is not the owner"):
            self.client.chmod(path, "777", user="nonowner")

if __name__ == '__main__':
    unittest.main()