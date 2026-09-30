# namenode/permissions.py
"""
Handles POSIX-style permissions and ownership checks.
"""
import logging
from typing import Dict, Any

from common.exceptions import PermissionDeniedException

class PermissionChecker:
    """A class to check user permissions against file/directory metadata."""

    def __init__(self, superuser: str, supergroup: str):
        self.superuser = superuser
        self.supergroup = supergroup
        self.logger = logging.getLogger("hdfs_clone.permission_checker")

    def _check_perm_digit(self, digit: int, required_perm: str) -> bool:
        """Checks if a single permission digit grants the required permission."""
        if required_perm == 'r' and (digit & 4):
            return True
        if required_perm == 'w' and (digit & 2):
            return True
        if required_perm == 'x' and (digit & 1):
            return True
        return False

    def check(self, user: str, group: str, path: str, metadata: Dict[str, Any], required_perm: str):
        """
        Checks if a user has the required permission for a given path.

        Args:
            user (str): The username performing the action.
            group (str): The primary group of the user.
            path (str): The path being accessed.
            metadata (Dict): The metadata of the file/directory.
            required_perm (str): 'r' for read, 'w' for write, 'x' for execute.
        
        Raises:
            PermissionDeniedException: If the check fails.
        """
        if user == self.superuser:
            self.logger.debug(f"Access granted for superuser '{user}' on path '{path}'")
            return

        owner = metadata.get('owner')
        meta_group = metadata.get('group')
        permissions = metadata.get('permissions', "000")
        
        try:
            perm_digits = [int(d) for d in permissions]
        except (ValueError, TypeError):
            raise PermissionDeniedException(f"Permission mode '{permissions}' for '{path}' is invalid.")

        # Check owner permissions
        if user == owner:
            if self._check_perm_digit(perm_digits[0], required_perm):
                return

        # Check group permissions
        elif group == meta_group:
            if self._check_perm_digit(perm_digits[1], required_perm):
                return
        
        # Check other permissions
        else:
            if self._check_perm_digit(perm_digits[2], required_perm):
                return

        # If no check passed, deny access
        self.logger.warning(f"Permission denied for user '{user}' on path '{path}'. "
                          f"Required: '{required_perm}', Permissions: '{permissions}'")
        raise PermissionDeniedException(f"Permission denied for user '{user}' on '{path}'")

    def check_owner(self, user: str, metadata: Dict[str, Any]):
        """Checks if the user is the owner of the file/directory or the superuser."""
        if user == self.superuser or user == metadata.get('owner'):
            return
        raise PermissionDeniedException(f"User '{user}' is not the owner.")