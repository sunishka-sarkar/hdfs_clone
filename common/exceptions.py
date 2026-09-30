# common/exceptions.py
"""
Defines custom exception classes for the HDFS clone.
"""

class HDFSCloneException(Exception):
    """Base exception class for all custom exceptions in this project."""
    pass

class ConfigurationError(HDFSCloneException):
    """Raised for errors in configuration loading or validation."""
    pass

class MetadataError(HDFSCloneException):
    """Base class for metadata-related errors."""
    pass

class MetadataCorruptionError(MetadataError):
    """Raised when metadata files (fsimage, editlog) are found to be corrupt."""
    pass

class AtomicWriteError(MetadataError):
    """Raised when an atomic write operation fails."""
    pass

class FileNotFoundException(HDFSCloneException):
    """Raised when a file or directory is not found in the namespace."""
    pass

class FileAlreadyExistsException(HDFSCloneException):
    """Raised when trying to create a file that already exists."""
    pass

class PermissionDeniedException(HDFSCloneException):
    """Raised on permission check failures."""
    pass
# common/exceptions.py
# ... (existing exceptions) ...

class SafeModeException(HDFSCloneException):
    """Raised when a write operation is attempted while the NameNode is in Safe Mode."""
    pass

# common/exceptions.py
# ... (existing exceptions) ...

class QuotaExceededException(HDFSCloneException):
    """Raised when a write operation would exceed a namespace or disk space quota."""
    pass