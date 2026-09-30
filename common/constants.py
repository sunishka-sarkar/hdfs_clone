# common/constants.py
"""
Defines constants used throughout the application.
"""

# DataNode Status
DATANODE_STATUS_LIVE = "LIVE"
DATANODE_STATUS_STALE = "STALE"
DATANODE_STATUS_DEAD = "DEAD"

# Heartbeat configuration (in seconds)
HEARTBEAT_INTERVAL = 3
STALE_THRESHOLD = 10 * 60  # 10 minutes

# Default file system values
DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024  # 128 MB
DEFAULT_REPLICATION_FACTOR = 3
DEFAULT_FILE_PERMISSIONS = "644"
DEFAULT_DIR_PERMISSIONS = "755"

# Safe Mode
SAFE_MODE_THRESHOLD = 0.999 # 99.9% of blocks must be reported to exit safe mode

# Replication Manager
REPLICATION_MANAGER_INTERVAL = 30 # Check for under-replicated blocks every 30 seconds