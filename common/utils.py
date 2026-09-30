# === START OF FILE: common/utils.py ===

import os
import json
import logging
import uuid
import threading
from typing import Dict, Any

from .exceptions import AtomicWriteError, MetadataCorruptionError

logging_lock = threading.Lock()

def load_config(config_path="config/hdfs-config.json"):
    """Loads configuration file for cluster components."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    
    with open(config_path, "r") as f:
        config = json.load(f)
    
    return config

def setup_logging(log_file: str, level: int = logging.INFO, force_reconfigure: bool = False) -> logging.Logger:
    """
    Sets up a centralized logger to write ONLY to a file.
    """
    with logging_lock:
        logger = logging.getLogger("hdfs_clone")
        
        if force_reconfigure and logger.hasHandlers():
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

        if not logger.handlers:
            logger.setLevel(level)
            
            log_dir = os.path.dirname(log_file)
            if log_dir:
                os.makedirs(log_dir, exist_ok=True)

            # Create a file handler to write logs to a file
            file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setLevel(level)

            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s'
            )
            file_handler.setFormatter(formatter)

            # Add ONLY the file handler to the logger
            logger.addHandler(file_handler)
            
            # --- CONSOLE HANDLER REMOVED ---
            # The section that added a StreamHandler has been deleted.

    return logger

def atomic_write_json(file_path: str, data: Dict[str, Any], logger: logging.Logger):
    """
    Atomically writes a dictionary to a JSON file.
    """
    temp_path = f"{file_path}.{uuid.uuid4()}.tmp"
    logger.debug(f"Starting atomic write to '{file_path}' via temp file '{temp_path}'")
    
    try:
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
            
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        
        os.rename(temp_path, file_path)
        logger.info(f"Successfully committed atomic write to '{file_path}'")

    except (IOError, OSError) as e:
        logger.error(f"Atomic write to '{file_path}' failed: {e}", exc_info=True)
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.warning(f"Cleaned up temporary file '{temp_path}' after failure.")
            except OSError as cleanup_e:
                logger.error(f"Failed to clean up temp file '{temp_path}': {cleanup_e}", exc_info=True)
        raise AtomicWriteError(f"Failed to atomically write to {file_path}") from e

def safe_read_json(file_path: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Safely reads a JSON file with detailed error handling.
    """
    logger.debug(f"Attempting to safely read JSON file: '{file_path}'")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in '{file_path}': File may be corrupt.", exc_info=True)
        raise MetadataCorruptionError(f"Failed to decode JSON from {file_path}") from e
    except (IOError, OSError) as e:
        logger.error(f"I/O error reading '{file_path}': Check permissions or disk health.", exc_info=True)
        raise MetadataCorruptionError(f"Could not read file {file_path}") from e

# === END OF FILE: common/utils.py ===