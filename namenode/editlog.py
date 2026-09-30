# namenode/editlog.py
"""
Manages the EditLog, the transaction log for all namespace modifications.
This version uses an append-only format (JSON Lines) for robustness and performance.
"""
import os
import threading
import logging
import time
import json
from typing import List, Dict, Any

from common.exceptions import MetadataCorruptionError

class EditLogManager:
    """Handles the transaction log for all namespace modifications."""

    def __init__(self, namenode_data_dir: str):
        # --- FIX: Ensure the parent directory exists on initialization ---
        os.makedirs(namenode_data_dir, exist_ok=True)
        
        self.editlog_path = os.path.join(namenode_data_dir, 'edits.json')
        self.logger = logging.getLogger("hdfs_clone.editlog")
        self._lock = threading.Lock()
        self.current_txid = self._get_last_txid()
        self.logger.info(f"EditLogManager initialized. EditLog path: '{self.editlog_path}'. Last txid: {self.current_txid}")

    def _get_last_txid(self) -> int:
        """Reads the log file to determine the last transaction ID."""
        last_id = 0
        if not os.path.exists(self.editlog_path):
            return last_id
        
        try:
            with open(self.editlog_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        tx = json.loads(line)
                        last_id = tx.get('txid', last_id)
            return last_id
        except (IOError, json.JSONDecodeError):
            self.logger.warning("Could not determine last txid from existing edit log. Assuming 0.")
            return 0

    def log_transaction(self, operation: str, **params: Any):
        """
        Logs a new transaction by appending it to the EditLog file.
        """
        with self._lock:
            self.current_txid += 1
            
            new_transaction = {
                "txid": self.current_txid,
                "operation": operation,
                "timestamp": time.time(),
                "params": params
            }
            
            try:
                # Append the new transaction as a JSON string on a new line
                with open(self.editlog_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(new_transaction) + '\n')
                
                self.logger.info(f"Successfully logged transaction ID {self.current_txid} ({operation})")

            except (IOError, OSError) as e:
                self.logger.critical(f"Failed to write transaction to EditLog! This can lead to data loss.", exc_info=True)
                # Roll back the txid increment on failure
                self.current_txid -= 1
                raise IOError(f"Failed to write to EditLog: {e}") from e

    def load(self) -> List[Dict[str, Any]]:
        """
        Loads all transactions from the EditLog by reading it line by line.
        """
        with self._lock:
            transactions = []
            if not os.path.exists(self.editlog_path):
                self.logger.info("No EditLog found to load. Returning empty transaction list.")
                return transactions
            
            try:
                with open(self.editlog_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip(): # Ignore empty lines
                            transactions.append(json.loads(line))
                
                self.logger.info(f"Loaded {len(transactions)} transactions from EditLog.")
                return transactions
            except (IOError, OSError, json.JSONDecodeError) as e:
                self.logger.critical("EditLog is corrupt or unreadable! Cannot proceed.", exc_info=True)
                raise MetadataCorruptionError(f"Failed to load transactions from {self.editlog_path}") from e

    def roll_log(self) -> str:
        """
        Rolls the current edit log to a new file and resets the transaction counter.
        """
        with self._lock:
            if not os.path.exists(self.editlog_path):
                self.logger.warning("Attempted to roll EditLog, but it does not exist.")
                return ""
                
            rolled_path = f"{self.editlog_path}.rolled_{int(time.time())}"
            self.logger.info(f"Rolling current EditLog to '{rolled_path}'")
            try:
                os.rename(self.editlog_path, rolled_path)
                # Reset the transaction ID for the new log file
                self.current_txid = 0
                return rolled_path
            except OSError as e:
                self.logger.error(f"Failed to roll EditLog: {e}", exc_info=True)
                return ""