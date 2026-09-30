# === START OF FILE: main.py ===

import argparse
import logging
import time
import sys
import os

from common.config import load_config
from common.utils import setup_logging
from namenode.namenode import NameNode
from datanode.datanode import DataNode
from client.hdfs_client import HDFSClient
from secondary.checkpoint import SecondaryNameNode
from cli.hdfs_shell import HDFSShell

def main():
    """Main function to run the HDFS clone cluster."""
    parser = argparse.ArgumentParser(description="A Python clone of HDFS.")
    # The --shell argument will now be the standard way to run
    parser.add_argument('--shell', action='store_true', help='Start the HDFS cluster and run the interactive shell.')
    
    args = parser.parse_args()
    if not args.shell:
        print("Usage: python main.py --shell")
        parser.print_help()
        sys.exit(0)

    # Setup logging to a file
    os.makedirs("logs", exist_ok=True)
    logger = setup_logging("logs/hdfs_clone.log", level=logging.INFO, force_reconfigure=True)
    
    try:
        config = load_config()
    except Exception as e:
        logger.critical(f"Failed to load configuration: {e}", exc_info=True)
        sys.exit(1)

    # --- Initialize all components ---
    namenode = NameNode(config)
    
    datanodes_map = {}
    for i in range(1, config["datanode"]["num_datanodes"] + 1):
        dn_id = f"dn{i}"
        datanodes_map[dn_id] = DataNode(dn_id, config, namenode, datanodes_map)
    
    secondary_namenode = SecondaryNameNode(config, namenode)
    client = HDFSClient(namenode, datanodes_map)

    try:
        print("--- Starting HDFS Clone Cluster in the background ---")
        print("--- All logs will be written to logs/hdfs_clone.log ---")
        namenode.start()
        for dn in datanodes_map.values():
            dn.start()
        secondary_namenode.start()
        
        # Wait for the NameNode to exit safe mode before starting the shell
        while namenode.in_safe_mode:
            print("Waiting for NameNode to exit safe mode...")
            time.sleep(2)

        # Start the interactive shell
        shell = HDFSShell(client)
        shell.run()
            
    except KeyboardInterrupt:
        logger.info("\nCtrl+C received. Shutting down the cluster...")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in main: {e}", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")
        
    finally:
        print("\n--- Stopping HDFS Clone Cluster ---")
        logger.info("--- Stopping HDFS Clone Cluster ---")
        secondary_namenode.stop()
        for dn in datanodes_map.values():
            dn.stop()
        namenode.stop()
        logger.info("--- HDFS Clone Cluster has been shut down. ---")
        print("--- Cluster has been shut down. ---")

if __name__ == "__main__":
    main()

# === END OF FILE: main.py ===