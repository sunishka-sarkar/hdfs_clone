# === START OF FILE: cli.py ===

import sys
import os
import logging
from xmlrpc.client import ServerProxy, Fault

# --- FIX: Add project root to Python path ---
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from common.config import load_config


def print_usage():
    print("\nUsage: python cli.py --namenode <namenode_ip> <command> [args...]")
    print("\nSupported commands:")
    print("  ls <path>               - List contents of a directory.")
    print("  dfsadmin -report        - Show a report of live datanodes.")
    print("  put <local> <remote>    - Upload file to HDFS clone.")
    print("  get <remote> <local>    - Download file from HDFS clone.")
    print("  cat <remote>            - Display text file contents.")
    print("\nExample:")
    print("  python cli.py --namenode 10.1.2.21 ls /")
    print("  python cli.py --namenode 10.1.2.21 put mydata.txt /userdata/files/mydata.txt")
    print("  python cli.py --namenode 10.1.2.21 get /userdata/files/mydata.txt local_copy.txt")
    print("  python cli.py --namenode 10.1.2.21 cat /userdata/files/mydata.txt\n")


def main():
    """The entry point for the command-line interface."""
    logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

    try:
        config = load_config()

        # --- Require the NameNode IP argument ---
        if len(sys.argv) < 3 or sys.argv[1] != '--namenode':
            print("Error: You must specify the NameNode's IP address.")
            print_usage()
            sys.exit(1)

        namenode_ip = sys.argv[2]
        args = sys.argv[3:]

        namenode_port = config['namenode']['port']
        namenode_rpc = ServerProxy(f"http://{namenode_ip}:{namenode_port}", allow_none=True)

        if not args:
            print_usage()
            sys.exit(1)

        cmd, cmd_args = args[0], args[1:]
        user, group = "cli_user", "cli_group"

        # ========================================================
        #                 BASIC FILESYSTEM COMMANDS
        # ========================================================

        if cmd == 'ls':
            path = cmd_args[0] if cmd_args else "/"
            items = namenode_rpc.list_status(path, user, group)
            print(f"Found {len(items)} items:")
            for item in sorted(items, key=lambda x: x['path']):
                is_dir = 'blocks' not in item
                perms = item.get('permissions', '---')
                owner = item.get('owner', '-')
                grp = item.get('group', '-')
                size = item.get('file_size', 0)
                print(f"{'d' if is_dir else '-'}{perms}\t{owner}:{grp}\t{size:>10}\t{item['path']}")

        elif cmd == 'dfsadmin' and '-report' in cmd_args:
            report = namenode_rpc.get_datanode_report()
            print("--- HDFS Datanode Report ---")
            for status, nodes in report.items():
                print(f"{status} Datanodes ({len(nodes)}): {', '.join(sorted(nodes)) if nodes else 'None'}")
            print("-----------------------------")

        # ========================================================
        #                 UPLOAD (PUT) COMMAND
        # ========================================================

        elif cmd == 'put':
            if len(cmd_args) != 2:
                print("Usage: put <local_file> <remote_path>")
                sys.exit(1)

            local_file, remote_path = cmd_args
            if not os.path.exists(local_file):
                print(f"❌ Local file not found: {local_file}")
                sys.exit(1)

            print(f"📤 Uploading {local_file} to {remote_path} ...")
            with open(local_file, "rb") as f:
                data_hex = f.read().hex()

            result = namenode_rpc.upload_file(remote_path, data_hex)
            print(result)

        # ========================================================
        #                 DOWNLOAD (GET) COMMAND
        # ========================================================

        elif cmd == 'get':
            if len(cmd_args) != 2:
                print("Usage: get <remote_path> <local_file>")
                sys.exit(1)

            remote_path, local_file = cmd_args
            print(f"📥 Downloading {remote_path} to {local_file} ...")
            data_hex = namenode_rpc.download_file(remote_path)

            if not data_hex:
                print(f"❌ Failed to download file: {remote_path}")
                sys.exit(1)

            with open(local_file, "wb") as f:
                f.write(bytes.fromhex(data_hex))

            print(f"✅ Downloaded {remote_path} → {local_file}")

        # ========================================================
        #                 VIEW FILE (CAT) COMMAND
        # ========================================================

        elif cmd == 'cat':
            if len(cmd_args) != 1:
                print("Usage: cat <remote_path>")
                sys.exit(1)

            remote_path = cmd_args[0]
            print(f"📖 Reading {remote_path} ...")
            data_hex = namenode_rpc.download_file(remote_path)

            if not data_hex:
                print(f"❌ File not found: {remote_path}")
                sys.exit(1)

            content = bytes.fromhex(data_hex).decode("utf-8", errors="ignore")
            print(content)

        else:
            print(f"Error: Command '{cmd}' is not supported in this client.")
            print_usage()

    except Fault as f:
        print(f"Error from NameNode: {f.faultString}")
    except ConnectionRefusedError:
        print(f"Error: Connection refused. Is the NameNode running at {namenode_ip}?")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()

# === END OF FILE: cli.py ===
