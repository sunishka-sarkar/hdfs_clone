# === START OF FILE: cli/hdfs_shell.py ===

import sys
import os
import re
from client.hdfs_client import HDFSClient
from common.exceptions import *

class HDFSShell:
    """An interactive shell for the HDFS clone."""

    def __init__(self, client: HDFSClient):
        self.client = client
        self.current_user = "testuser" # Simulate a logged-in user
        self.current_group = "testgroup"
        self.commands = {
            "ls": self._do_ls,
            "mkdir": self._do_mkdir,
            "put": self._do_put,
            "get": self._do_get,
            "cat": self._do_cat,
            "rm": self._do_rm,
            "chmod": self._do_chmod,
            "chown": self._do_chown,
            "mv": self._do_mv,
            "count": self._do_count,
            "setQuota": self._do_set_quota,
            "clrQuota": self._do_clr_quota,
            "setSpaceQuota": self._do_set_space_quota,
            "clrSpaceQuota": self._do_clr_space_quota,
            "dfsadmin": self._do_dfsadmin,
            "whoami": self._do_whoami,
            "help": self._do_help,
            "quit": self._do_quit,
            "exit": self._do_quit,
        }

    def run(self):
        """Starts the interactive shell loop."""
        print("\n--- Welcome to the HDFS Clone Shell ---")
        print(f"Logged in as: {self.current_user} (group: {self.current_group})")
        print("Type 'help' for a list of commands.")
        while True:
            try:
                line = input("hdfs> ").strip()
                if not line: continue
                parts = line.split(); cmd, args = parts[0], parts[1:]
                if cmd in self.commands: self.commands[cmd](args)
                else: print(f"Error: Unknown command '{cmd}'")
            except (HDFSCloneException, FileNotFoundError, PermissionDeniedException) as e: print(f"Error: {e}")
            except EOFError: self._do_quit([])
            except KeyboardInterrupt: print("\nUse 'quit' or 'exit' to leave the shell."); continue

    def _do_ls(self, args):
        path = args[0] if args else "/"
        try: items = self.client.list_status(path, user=self.current_user, group=self.current_group)
        except HDFSCloneException:
            try:
                items = [self.client.get_file_info(path, user=self.current_user, group=self.current_group)]
                items[0]['path'] = path
            except (FileNotFoundException, PermissionDeniedException) as e2: print(f"ls: Cannot access '{path}': {e2}"); return
        if not items and path == "/": items = [self.client.get_file_info(path, user=self.current_user, group=self.current_group)]
        print(f"Found {len(items)} items")
        for item in sorted(items, key=lambda x: x.get('path', path)):
            is_dir = 'blocks' not in item; type_char = 'd' if is_dir else '-'
            perms = item.get('permissions', '---'); owner = item.get('owner', '-'); group = item.get('group', '-')
            size = item.get('file_size', 0); name = item.get('path', path)
            print(f"{type_char}{perms}\t{owner:<10}\t{group:<10}\t{size:>10}\t{name}")
    def _do_mkdir(self, args):
        if not args: print("Usage: mkdir <path>"); return
        self.client.mkdir(args[0], user=self.current_user, group=self.current_group); print(f"Created directory '{args[0]}'")
    def _do_put(self, args):
        if len(args) != 2: print("Usage: put <local_src> <hdfs_dst>"); return
        self.client.copy_from_local(args[0], args[1], user=self.current_user, group=self.current_group); print("Upload successful.")
    def _do_get(self, args):
        if len(args) != 2: print("Usage: get <hdfs_src> <local_dst>"); return
        self.client.copy_to_local(args[0], args[1], user=self.current_user, group=self.current_group); print("Download successful.")
    def _do_cat(self, args):
        if not args: print("Usage: cat <path>"); return
        with self.client.open(args[0], user=self.current_user, group=self.current_group) as reader:
            content = reader.read()
            try: print(content.decode('utf-8'))
            except UnicodeDecodeError: print(content)
    def _do_rm(self, args):
        if not args: print("Usage: rm <path>"); return
        self.client.delete(args[0], user=self.current_user, group=self.current_group); print(f"Removed '{args[0]}'")
    def _do_mv(self, args):
        if len(args) != 2: print("Usage: mv <src> <dst>"); return
        self.client.rename(args[0], args[1], user=self.current_user, group=self.current_group); print(f"Renamed '{args[0]}' to '{args[1]}'")
    def _do_chmod(self, args):
        if len(args) != 2: print("Usage: chmod <mode> <path>"); return
        self.client.chmod(args[1], args[0], user=self.current_user); print(f"Permissions for '{args[1]}' changed.")
    def _do_chown(self, args):
        if len(args) != 2: print("Usage: chown <owner>[:<group>] <path>"); return
        owner_group_str, path = args[0], args[1]; owner, group = None, None
        if ':' in owner_group_str: parts = owner_group_str.split(':'); owner = parts[0] or None; group = parts[1] or None
        else: owner = owner_group_str
        self.client.chown(path, owner, group, user=self.current_user); print(f"Ownership of '{path}' changed.")
    def _do_count(self, args):
        if not args: print("Usage: count <path>"); return
        summary = self.client.count(args[0], user=self.current_user, group=self.current_group)
        def fmt(v): return "inf" if v is None else str(v)
        print(f"{'NAME_QUOTA':>12} {'SPACE_QUOTA':>12} {'NAME_COUNT':>12} {'SPACE_USED':>12} PATH")
        print(f"{fmt(summary['name_quota']):>12} {fmt(summary['space_quota']):>12} {summary['name_count']:>12} {summary['space_consumed']:>12} {summary['path']}")
    def _do_set_quota(self, args):
        if len(args) != 2: print("Usage: setQuota <count> <path>"); return
        self.client.set_quota(args[1], name_quota=int(args[0]), user=self.current_user); print(f"Name quota set for '{args[1]}'")
    def _do_clr_quota(self, args):
        if not args: print("Usage: clrQuota <path>"); return
        self.client.set_quota(args[0], name_quota=-1, user=self.current_user); print(f"Name quota cleared for '{args[0]}'")
    def _do_set_space_quota(self, args):
        if len(args) != 2: print("Usage: setSpaceQuota <bytes> <path>"); return
        self.client.set_quota(args[1], space_quota=int(args[0]), user=self.current_user); print(f"Space quota set for '{args[1]}'")
    def _do_clr_space_quota(self, args):
        if not args: print("Usage: clrSpaceQuota <path>"); return
        self.client.set_quota(args[0], space_quota=-1, user=self.current_user); print(f"Space quota cleared for '{args[0]}'")
    def _do_dfsadmin(self, args):
        if '-report' in args:
            report = self.client.get_datanode_report()
            print("--- HDFS Datanode Report ---")
            for status, nodes in report.items(): print(f"{status} Datanodes ({len(nodes)}): {', '.join(sorted(nodes)) if nodes else 'None'}")
            print("-----------------------------")
        elif '-safemode' in args and 'get' in args: print(f"Safe mode is {self.client.get_safe_mode_status()}")
        else: print("Usage: dfsadmin [-report | -safemode get]")
    def _do_whoami(self, args): print(f"User: {self.current_user}, Group: {self.current_group}")
    def _do_help(self, args):
        print("Available commands:"); [print(f"  {cmd}") for cmd in sorted(self.commands.keys())]
    def _do_quit(self, args): raise EOFError

# === END OF FILE: cli/hdfs_shell.py ===