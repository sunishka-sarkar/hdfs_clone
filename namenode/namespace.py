# === START OF FILE: namenode/namespace.py ===

import logging
import threading
import time
import uuid
import os
from typing import Dict, Any, List

from .fsimage import FsImageManager
from .editlog import EditLogManager
from .block_manager import BlockManager
from .permissions import PermissionChecker
from common import constants
from common.exceptions import *

class NamespaceManager:
    """
    Manages the entire filesystem namespace in-memory.
    """
    def __init__(self, fsimage_manager: FsImageManager, editlog_manager: EditLogManager, 
                 block_manager: BlockManager, namenode_instance, config: Dict[str, Any]):
        self.logger = logging.getLogger("hdfs_clone.namespace")
        self.fsimage_manager = fsimage_manager
        self.editlog_manager = editlog_manager
        self.block_manager = block_manager
        self.namenode = namenode_instance
        superuser = config['filesystem'].get('permissions_superuser', 'hdfs')
        self.permission_checker = PermissionChecker(superuser, 'supergroup')
        self.lock = threading.RLock()
        self.namespace: Dict[str, Any] = {}
        self.block_to_path: Dict[str, str] = {}
        self.quota_usage: Dict[str, Dict[str, int]] = {}

    def load(self):
        with self.lock:
            self.logger.info("Loading namespace from FsImage...")
            self.namespace = self.fsimage_manager.load()
            self.logger.info("Replaying transactions from EditLog...")
            transactions = self.editlog_manager.load()
            for tx in transactions: self._apply_transaction(tx)
            self.logger.info(f"Namespace loaded and {len(transactions)} transactions replayed.")
            self.logger.info("Building block-to-path map from loaded namespace...")
            self.block_to_path.clear()
            for path, meta in self.namespace.get('files', {}).items():
                for block_id in meta.get('blocks', []): self.block_to_path[block_id] = path
            self.logger.info(f"Block-to-path map built with {len(self.block_to_path)} entries.")
            self.logger.info("Calculating initial quota usage..."); self._recalculate_all_quotas(); self.logger.info("Initial quota usage calculated.")

    def _apply_transaction(self, tx: Dict[str, Any]):
        op, params = tx.get("operation"), tx.get("params", {})
        if op == "MKDIR": self._mkdir_internal(**params)
        elif op == "CREATE": self._create_internal(**params)
        elif op == "ADD_BLOCK":
            meta = self.namespace["files"][params["path"]]; meta["blocks"].append(params["block_id"])
            for dn_id in params["locations"]: self.block_manager.add_block_location(params["block_id"], dn_id)
        elif op == "COMPLETE_FILE": self.namespace["files"][params["path"]]["file_size"] = params["file_size"]
        elif op == "DELETE": self._delete_internal(**params)
        elif op == "RENAME": self._rename_internal(**params)
        elif op == "CHOWN": meta = self._get_metadata(params["path"]); meta.update({'owner': params['owner'], 'group': params['group']})
        elif op == "CHMOD": meta = self._get_metadata(params["path"]); meta['permissions'] = params['permissions']
        elif op == "SET_QUOTA":
            meta = self._get_metadata(params["path"])
            if 'quota' not in meta: meta['quota'] = {}; meta['quota']['name_quota'] = params.get('name_quota'); meta['quota']['space_quota'] = params.get('space_quota')
        else: self.logger.warning(f"Unknown transaction operation during replay: '{op}'")

    def _recalculate_all_quotas(self):
        self.quota_usage.clear()
        all_paths = list(self.namespace.get('directories', {}).keys()) + list(self.namespace.get('files', {}).keys())
        for path in all_paths: self.quota_usage[path] = {"name_count": 0, "space_consumed": 0}
        for path in self.namespace.get('directories', {}): self.quota_usage[path]["name_count"] = 1
        for path, meta in self.namespace.get('files', {}).items():
            self.quota_usage[path]["name_count"] = 1; self.quota_usage[path]["space_consumed"] = meta.get('file_size', 0) * meta.get('replication', 0)
        for path in sorted(self.quota_usage.keys(), key=len, reverse=True):
            parent = self._get_parent_path(path)
            if parent and parent in self.quota_usage:
                self.quota_usage[parent]["name_count"] += self.quota_usage[path]["name_count"]
                self.quota_usage[parent]["space_consumed"] += self.quota_usage[path]["space_consumed"]

    def _update_quota(self, path: str, name_delta: int, space_delta: int):
        current_path = path
        while current_path:
            if current_path in self.quota_usage:
                self.quota_usage[current_path]["name_count"] += name_delta; self.quota_usage[current_path]["space_consumed"] += space_delta
            current_path = self._get_parent_path(current_path)

    def _check_quotas(self, path: str, name_delta: int, space_delta: int):
        current_path = path
        while current_path:
            meta = self.namespace['directories'].get(current_path)
            if meta and 'quota' in meta:
                usage = self.quota_usage.get(current_path, {"name_count": 0, "space_consumed": 0})
                name_quota = meta['quota'].get('name_quota')
                if name_quota is not None and (usage["name_count"] + name_delta > name_quota): raise QuotaExceededException(f"Name quota exceeded for '{current_path}'. Limit: {name_quota}")
                space_quota = meta['quota'].get('space_quota')
                if space_quota is not None and (usage["space_consumed"] + space_delta > space_quota): raise QuotaExceededException(f"Space quota exceeded for '{current_path}'. Limit: {space_quota}")
            current_path = self._get_parent_path(current_path)

    def _get_parent_path(self, path: str) -> str:
        if path == "/": return None
        return os.path.dirname(path) or "/"
        
    def _path_exists(self, path: str) -> bool: return path in self.namespace["files"] or path in self.namespace["directories"]
    def _get_metadata(self, path: str, expect: str = None) -> Dict[str, Any]:
        if path in self.namespace["files"]:
            if expect == 'dir': raise HDFSCloneException(f"Path is a file, expected a directory: {path}")
            return self.namespace["files"][path]
        if path in self.namespace["directories"]:
            if expect == 'file': raise HDFSCloneException(f"Path is a directory, expected a file: {path}")
            return self.namespace["directories"][path]
        raise FileNotFoundException(f"Path '{path}' not found.")

    def mkdir(self, path: str, permissions: str, user: str, group: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot mkdir. NameNode is in Safe Mode.")
        with self.lock:
            parent = self._get_parent_path(path)
            if not parent: raise PermissionDeniedException("Cannot create directory at root.")
            if not self._path_exists(parent): raise FileNotFoundException(f"Parent directory '{parent}' does not exist.")
            parent_meta = self._get_metadata(parent); self.permission_checker.check(user, group, parent, parent_meta, 'w')
            if self._path_exists(path): raise FileAlreadyExistsException(f"Path '{path}' already exists.")
            self._check_quotas(parent, name_delta=1, space_delta=0)
            self.editlog_manager.log_transaction("MKDIR", path=path, permissions=permissions, owner=user, group=group)
            self._mkdir_internal(path, permissions, user, group)
            self.quota_usage[path] = {"name_count": 1, "space_consumed": 0}; self._update_quota(parent, name_delta=1, space_delta=0)
            return True
            
    def _mkdir_internal(self, path: str, permissions: str, owner: str, group: str):
        self.namespace["directories"][path] = {"permissions": permissions, "owner": owner, "group": group, "modification_time": time.time()}

    def create(self, path: str, replication: int, block_size: int, permissions: str, user: str, group: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot create file. NameNode is in Safe Mode.")
        with self.lock:
            parent = self._get_parent_path(path)
            if not self._path_exists(parent): raise FileNotFoundException(f"Parent directory '{parent}' does not exist.")
            parent_meta = self._get_metadata(parent); self.permission_checker.check(user, group, parent, parent_meta, 'w')
            if self._path_exists(path): raise FileAlreadyExistsException(f"Path '{path}' already exists.")
            self._check_quotas(parent, name_delta=1, space_delta=0)
            self.editlog_manager.log_transaction("CREATE", path=path, permissions=permissions, replication=replication, block_size=block_size, owner=user, group=group)
            self._create_internal(path, permissions, replication, block_size, user, group)
            self.quota_usage[path] = {"name_count": 1, "space_consumed": 0}; self._update_quota(parent, name_delta=1, space_delta=0)
            return True
            
    def _create_internal(self, path, permissions, replication, block_size, owner, group):
        self.namespace["files"][path] = {"permissions": permissions, "replication": replication, "block_size": block_size, "owner": owner, "group": group, "blocks": [], "file_size": 0, "creation_time": time.time(), "modification_time": time.time()}

    # --- FIX: Re-added the missing method ---
    def get_block_locations_for_file(self, path: str, user: str, group: str) -> List[Dict[str, Any]]:
        with self.lock:
            meta = self._get_metadata(path, expect='file')
            self.permission_checker.check(user, group, path, meta, 'r')
            
            block_locations = []
            for block_id in meta["blocks"]:
                locations = self.block_manager.get_block_locations(block_id)
                block_locations.append({ "block_id": block_id, "locations": locations })
            return block_locations
    
    def add_block_to_file(self, path: str, user: str, group: str) -> Dict[str, Any]:
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot add block. NameNode is in Safe Mode.")
        with self.lock:
            meta = self._get_metadata(path, expect='file'); self.permission_checker.check(user, group, path, meta, 'w')
            space_needed = meta['block_size'] * meta['replication']
            self._check_quotas(path, name_delta=0, space_delta=space_needed)
            block_id = f"blk_{uuid.uuid4()}"; locations = self.block_manager.select_datanodes_for_block(meta["replication"])
            self.editlog_manager.log_transaction("ADD_BLOCK", path=path, block_id=block_id, locations=locations)
            meta["blocks"].append(block_id); self.block_to_path[block_id] = path
            for dn_id in locations: self.block_manager.add_block_location(block_id, dn_id)
            return {"block_id": block_id, "locations": locations}

    def complete_file(self, path: str, file_size: int, user: str, group: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot complete file. NameNode is in Safe Mode.")
        with self.lock:
            meta = self._get_metadata(path, expect='file'); self.permission_checker.check(user, group, path, meta, 'w')
            current_space_usage = self.quota_usage[path]['space_consumed']; space_consumed_final = file_size * meta['replication']; space_delta = space_consumed_final - current_space_usage
            self._check_quotas(path, name_delta=0, space_delta=space_delta)
            self._update_quota(self._get_parent_path(path), name_delta=0, space_delta=space_delta)
            self.quota_usage[path]['space_consumed'] = space_consumed_final
            self.editlog_manager.log_transaction("COMPLETE_FILE", path=path, file_size=file_size)
            meta["file_size"] = file_size; meta["modification_time"] = time.time()
            return True

    def _delete_internal(self, path: str):
        usage = self.quota_usage.pop(path, {"name_count": 0, "space_consumed": 0})
        if path in self.namespace['files']:
            file_meta = self.namespace['files'].pop(path)
            for block_id in file_meta.get('blocks', []):
                self.block_manager.remove_block(block_id)
                if block_id in self.block_to_path: del self.block_to_path[block_id]
        elif path in self.namespace['directories']: self.namespace['directories'].pop(path)
        parent = self._get_parent_path(path)
        if parent: self._update_quota(parent, name_delta=-usage["name_count"], space_delta=-usage["space_consumed"])

    def delete(self, path: str, user: str, group: str, recursive: bool = False):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot delete. NameNode is in Safe Mode.")
        with self.lock:
            if path == "/": raise PermissionDeniedException("Cannot delete root.")
            meta = self._get_metadata(path); parent = self._get_parent_path(path); parent_meta = self._get_metadata(parent); self.permission_checker.check(user, group, parent, parent_meta, 'w')
            if path in self.namespace['directories']:
                is_empty = not any(p.startswith(f"{path}/") for p in list(self.namespace['files']) + list(self.namespace['directories']))
                if not is_empty and not recursive: raise HDFSCloneException(f"Directory '{path}' is not empty.")
                if not is_empty and recursive: raise HDFSCloneException("Recursive delete not implemented.")
            self.editlog_manager.log_transaction("DELETE", path=path)
            self._delete_internal(path=path)
            return True

    def rename(self, src_path: str, dst_path: str, user: str, group: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot rename. NameNode is in Safe Mode.")
        with self.lock:
            if not self._path_exists(src_path): raise FileNotFoundException(f"Source path does not exist: {src_path}")
            if self._path_exists(dst_path): raise FileAlreadyExistsException(f"Destination path already exists: {dst_path}")
            if dst_path.startswith(src_path + '/'): raise HDFSCloneException(f"Cannot move '{src_path}' into itself.")
            if src_path == "/": raise PermissionDeniedException("Cannot rename root.")
            src_parent = self._get_parent_path(src_path); src_parent_meta = self._get_metadata(src_parent); self.permission_checker.check(user, group, src_parent, src_parent_meta, 'w')
            dst_parent = self._get_parent_path(dst_path)
            if not self._path_exists(dst_parent): raise FileNotFoundException(f"Destination parent does not exist: {dst_parent}")
            dst_parent_meta = self._get_metadata(dst_parent); self.permission_checker.check(user, group, dst_parent, dst_parent_meta, 'w')
            src_usage = self.quota_usage.get(src_path, {"name_count": 0, "space_consumed": 0})
            self._check_quotas(dst_parent, name_delta=src_usage['name_count'], space_delta=src_usage['space_consumed'])
            self.editlog_manager.log_transaction("RENAME", src_path=src_path, dst_path=dst_path)
            self._rename_internal(src_path, dst_path)
            self._update_quota(src_parent, name_delta=-src_usage['name_count'], space_delta=-src_usage['space_consumed'])
            self._update_quota(dst_parent, name_delta=src_usage['name_count'], space_delta=src_usage['space_consumed'])
            return True

    def _rename_internal(self, src_path: str, dst_path: str):
        if src_path in self.namespace['files']:
            item = self.namespace['files'].pop(src_path); self.namespace['files'][dst_path] = item
            for block_id in item.get('blocks', []): self.block_to_path[block_id] = dst_path
            if src_path in self.quota_usage: self.quota_usage[dst_path] = self.quota_usage.pop(src_path)
            return
        if src_path in self.namespace['directories']:
            item = self.namespace['directories'].pop(src_path); self.namespace['directories'][dst_path] = item
            if src_path in self.quota_usage: self.quota_usage[dst_path] = self.quota_usage.pop(src_path)
            children_to_rename = {}
            for path in list(self.namespace['files']):
                if path.startswith(src_path + '/'): children_to_rename[path] = 'file'
            for path in list(self.namespace['directories']):
                if path.startswith(src_path + '/'): children_to_rename[path] = 'dir'
            for old_path, item_type in children_to_rename.items():
                new_path = old_path.replace(src_path, dst_path, 1)
                if item_type == 'file':
                    child_item = self.namespace['files'].pop(old_path); self.namespace['files'][new_path] = child_item
                    for block_id in child_item.get('blocks', []): self.block_to_path[block_id] = new_path
                else: self.namespace['directories'][new_path] = self.namespace['directories'].pop(old_path)
                if old_path in self.quota_usage: self.quota_usage[new_path] = self.quota_usage.pop(old_path)

    def set_quota(self, path: str, name_quota: int, space_quota: int, user: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot set quota. NameNode is in Safe Mode.")
        with self.lock:
            meta = self._get_metadata(path, expect='dir'); self.permission_checker.check_owner(user, meta)
            if 'quota' not in meta: meta['quota'] = {}
            if name_quota is not None: meta['quota']['name_quota'] = name_quota if name_quota > -1 else None
            if space_quota is not None: meta['quota']['space_quota'] = space_quota if space_quota > -1 else None
            self.editlog_manager.log_transaction("SET_QUOTA", path=path, name_quota=meta['quota'].get('name_quota'), space_quota=meta['quota'].get('space_quota'))
            return True

    def get_content_summary(self, path: str, user: str, group: str):
        with self.lock:
            meta = self._get_metadata(path); self.permission_checker.check(user, group, path, meta, 'r')
            usage = self.quota_usage.get(path, {"name_count": 0, "space_consumed": 0})
            return {"path": path, "name_count": usage.get('name_count', 0), "space_consumed": usage.get('space_consumed', 0), "name_quota": meta.get('quota', {}).get('name_quota'), "space_quota": meta.get('quota', {}).get('space_quota')}
            
    def set_owner(self, path: str, owner: str, group: str, user: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot set owner. NameNode is in Safe Mode.")
        with self.lock:
            meta = self._get_metadata(path); self.permission_checker.check_owner(user, meta)
            new_owner = owner if owner else meta['owner']; new_group = group if group else meta['group']
            self.editlog_manager.log_transaction("CHOWN", path=path, owner=new_owner, group=new_group)
            meta['owner'] = new_owner; meta['group'] = new_group; meta['modification_time'] = time.time()
            return True

    def set_permissions(self, path: str, mode: str, user: str):
        if self.namenode.in_safe_mode: raise SafeModeException("Cannot set permission. NameNode is in Safe Mode.")
        with self.lock:
            meta = self._get_metadata(path); self.permission_checker.check_owner(user, meta)
            if not (len(mode) == 3 and mode.isdigit() and all('0' <= char <= '7' for char in mode)): raise HDFSCloneException(f"Invalid mode: '{mode}'.")
            self.editlog_manager.log_transaction("CHMOD", path=path, permissions=mode)
            meta['permissions'] = mode; meta['modification_time'] = time.time()
            return True
            
    def get_file_info(self, path: str, user: str, group: str) -> Dict[str, Any]:
        with self.lock:
            meta = self._get_metadata(path); self.permission_checker.check(user, group, path, meta, 'r')
            return meta

    def list_status(self, path: str, user: str, group: str) -> List[Dict[str, Any]]:
        with self.lock:
            dir_meta = self._get_metadata(path, expect='dir'); self.permission_checker.check(user, group, path, dir_meta, 'x')
            results = []
            for p, meta in list(self.namespace['files'].items()) + list(self.namespace['directories'].items()):
                if self._get_parent_path(p) == path: results.append({'path': p, **meta})
            return results
            
    def get_total_block_count(self) -> int:
        with self.lock: return len(self.block_to_path)

    def get_block_to_path_map(self):
        with self.lock: return dict(self.block_to_path)

    def get_file_metadata_internal(self, path: str):
        with self.lock: return self.namespace['files'].get(path)

# === END OF FILE: namenode/namespace.py ===