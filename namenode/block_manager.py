# === START OF FILE: namenode/block_manager.py ===

import logging
import threading
import time
import random
import json
from collections import defaultdict
from typing import Dict, List, Set

from common import constants
from common.exceptions import HDFSCloneException, ConfigurationError

class BlockManager:
    """Manages all block and DataNode related metadata in the NameNode."""

    def __init__(self):
        self.logger = logging.getLogger("hdfs_clone.block_manager")
        self.lock = threading.RLock()
        
        self.block_to_datanodes: Dict[str, Set[str]] = {}
        self.datanodes: Dict[str, Dict] = {}
        
        # --- Topology State ---
        self.topology: Dict[str, List[str]] = {} # rack -> [dn1, dn2]
        self.datanode_to_rack: Dict[str, str] = {} # dn1 -> rack
        
        self.logger.info("BlockManager initialized.")

    def load_topology(self, topology_path: str = "config/topology.json"):
        """Loads the rack topology configuration from a JSON file."""
        self.logger.info(f"Loading topology from '{topology_path}'")
        try:
            with open(topology_path, 'r') as f:
                data = json.load(f)
            
            self.topology = data.get("racks", {})
            # Build the reverse map for quick lookups
            for rack, datanodes in self.topology.items():
                for dn_id in datanodes:
                    self.datanode_to_rack[dn_id] = rack
            self.logger.info(f"Topology loaded: {len(self.topology)} racks found.")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.warning(f"Could not load or parse topology file: {e}. All nodes will be on '/default-rack'.")
            self.topology = {}
            self.datanode_to_rack = {}

    def get_rack(self, datanode_id: str) -> str:
        """Returns the rack for a given datanode."""
        return self.datanode_to_rack.get(datanode_id, "/default-rack")

    def register_datanode(self, datanode_id: str):
        with self.lock:
            self.logger.info(f"Registering DataNode: '{datanode_id}'")
            if datanode_id not in self.datanodes:
                self.datanodes[datanode_id] = {}
            self.datanodes[datanode_id].update({
                "status": constants.DATANODE_STATUS_LIVE,
                "last_heartbeat": time.time()
            })
            return True

    def process_heartbeat(self, datanode_id: str):
        with self.lock:
            if datanode_id not in self.datanodes:
                self.logger.warning(f"Heartbeat from unregistered DataNode '{datanode_id}'. Registering now.")
                self.register_datanode(datanode_id)
                return
            self.logger.debug(f"Heartbeat received from '{datanode_id}'")
            self.datanodes[datanode_id]["status"] = constants.DATANODE_STATUS_LIVE
            self.datanodes[datanode_id]["last_heartbeat"] = time.time()

    def process_block_report(self, datanode_id: str, blocks: List[str]):
        with self.lock:
            self.logger.info(f"Processing block report from '{datanode_id}' with {len(blocks)} blocks.")
            for block_id in blocks:
                if block_id not in self.block_to_datanodes:
                    self.block_to_datanodes[block_id] = set()
                self.block_to_datanodes[block_id].add(datanode_id)

    def get_live_datanodes(self) -> List[str]:
        with self.lock:
            return [dn_id for dn_id, info in self.datanodes.items() if info.get("status") == constants.DATANODE_STATUS_LIVE]

    def select_datanodes_for_block(self, num_replicas: int, existing_locations: Set[str] = None, 
                                   client_node_id: str = None) -> List[str]:
        with self.lock:
            replicas = []
            if existing_locations: replicas.extend(list(existing_locations))
            nodes_to_exclude = set(replicas)
            if len(replicas) >= num_replicas: return replicas[:num_replicas]

            live_nodes = self.get_live_datanodes()
            if len(live_nodes) - len(nodes_to_exclude) < num_replicas - len(replicas):
                raise HDFSCloneException(f"Insufficient live DataNodes for {num_replicas} replicas.")

            racks = defaultdict(list)
            for node in live_nodes:
                if node not in nodes_to_exclude:
                    racks[self.get_rack(node)].append(node)

            if len(replicas) < num_replicas:
                if client_node_id and client_node_id in live_nodes and client_node_id not in nodes_to_exclude:
                    replicas.append(client_node_id)
                else:
                    available_racks = [r for r in racks if racks[r]]
                    if not available_racks: raise HDFSCloneException("No live nodes available for replica placement.")
                    random_rack = random.choice(available_racks)
                    random_node = random.choice(racks[random_rack])
                    replicas.append(random_node)
                nodes_to_exclude.add(replicas[-1])

            if len(replicas) < num_replicas:
                first_replica_rack = self.get_rack(replicas[0])
                available_racks = [r for r in racks if r != first_replica_rack and racks[r]]
                if not available_racks: available_racks = [r for r in racks if racks[r] and r == first_replica_rack]
                if not available_racks: raise HDFSCloneException("Cannot find a suitable rack for the second replica.")
                chosen_rack = random.choice(available_racks)
                nodes_on_rack = [n for n in racks[chosen_rack] if n not in nodes_to_exclude]
                if not nodes_on_rack: raise HDFSCloneException("Cannot find a suitable node for the second replica.")
                chosen_node = random.choice(nodes_on_rack)
                replicas.append(chosen_node); nodes_to_exclude.add(replicas[-1])
            
            if len(replicas) < num_replicas:
                second_replica_rack = self.get_rack(replicas[1])
                nodes_on_rack = [n for n in racks.get(second_replica_rack, []) if n not in nodes_to_exclude]
                if nodes_on_rack:
                    chosen_node = random.choice(nodes_on_rack)
                    replicas.append(chosen_node)
                else:
                    all_other_nodes = [n for n in live_nodes if n not in nodes_to_exclude]
                    if not all_other_nodes: raise HDFSCloneException("Cannot find a suitable node for the third replica.")
                    replicas.append(random.choice(all_other_nodes))
                nodes_to_exclude.add(replicas[-1])

            while len(replicas) < num_replicas:
                all_other_nodes = [n for n in live_nodes if n not in nodes_to_exclude]
                if not all_other_nodes: break
                replicas.append(random.choice(all_other_nodes)); nodes_to_exclude.add(replicas[-1])

            if len(replicas) < num_replicas:
                 self.logger.warning(f"Could only select {len(replicas)} of {num_replicas} required replicas.")
            return replicas

    def add_block_location(self, block_id: str, datanode_id: str):
        with self.lock:
            if block_id not in self.block_to_datanodes: self.block_to_datanodes[block_id] = set()
            self.block_to_datanodes[block_id].add(datanode_id)
            self.logger.info(f"Added block '{block_id}' to location '{datanode_id}'")

    def get_block_locations(self, block_id: str) -> List[str]:
        with self.lock:
            return list(self.block_to_datanodes.get(block_id, []))

    def remove_block(self, block_id: str):
        with self.lock:
            if block_id in self.block_to_datanodes:
                del self.block_to_datanodes[block_id]

    def check_datanode_health(self):
        with self.lock:
            now = time.time()
            for dn_id, info in self.datanodes.items():
                if info.get('status') == constants.DATANODE_STATUS_DEAD: continue
                time_since_heartbeat = now - info.get("last_heartbeat", 0)
                if time_since_heartbeat > constants.STALE_THRESHOLD:
                    if info.get('status') != constants.DATANODE_STATUS_DEAD:
                        self.logger.warning(f"DataNode '{dn_id}' is DEAD. Last heartbeat: {time_since_heartbeat:.2f}s ago.")
                        info['status'] = constants.DATANODE_STATUS_DEAD
                elif time_since_heartbeat > constants.HEARTBEAT_INTERVAL * 3:
                     if info.get('status') != constants.DATANODE_STATUS_STALE:
                        self.logger.warning(f"DataNode '{dn_id}' is STALE. Last heartbeat: {time_since_heartbeat:.2f}s ago.")
                        info['status'] = constants.DATANODE_STATUS_STALE

    def get_all_block_locations(self):
        with self.lock:
            return dict(self.block_to_datanodes)
            
    def is_datanode_live(self, datanode_id: str) -> bool:
        with self.lock:
            dn_info = self.datanodes.get(datanode_id)
            return dn_info and dn_info.get("status") == constants.DATANODE_STATUS_LIVE

    def get_datanode_report(self):
        with self.lock:
            report = {status: [] for status in [constants.DATANODE_STATUS_LIVE, constants.DATANODE_STATUS_STALE, constants.DATANODE_STATUS_DEAD]}
            for dn_id, info in self.datanodes.items():
                status = info.get("status", constants.DATANODE_STATUS_DEAD)
                if status in report: report[status].append(dn_id)
            return report

# === END OF FILE: namenode/block_manager.py ===```
