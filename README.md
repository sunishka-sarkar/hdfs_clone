# HDFS Clone in Python

This project is a comprehensive implementation of the Hadoop Distributed File System (HDFS) in Python. It emulates the core functionalities of HDFS using a master-slave architecture with a NameNode, DataNodes, and a client, all running on a single machine.

## Architecture
- **NameNode**: Manages the filesystem namespace and metadata.
- **DataNodes**: Store the actual data blocks.
- **Client**: Provides an interface for user operations.

## Features
- Block storage and replication
- Fault tolerance with heartbeats and block reports
- Checkpointing with a Secondary NameNode
- POSIX-like permissions and ACLs
- Namespace and disk space quotas
- A command-line interface for interaction

## Getting Started

### Prerequisites
- Python 3.8+

### Installation
1. Clone the repository:
   \\\ash
   git clone <repository-url>
   cd hdfs-clone
   \\\
2. Install dependencies:
   \\\ash
   pip install -r requirements.txt
   \\\

### Usage
- To start the cluster: \python main.py --start\
- To use the interactive shell: \python main.py --shell\

## Configuration
Configuration settings can be found in \config/hdfs-config.json\.
