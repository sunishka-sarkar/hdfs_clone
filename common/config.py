# common/config.py
"""
Manages loading and accessing configuration settings.
"""
import json
from typing import Dict, Any

from .exceptions import ConfigurationError

def load_config(config_path: str = "config/hdfs-config.json") -> Dict[str, Any]:
    """
    Loads the main configuration file.

    Args:
        config_path (str): The path to the configuration file.

    Returns:
        Dict: A dictionary containing the configuration.

    Raises:
        ConfigurationError: If the file is not found, cannot be read, or is invalid JSON.
    """
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise ConfigurationError(f"Configuration file not found at '{config_path}'")
    except json.JSONDecodeError as e:
        raise ConfigurationError(f"Invalid JSON in configuration file '{config_path}': {e}") from e
    except (IOError, OSError) as e:
        raise ConfigurationError(f"Could not read configuration file '{config_path}': {e}") from e