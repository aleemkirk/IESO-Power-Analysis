"""
Configuration loader for IESO Power Analysis.

This module provides utilities to load and access configuration values
from the settings.yaml file.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Cache for configuration to avoid repeated file reads
_config_cache: Optional[Dict[str, Any]] = None


def load_config(force_reload: bool = False) -> Dict[str, Any]:
    """
    Load configuration from YAML file with caching.

    Args:
        force_reload: If True, reload config even if cached

    Returns:
        Dict containing all configuration values

    Raises:
        FileNotFoundError: If settings.yaml doesn't exist
        yaml.YAMLError: If YAML file is malformed
    """
    global _config_cache

    if _config_cache is None or force_reload:
        config_path = Path(__file__).parent / 'settings.yaml'

        if not config_path.exists():
            raise FileNotFoundError(
                f"Configuration file not found: {config_path}"
            )

        logger.info(f"Loading configuration from {config_path}")

        with open(config_path, 'r') as f:
            _config_cache = yaml.safe_load(f)

        logger.info("Configuration loaded successfully.")

    return _config_cache


def get_config(key_path: str, default: Any = None) -> Any:
    """
    Get configuration value by dot-notation path.

    Supports nested key access using dot notation (e.g., 'ieso.base_url').

    Args:
        key_path: Dot-separated path to config value (e.g., 'database.schemas.raw')
        default: Default value if key not found

    Returns:
        Configuration value or default if not found

    Examples:
        >>> get_config('ieso.base_url')
        'https://reports-public.ieso.ca/public/'

        >>> get_config('database.schemas.raw')
        '00_RAW'

        >>> get_config('nonexistent.key', default='fallback')
        'fallback'
    """
    config = load_config()
    keys = key_path.split('.')
    value = config

    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
            if value is None:
                logger.debug(f"Config key '{key_path}' not found, using default: {default}")
                return default
        else:
            logger.debug(f"Config key '{key_path}' not found (non-dict encountered), using default: {default}")
            return default

    return value


def get_ieso_url(endpoint_key: str) -> str:
    """
    Construct full IESO API URL for a given endpoint.

    Args:
        endpoint_key: Key for endpoint in config (e.g., 'demand', 'zonal_demand')

    Returns:
        Full URL to IESO API endpoint

    Example:
        >>> get_ieso_url('demand')
        'https://reports-public.ieso.ca/public/Demand/PUB_Demand.csv'
    """
    base_url = get_config('ieso.base_url')
    endpoint = get_config(f'ieso.endpoints.{endpoint_key}')

    if not base_url or not endpoint:
        raise ValueError(
            f"Invalid endpoint configuration for '{endpoint_key}'. "
            "Check settings.yaml."
        )

    return f"{base_url}{endpoint}"


def get_table_name(table_key: str) -> str:
    """
    Get table name from configuration.

    Args:
        table_key: Key for table in config (e.g., 'demand', 'zonal_demand')

    Returns:
        Table name

    Example:
        >>> get_table_name('demand')
        '00_IESO_DEMAND'
    """
    table_name = get_config(f'database.tables.{table_key}')
    if not table_name:
        raise ValueError(f"Table '{table_key}' not found in configuration.")
    return table_name


def get_schema_name(schema_key: str) -> str:
    """
    Get schema name from configuration.

    Args:
        schema_key: Key for schema in config (e.g., 'raw', 'primary', 'reference')

    Returns:
        Schema name

    Example:
        >>> get_schema_name('raw')
        '00_RAW'
    """
    schema_name = get_config(f'database.schemas.{schema_key}')
    if not schema_name:
        raise ValueError(f"Schema '{schema_key}' not found in configuration.")
    return schema_name
