"""Configuration management utilities."""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """Load configuration from YAML file.

    Args:
        config_path: Path to config file

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file is invalid YAML
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Please copy config.yaml.example to config.yaml and fill in your settings."
        )

    logger.info(f"Loading configuration from {config_path}")

    with open(config_file) as f:
        config = yaml.safe_load(f)

    logger.info("Configuration loaded successfully")
    return config


def validate_config(config: dict[str, Any]) -> None:
    """Validate required configuration fields.

    Args:
        config: Configuration dictionary

    Raises:
        ValueError: If required fields are missing or invalid
    """
    required_fields = {
        "api": ["base_url", "api_key"],
        "database": ["path"],
    }

    for section, fields in required_fields.items():
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")

        for field in fields:
            if field not in config[section]:
                raise ValueError(f"Missing required config field: {section}.{field}")

    # Validate API key is not the default placeholder
    if config["api"]["api_key"] == "your-api-key-here":
        raise ValueError("Please update config.yaml with your actual API key")

    logger.info("Configuration validated successfully")
