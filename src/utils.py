"""
utils.py

This module contains utility functions for the data pipeline.
It includes functions to load configuration files and set up logging.

Functions:
- load_config: Load configuration from a JSON file.
- get_logger: Get a logger with the specified name.
"""

import json
import logging
from typing import Dict

def load_config(config_path: str) -> Dict:
    """
    Load configuration from a JSON file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        dict: Configuration dictionary.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError as e:
        logging.error("Configuration file not found: %s - %s", config_path, e)
        raise
    except json.JSONDecodeError as e:
        logging.error("Error decoding JSON from configuration file: %s - %s", config_path, e)
        raise
    except Exception as e:
        logging.error("Unexpected error loading configuration file: %s - %s", config_path, e)
        raise

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.

    Args:
        name (str): Name of the logger.

    Returns:
        Logger: Configured logger.
    """
    try:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # Create a file handler
        handler = logging.FileHandler('logs/app.log')
        handler.setLevel(logging.DEBUG)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(handler)

        return logger
    except Exception as e:
        logging.error("Error setting up logger: %s - %s", name, e)
        raise
