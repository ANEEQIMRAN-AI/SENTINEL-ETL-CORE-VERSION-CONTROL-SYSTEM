"""
Utility functions for the data versioning system.
Includes configuration loading, logging setup, and common helpers.
"""

import os
import json
import yaml
import logging
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from logging.handlers import RotatingFileHandler


def load_config(config_path: str = "config/versioning_config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Dictionary containing configuration
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If YAML parsing fails
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML configuration: {str(e)}")


def setup_logging(config: Dict[str, Any], logger_name: str = "DataVersioning") -> logging.Logger:
    """
    Setup logging configuration based on config file.
    
    Args:
        config: Configuration dictionary
        logger_name: Name of the logger
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, config['logging']['level']))
    
    # Create logs directory if it doesn't exist
    logs_dir = config['storage']['logs_dir']
    os.makedirs(logs_dir, exist_ok=True)
    
    # File handler with rotation
    log_file = os.path.join(logs_dir, config['logging']['file'])
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=config['logging']['max_file_size'] * 1024 * 1024,
        backupCount=config['logging']['backup_count']
    )
    file_handler.setLevel(getattr(logging, config['logging']['level']))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, config['logging']['level']))
    
    # Formatter
    formatter = logging.Formatter(config['logging']['format'])
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_file_hash(file_path: str, algorithm: str = 'sha256') -> str:
    """
    Calculate hash of a file for integrity checking.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm to use (default: sha256)
        
    Returns:
        Hex digest of the file hash
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    hash_obj = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    
    return hash_obj.hexdigest()


def get_file_size(file_path: str) -> int:
    """
    Get file size in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File size in bytes
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    return os.path.getsize(file_path)


def get_timestamp() -> str:
    """
    Get current timestamp in ISO format.
    
    Returns:
        ISO format timestamp string
    """
    return datetime.now().isoformat()


def ensure_directory(directory: str) -> None:
    """
    Ensure a directory exists, create if it doesn't.
    
    Args:
        directory: Path to the directory
    """
    os.makedirs(directory, exist_ok=True)


def load_json(file_path: str) -> Dict[str, Any]:
    """
    Load JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Parsed JSON content
        
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If JSON parsing fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"JSON file not found: {file_path}")
    
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error parsing JSON: {str(e)}", e.doc, e.pos)


def save_json(data: Dict[str, Any], file_path: str, pretty: bool = True) -> None:
    """
    Save data to JSON file.
    
    Args:
        data: Dictionary to save
        file_path: Path to save the JSON file
        pretty: Whether to pretty-print the JSON
    """
    ensure_directory(os.path.dirname(file_path))
    
    with open(file_path, 'w') as f:
        if pretty:
            json.dump(data, f, indent=2, default=str)
        else:
            json.dump(data, f, default=str)


def read_text_file(file_path: str) -> str:
    """
    Read text file content.
    
    Args:
        file_path: Path to the text file
        
    Returns:
        File content as string
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Text file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        return f.read()


def write_text_file(content: str, file_path: str) -> None:
    """
    Write content to text file.
    
    Args:
        content: Content to write
        file_path: Path to save the file
    """
    ensure_directory(os.path.dirname(file_path))
    
    with open(file_path, 'w') as f:
        f.write(content)


def get_next_version_number(versions_dir: str) -> int:
    """
    Get the next version number based on existing versions.
    
    Args:
        versions_dir: Path to the versions directory
        
    Returns:
        Next version number
    """
    if not os.path.exists(versions_dir):
        return 1
    
    existing_versions = []
    for item in os.listdir(versions_dir):
        if item.startswith('v') and os.path.isdir(os.path.join(versions_dir, item)):
            try:
                version_num = int(item[1:])
                existing_versions.append(version_num)
            except ValueError:
                continue
    
    if not existing_versions:
        return 1
    
    return max(existing_versions) + 1


def validate_version_exists(version_name: str, versions_dir: str, logger: logging.Logger) -> bool:
    """
    Validate if a version exists.
    
    Args:
        version_name: Name of the version (e.g., 'v1')
        versions_dir: Path to the versions directory
        logger: Logger instance
        
    Returns:
        True if version exists, False otherwise
    """
    version_path = os.path.join(versions_dir, version_name)
    
    if not os.path.exists(version_path):
        logger.error(f"Version '{version_name}' does not exist at {version_path}")
        return False
    
    return True
