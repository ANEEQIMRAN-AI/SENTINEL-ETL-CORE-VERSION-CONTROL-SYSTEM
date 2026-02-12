"""
Core versioning logic for the data versioning system.
Handles version creation, metadata generation, and index management.
"""

import os
import csv
import shutil
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from utils import (
    load_config, get_file_hash, get_file_size, get_timestamp,
    ensure_directory, load_json, save_json, get_next_version_number,
    validate_version_exists, read_text_file, write_text_file
)


class VersionManager:
    """
    Manages dataset versioning including creation, indexing, and metadata.
    """
    
    def __init__(self, config_path: str = "config/versioning_config.yaml", logger: Optional[logging.Logger] = None):
        """
        Initialize the VersionManager.
        
        Args:
            config_path: Path to the configuration file
            logger: Logger instance (will be created if not provided)
        """
        self.config = load_config(config_path)
        self.logger = logger or logging.getLogger("VersionManager")
        
        # Set up directory paths
        self.versions_dir = self.config['storage']['versions_dir']
        self.processed_data_dir = self.config['storage']['processed_data_dir']
        self.index_file = self.config['storage']['index_file']
        self.current_version_file = self.config['storage']['current_version_file']
        
        # Ensure directories exist
        ensure_directory(self.versions_dir)
        ensure_directory(self.processed_data_dir)
        
        self.logger.info("VersionManager initialized")
    
    def create_version(self, input_file: str, quality_score: Optional[float] = None) -> str:
        """
        Create a new version from a processed dataset.
        
        Args:
            input_file: Path to the processed dataset file
            quality_score: Optional data quality score (0-100)
            
        Returns:
            Version name (e.g., 'v1')
            
        Raises:
            FileNotFoundError: If input file doesn't exist
            ValueError: If quality_score is invalid
        """
        if not os.path.exists(input_file):
            self.logger.error(f"Input file not found: {input_file}")
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        if quality_score is not None and not (0 <= quality_score <= 100):
            self.logger.error(f"Invalid quality score: {quality_score}")
            raise ValueError("Quality score must be between 0 and 100")
        
        # Get next version number
        version_number = get_next_version_number(self.versions_dir)
        version_name = f"v{version_number}"
        version_path = os.path.join(self.versions_dir, version_name)
        
        # Create version directory
        ensure_directory(version_path)
        
        # Copy dataset to version directory
        dataset_filename = os.path.basename(input_file)
        versioned_dataset_path = os.path.join(version_path, dataset_filename)
        shutil.copy2(input_file, versioned_dataset_path)
        
        self.logger.info(f"Copied dataset to {versioned_dataset_path}")
        
        # Generate metadata
        metadata = self._generate_metadata(
            version_name=version_name,
            dataset_path=versioned_dataset_path,
            source_file=input_file,
            quality_score=quality_score
        )
        
        # Save metadata
        metadata_file = os.path.join(version_path, "metadata.json")
        save_json(metadata, metadata_file)
        
        self.logger.info(f"Created metadata for {version_name}")
        
        # Update index
        self._update_index(version_name, metadata)
        
        # Set as current version
        self._set_current_version(version_name)
        
        self.logger.info(f"Version '{version_name}' created successfully")
        
        return version_name
    
    def _generate_metadata(
        self,
        version_name: str,
        dataset_path: str,
        source_file: str,
        quality_score: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Generate metadata for a dataset version.
        
        Args:
            version_name: Name of the version
            dataset_path: Path to the dataset file
            source_file: Original source file path
            quality_score: Optional quality score
            
        Returns:
            Dictionary containing metadata
        """
        metadata = {
            "version": version_name,
            "created_at": get_timestamp(),
            "source_file": source_file,
        }
        
        # Add row and column information
        if dataset_path.endswith('.csv'):
            row_count, columns = self._analyze_csv(dataset_path)
            
            if self.config['metadata']['include_row_count']:
                metadata["row_count"] = row_count
            
            if self.config['metadata']['include_columns']:
                metadata["columns"] = columns
                metadata["column_count"] = len(columns)
            
            if self.config['metadata']['include_data_types']:
                metadata["data_types"] = self._infer_data_types(dataset_path, columns)
        
        # Add file hash
        if self.config['metadata']['include_file_hash']:
            metadata["file_hash"] = get_file_hash(dataset_path)
        
        # Add file size
        if self.config['metadata']['include_file_size']:
            metadata["file_size_bytes"] = get_file_size(dataset_path)
        
        # Add quality score
        if quality_score is not None:
            metadata["quality_score"] = quality_score
        
        return metadata
    
    def _analyze_csv(self, csv_path: str) -> tuple:
        """
        Analyze a CSV file to extract row count and column names.
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Tuple of (row_count, column_list)
        """
        row_count = 0
        columns = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                columns = next(reader, [])
                row_count = sum(1 for _ in reader)
            
            self.logger.debug(f"CSV analysis: {row_count} rows, {len(columns)} columns")
        except Exception as e:
            self.logger.warning(f"Error analyzing CSV: {str(e)}")
        
        return row_count, columns
    
    def _infer_data_types(self, csv_path: str, columns: List[str]) -> Dict[str, str]:
        """
        Infer data types for CSV columns.
        
        Args:
            csv_path: Path to the CSV file
            columns: List of column names
            
        Returns:
            Dictionary mapping column names to inferred data types
        """
        data_types = {col: "string" for col in columns}
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                sample_rows = [next(reader, None) for _ in range(min(100, 100))]
            
            for col in columns:
                types_found = set()
                
                for row in sample_rows:
                    if row and col in row:
                        value = row[col]
                        
                        if not value or value.strip() == '':
                            types_found.add('null')
                        elif self._is_integer(value):
                            types_found.add('integer')
                        elif self._is_float(value):
                            types_found.add('float')
                        elif self._is_boolean(value):
                            types_found.add('boolean')
                        else:
                            types_found.add('string')
                
                if 'float' in types_found:
                    data_types[col] = 'float'
                elif 'integer' in types_found:
                    data_types[col] = 'integer'
                elif 'boolean' in types_found:
                    data_types[col] = 'boolean'
                else:
                    data_types[col] = 'string'
        
        except Exception as e:
            self.logger.warning(f"Error inferring data types: {str(e)}")
        
        return data_types
    
    @staticmethod
    def _is_integer(value: str) -> bool:
        """Check if value is an integer."""
        try:
            int(value)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def _is_float(value: str) -> bool:
        """Check if value is a float."""
        try:
            float(value)
            return '.' in value
        except ValueError:
            return False
    
    @staticmethod
    def _is_boolean(value: str) -> bool:
        """Check if value is a boolean."""
        return value.lower() in ['true', 'false', 'yes', 'no', '1', '0']
    
    def _update_index(self, version_name: str, metadata: Dict[str, Any]) -> None:
        """
        Update the master versions index.
        
        Args:
            version_name: Name of the version
            metadata: Metadata dictionary for the version
        """
        # Load existing index or create new one
        if os.path.exists(self.index_file):
            index = load_json(self.index_file)
        else:
            index = {"versions": [], "created_at": get_timestamp()}
        
        # Add new version to index
        version_entry = {
            "version": version_name,
            "created_at": metadata.get("created_at"),
            "row_count": metadata.get("row_count"),
            "column_count": metadata.get("column_count"),
            "quality_score": metadata.get("quality_score"),
            "file_hash": metadata.get("file_hash")
        }
        
        index["versions"].append(version_entry)
        index["last_updated"] = get_timestamp()
        
        # Save updated index
        save_json(index, self.index_file)
        
        self.logger.info(f"Updated versions index with {version_name}")
    
    def _set_current_version(self, version_name: str) -> None:
        """
        Set the current active version.
        
        Args:
            version_name: Name of the version to set as current
        """
        write_text_file(version_name, self.current_version_file)
        self.logger.info(f"Set current version to {version_name}")
    
    def get_current_version(self) -> Optional[str]:
        """
        Get the current active version.
        
        Returns:
            Current version name or None if not set
        """
        if not os.path.exists(self.current_version_file):
            return None
        
        return read_text_file(self.current_version_file).strip()
    
    def get_all_versions(self) -> List[str]:
        """
        Get list of all available versions.
        
        Returns:
            List of version names sorted by version number
        """
        versions = []
        
        if not os.path.exists(self.versions_dir):
            return versions
        
        for item in os.listdir(self.versions_dir):
            if item.startswith('v') and os.path.isdir(os.path.join(self.versions_dir, item)):
                try:
                    int(item[1:])
                    versions.append(item)
                except ValueError:
                    continue
        
        # Sort by version number
        versions.sort(key=lambda x: int(x[1:]))
        
        return versions
    
    def get_version_metadata(self, version_name: str) -> Dict[str, Any]:
        """
        Get metadata for a specific version.
        
        Args:
            version_name: Name of the version
            
        Returns:
            Metadata dictionary
            
        Raises:
            FileNotFoundError: If version or metadata doesn't exist
        """
        metadata_file = os.path.join(self.versions_dir, version_name, "metadata.json")
        
        if not os.path.exists(metadata_file):
            self.logger.error(f"Metadata file not found for {version_name}")
            raise FileNotFoundError(f"Metadata file not found for {version_name}")
        
        return load_json(metadata_file)
    
    def get_version_dataset_path(self, version_name: str) -> str:
        """
        Get the path to the dataset file for a version.
        
        Args:
            version_name: Name of the version
            
        Returns:
            Path to the dataset file
            
        Raises:
            FileNotFoundError: If version doesn't exist
        """
        version_path = os.path.join(self.versions_dir, version_name)
        
        if not os.path.exists(version_path):
            self.logger.error(f"Version directory not found: {version_path}")
            raise FileNotFoundError(f"Version directory not found: {version_path}")
        
        # Find the dataset file (usually the CSV file)
        for file in os.listdir(version_path):
            if file.endswith('.csv'):
                return os.path.join(version_path, file)
        
        raise FileNotFoundError(f"No dataset file found in {version_path}")
