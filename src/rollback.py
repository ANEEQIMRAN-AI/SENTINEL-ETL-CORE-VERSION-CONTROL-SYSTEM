"""
Rollback functionality for the data versioning system.
Handles safe rollback to previous versions.
"""

import os
import shutil
import logging
from typing import Optional

from utils import get_timestamp, load_json, save_json, write_text_file
from version_manager import VersionManager


class RollbackManager:
    """
    Manages safe rollback operations to previous dataset versions.
    """
    
    def __init__(self, version_manager: VersionManager, logger: Optional[logging.Logger] = None):
        """
        Initialize the RollbackManager.
        
        Args:
            version_manager: VersionManager instance
            logger: Logger instance
        """
        self.version_manager = version_manager
        self.logger = logger or logging.getLogger("RollbackManager")
        self.config = version_manager.config
    
    def rollback_to_version(self, target_version: str, create_backup: bool = True) -> bool:
        """
        Rollback to a previous version.
        
        Args:
            target_version: Name of the version to rollback to
            create_backup: Whether to create a backup before rollback
            
        Returns:
            True if rollback successful, False otherwise
            
        Raises:
            FileNotFoundError: If target version doesn't exist
            ValueError: If trying to rollback to current version
        """
        # Validate target version exists
        all_versions = self.version_manager.get_all_versions()
        if target_version not in all_versions:
            self.logger.error(f"Target version '{target_version}' does not exist")
            raise FileNotFoundError(f"Target version '{target_version}' does not exist")
        
        # Get current version
        current_version = self.version_manager.get_current_version()
        
        if current_version == target_version:
            self.logger.warning(f"Already at version {target_version}")
            raise ValueError(f"Already at version {target_version}")
        
        self.logger.info(f"Starting rollback from {current_version} to {target_version}")
        
        # Create backup if requested
        if create_backup and self.config['version_management']['backup_before_rollback']:
            self._create_rollback_backup(current_version)
        
        # Set target version as current
        self.version_manager._set_current_version(target_version)
        
        # Log rollback event
        self._log_rollback_event(current_version, target_version)
        
        self.logger.info(f"Successfully rolled back to {target_version}")
        
        return True
    
    def _create_rollback_backup(self, version_name: str) -> None:
        """
        Create a backup of the current version before rollback.
        
        Args:
            version_name: Name of the version to backup
        """
        try:
            backup_info = {
                "backup_timestamp": get_timestamp(),
                "backed_up_version": version_name,
                "reason": "Pre-rollback backup"
            }
            
            # Store backup info in version metadata
            metadata_file = os.path.join(
                self.version_manager.versions_dir,
                version_name,
                "metadata.json"
            )
            
            if os.path.exists(metadata_file):
                metadata = load_json(metadata_file)
                if "backups" not in metadata:
                    metadata["backups"] = []
                metadata["backups"].append(backup_info)
                save_json(metadata, metadata_file)
            
            self.logger.info(f"Created backup for {version_name}")
        
        except Exception as e:
            self.logger.warning(f"Failed to create backup: {str(e)}")
    
    def _log_rollback_event(self, from_version: str, to_version: str) -> None:
        """
        Log rollback event for audit trail.
        
        Args:
            from_version: Version rolled back from
            to_version: Version rolled back to
        """
        try:
            rollback_log_file = os.path.join(
                self.version_manager.config['storage']['logs_dir'],
                "rollback_history.json"
            )
            
            # Load existing rollback history or create new
            if os.path.exists(rollback_log_file):
                rollback_history = load_json(rollback_log_file)
            else:
                rollback_history = {"rollbacks": []}
            
            # Add new rollback event
            rollback_event = {
                "timestamp": get_timestamp(),
                "from_version": from_version,
                "to_version": to_version
            }
            
            rollback_history["rollbacks"].append(rollback_event)
            
            # Save updated history
            save_json(rollback_history, rollback_log_file)
            
            self.logger.info(f"Logged rollback event: {from_version} -> {to_version}")
        
        except Exception as e:
            self.logger.warning(f"Failed to log rollback event: {str(e)}")
    
    def get_rollback_history(self) -> list:
        """
        Get the rollback history.
        
        Returns:
            List of rollback events
        """
        try:
            rollback_log_file = os.path.join(
                self.version_manager.config['storage']['logs_dir'],
                "rollback_history.json"
            )
            
            if os.path.exists(rollback_log_file):
                history = load_json(rollback_log_file)
                return history.get("rollbacks", [])
            
            return []
        
        except Exception as e:
            self.logger.error(f"Failed to read rollback history: {str(e)}")
            return []
    
    def get_version_history(self) -> list:
        """
        Get the version history from the index.
        
        Returns:
            List of versions from the index
        """
        try:
            if os.path.exists(self.version_manager.index_file):
                index = load_json(self.version_manager.index_file)
                return index.get("versions", [])
            
            return []
        
        except Exception as e:
            self.logger.error(f"Failed to read version history: {str(e)}")
            return []
    
    def is_safe_to_rollback(self, target_version: str) -> tuple:
        """
        Check if it's safe to rollback to a version.
        
        Args:
            target_version: Name of the version to check
            
        Returns:
            Tuple of (is_safe, reason)
        """
        all_versions = self.version_manager.get_all_versions()
        
        if target_version not in all_versions:
            return False, f"Version '{target_version}' does not exist"
        
        current_version = self.version_manager.get_current_version()
        
        if current_version == target_version:
            return False, f"Already at version {target_version}"
        
        # Check if version directory exists and has metadata
        version_path = os.path.join(self.version_manager.versions_dir, target_version)
        metadata_file = os.path.join(version_path, "metadata.json")
        
        if not os.path.exists(metadata_file):
            return False, f"Metadata file not found for {target_version}"
        
        return True, "Safe to rollback"
