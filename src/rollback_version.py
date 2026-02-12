#!/usr/bin/env python3
"""
CLI script to rollback to a previous dataset version.

Usage:
    python rollback_version.py --to v1 [--no-backup]
"""

import os
import sys
import argparse
import logging

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import setup_logging, load_config
from version_manager import VersionManager
from rollback import RollbackManager


def main():
    """Main entry point for rollback_version CLI."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Rollback to a previous dataset version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python rollback_version.py --to v1
  python rollback_version.py --to v1 --no-backup
  python rollback_version.py --list
  python rollback_version.py --history
        """
    )
    
    parser.add_argument(
        '--to',
        help='Target version to rollback to (e.g., v1)'
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available versions'
    )
    
    parser.add_argument(
        '--history',
        action='store_true',
        help='Show rollback history'
    )
    
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip backup creation before rollback'
    )
    
    parser.add_argument(
        '--config', '-c',
        default='config/versioning_config.yaml',
        help='Path to configuration file (default: config/versioning_config.yaml)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Setup logging
        logger = setup_logging(config, "RollbackVersion")
        
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        
        logger.info("=" * 60)
        logger.info("Data Versioning System - Rollback")
        logger.info("=" * 60)
        
        # Initialize managers
        version_manager = VersionManager(args.config, logger)
        rollback_manager = RollbackManager(version_manager, logger)
        
        # Handle --list flag
        if args.list:
            all_versions = version_manager.get_all_versions()
            current_version = version_manager.get_current_version()
            
            print("\n" + "=" * 60)
            print("Available Versions")
            print("=" * 60)
            
            if not all_versions:
                print("No versions found")
            else:
                for version in all_versions:
                    marker = " ← CURRENT" if version == current_version else ""
                    try:
                        metadata = version_manager.get_version_metadata(version)
                        created_at = metadata.get('created_at', 'N/A')
                        row_count = metadata.get('row_count', 'N/A')
                        print(f"  {version}: {created_at} ({row_count} rows){marker}")
                    except Exception as e:
                        print(f"  {version}: Error reading metadata{marker}")
            
            print("=" * 60)
            return 0
        
        # Handle --history flag
        if args.history:
            rollback_history = rollback_manager.get_rollback_history()
            
            print("\n" + "=" * 60)
            print("Rollback History")
            print("=" * 60)
            
            if not rollback_history:
                print("No rollback history found")
            else:
                for event in rollback_history:
                    print(f"  {event['timestamp']}: {event['from_version']} → {event['to_version']}")
            
            print("=" * 60)
            return 0
        
        # Handle rollback operation
        if not args.to:
            parser.print_help()
            return 1
        
        # Check if safe to rollback
        is_safe, reason = rollback_manager.is_safe_to_rollback(args.to)
        
        if not is_safe:
            logger.error(f"Cannot rollback: {reason}")
            print(f"ERROR: {reason}")
            return 1
        
        # Get current version
        current_version = version_manager.get_current_version()
        
        print("\n" + "=" * 60)
        print("Rollback Confirmation")
        print("=" * 60)
        print(f"Current Version: {current_version}")
        print(f"Target Version: {args.to}")
        
        if not args.no_backup:
            print("Backup: Will be created before rollback")
        else:
            print("Backup: Skipped")
        
        # Ask for confirmation
        response = input("\nProceed with rollback? (yes/no): ").strip().lower()
        
        if response not in ['yes', 'y']:
            print("Rollback cancelled")
            logger.info("Rollback cancelled by user")
            return 0
        
        # Perform rollback
        create_backup = not args.no_backup
        rollback_manager.rollback_to_version(args.to, create_backup=create_backup)
        
        print("\n" + "=" * 60)
        print("✓ Rollback completed successfully!")
        print("=" * 60)
        print(f"Previous Version: {current_version}")
        print(f"Current Version: {args.to}")
        print("=" * 60)
        
        logger.info(f"Rollback completed: {current_version} → {args.to}")
        
        return 0
    
    except FileNotFoundError as e:
        print(f"ERROR: {str(e)}")
        return 1
    except ValueError as e:
        print(f"ERROR: {str(e)}")
        return 1
    except Exception as e:
        print(f"ERROR: Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
