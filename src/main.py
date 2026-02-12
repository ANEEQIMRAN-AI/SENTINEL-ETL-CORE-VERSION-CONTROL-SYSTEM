#!/usr/bin/env python3
"""
Main CLI entry point for the Data Versioning System.
Provides a unified interface for all versioning operations.

Usage:
    python main.py create --input data/processed/clean_dataset.csv
    python main.py compare --from v1 --to v2
    python main.py rollback --to v1
    python main.py list
    python main.py info --version v1
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import setup_logging, load_config
from version_manager import VersionManager
from comparison import VersionComparator
from rollback import RollbackManager


def cmd_create(args, version_manager, logger):
    """Handle create command."""
    try:
        if not os.path.exists(args.input):
            print(f"ERROR: Input file not found: {args.input}")
            return 1
        
        version_name = version_manager.create_version(
            input_file=args.input,
            quality_score=args.quality_score
        )
        
        metadata = version_manager.get_version_metadata(version_name)
        
        print("\n✓ Version created successfully!")
        print(f"Version: {version_name}")
        print(f"Created: {metadata.get('created_at')}")
        print(f"Rows: {metadata.get('row_count')}")
        print(f"Columns: {metadata.get('column_count')}")
        
        return 0
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


def cmd_compare(args, version_manager, logger):
    """Handle compare command."""
    try:
        all_versions = version_manager.get_all_versions()
        
        if args.from_version not in all_versions:
            print(f"ERROR: Version '{args.from_version}' not found")
            return 1
        
        if args.to_version not in all_versions:
            print(f"ERROR: Version '{args.to_version}' not found")
            return 1
        
        comparator = VersionComparator(version_manager, logger)
        comparison = comparator.compare_versions(args.from_version, args.to_version)
        
        # Save comparison
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        comparator.save_comparison(comparison, args.output)
        
        print(f"\n✓ Comparison completed!")
        print(f"Comparing: {args.from_version} → {args.to_version}")
        
        if "row_count_comparison" in comparison:
            rc = comparison["row_count_comparison"]
            print(f"Row count change: {rc['difference']:+,} ({rc['percentage_change']:+.2f}%)")
        
        if "column_comparison" in comparison:
            cc = comparison["column_comparison"]
            if cc['columns_added_count'] > 0:
                print(f"Columns added: {cc['columns_added_count']}")
            if cc['columns_removed_count'] > 0:
                print(f"Columns removed: {cc['columns_removed_count']}")
        
        print(f"Report saved to: {args.output}")
        
        return 0
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


def cmd_rollback(args, version_manager, logger):
    """Handle rollback command."""
    try:
        rollback_manager = RollbackManager(version_manager, logger)
        
        is_safe, reason = rollback_manager.is_safe_to_rollback(args.to_version)
        
        if not is_safe:
            print(f"ERROR: {reason}")
            return 1
        
        current_version = version_manager.get_current_version()
        
        print(f"\nRollback: {current_version} → {args.to_version}")
        
        if not args.force:
            response = input("Proceed? (yes/no): ").strip().lower()
            if response not in ['yes', 'y']:
                print("Cancelled")
                return 0
        
        rollback_manager.rollback_to_version(args.to_version, create_backup=True)
        
        print("✓ Rollback completed!")
        
        return 0
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


def cmd_list(args, version_manager, logger):
    """Handle list command."""
    try:
        all_versions = version_manager.get_all_versions()
        current_version = version_manager.get_current_version()
        
        print("\nAvailable Versions:")
        print("-" * 60)
        
        if not all_versions:
            print("No versions found")
        else:
            for version in all_versions:
                marker = " ← CURRENT" if version == current_version else ""
                try:
                    metadata = version_manager.get_version_metadata(version)
                    created_at = metadata.get('created_at', 'N/A')
                    row_count = metadata.get('row_count', 'N/A')
                    print(f"{version}: {created_at} ({row_count} rows){marker}")
                except:
                    print(f"{version}: Error reading metadata{marker}")
        
        print("-" * 60)
        
        return 0
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


def cmd_info(args, version_manager, logger):
    """Handle info command."""
    try:
        metadata = version_manager.get_version_metadata(args.version)
        
        print(f"\nVersion Information: {args.version}")
        print("-" * 60)
        print(f"Created: {metadata.get('created_at')}")
        print(f"Source: {metadata.get('source_file')}")
        print(f"Rows: {metadata.get('row_count')}")
        print(f"Columns: {metadata.get('column_count')}")
        
        if metadata.get('columns'):
            print(f"Column List: {', '.join(metadata['columns'][:5])}{'...' if len(metadata['columns']) > 5 else ''}")
        
        if metadata.get('quality_score'):
            print(f"Quality Score: {metadata.get('quality_score')}")
        
        print(f"File Hash: {metadata.get('file_hash', 'N/A')[:16]}...")
        print(f"File Size: {metadata.get('file_size_bytes', 0) / 1024:.2f} KB")
        print("-" * 60)
        
        return 0
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


def main():
    """Main entry point."""
    
    # Create main parser
    parser = argparse.ArgumentParser(
        description="Data Versioning System - Manage dataset versions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  create      Create a new version
  compare     Compare two versions
  rollback    Rollback to a previous version
  list        List all versions
  info        Show version information

Examples:
  python main.py create --input data/processed/clean_dataset.csv
  python main.py compare --from v1 --to v2
  python main.py rollback --to v1
  python main.py list
  python main.py info --version v1
        """
    )
    
    # Global options
    parser.add_argument(
        '--config', '-c',
        default='config/versioning_config.yaml',
        help='Configuration file (default: config/versioning_config.yaml)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    # Subparsers for commands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Create command
    create_parser = subparsers.add_parser('create', help='Create a new version')
    create_parser.add_argument('--input', '-i', required=True, help='Input dataset file')
    create_parser.add_argument('--quality-score', '-q', type=float, help='Quality score (0-100)')
    
    # Compare command
    compare_parser = subparsers.add_parser('compare', help='Compare two versions')
    compare_parser.add_argument('--from', dest='from_version', required=True, help='Source version')
    compare_parser.add_argument('--to', dest='to_version', required=True, help='Target version')
    compare_parser.add_argument('--output', '-o', default='data/version_comparison.json', help='Output file')
    
    # Rollback command
    rollback_parser = subparsers.add_parser('rollback', help='Rollback to a version')
    rollback_parser.add_argument('--to', dest='to_version', required=True, help='Target version')
    rollback_parser.add_argument('--force', '-f', action='store_true', help='Skip confirmation')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all versions')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show version information')
    info_parser.add_argument('--version', '-v', required=True, help='Version name')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Setup logging
        logger = setup_logging(config, "DataVersioning")
        
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        
        # Initialize version manager
        version_manager = VersionManager(args.config, logger)
        
        # Execute command
        if args.command == 'create':
            return cmd_create(args, version_manager, logger)
        elif args.command == 'compare':
            return cmd_compare(args, version_manager, logger)
        elif args.command == 'rollback':
            return cmd_rollback(args, version_manager, logger)
        elif args.command == 'list':
            return cmd_list(args, version_manager, logger)
        elif args.command == 'info':
            return cmd_info(args, version_manager, logger)
        else:
            print(f"Unknown command: {args.command}")
            return 1
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
