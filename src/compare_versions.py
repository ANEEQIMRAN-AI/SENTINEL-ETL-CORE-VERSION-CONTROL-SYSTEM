#!/usr/bin/env python3
"""
CLI script to compare two dataset versions.

Usage:
    python compare_versions.py --from v1 --to v2 [--output version_comparison.json]
"""

import os
import sys
import json
import argparse
import logging

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import setup_logging, load_config
from version_manager import VersionManager
from comparison import VersionComparator


def print_comparison_summary(comparison: dict) -> None:
    """
    Print a formatted summary of the comparison.
    
    Args:
        comparison: Comparison dictionary
    """
    print("\n" + "=" * 60)
    print("Version Comparison Summary")
    print("=" * 60)
    
    print(f"\nComparing: {comparison['version1']} → {comparison['version2']}")
    print(f"Comparison Time: {comparison['comparison_timestamp']}")
    
    # Row count comparison
    if "row_count_comparison" in comparison:
        rc = comparison["row_count_comparison"]
        print(f"\nRow Count:")
        print(f"  {comparison['version1']}: {rc['version1_row_count']:,} rows")
        print(f"  {comparison['version2']}: {rc['version2_row_count']:,} rows")
        print(f"  Change: {rc['difference']:+,} ({rc['percentage_change']:+.2f}%)")
    
    # Column comparison
    if "column_comparison" in comparison:
        cc = comparison["column_comparison"]
        print(f"\nColumns:")
        print(f"  {comparison['version1']}: {cc['version1_column_count']} columns")
        print(f"  {comparison['version2']}: {cc['version2_column_count']} columns")
        
        if cc['columns_added_count'] > 0:
            print(f"  Added: {cc['columns_added_count']} columns")
            if cc['added_columns']:
                print(f"    {', '.join(cc['added_columns'][:3])}{'...' if len(cc['added_columns']) > 3 else ''}")
        
        if cc['columns_removed_count'] > 0:
            print(f"  Removed: {cc['columns_removed_count']} columns")
            if cc['removed_columns']:
                print(f"    {', '.join(cc['removed_columns'][:3])}{'...' if len(cc['removed_columns']) > 3 else ''}")
    
    # Data type comparison
    if "data_type_comparison" in comparison:
        dtc = comparison["data_type_comparison"]
        if dtc['total_changes'] > 0:
            print(f"\nData Type Changes: {dtc['total_changes']}")
            for col, change in list(dtc['changed_columns'].items())[:3]:
                print(f"  {col}: {change['version1_type']} → {change['version2_type']}")
            if len(dtc['changed_columns']) > 3:
                print(f"  ... and {len(dtc['changed_columns']) - 3} more")
    
    # Summary
    if "summary" in comparison:
        summary = comparison["summary"]
        print(f"\nSummary:")
        print(f"  Total Differences: {summary['total_differences']}")
        for change in summary['key_changes']:
            print(f"  • {change}")
    
    print("\n" + "=" * 60)


def main():
    """Main entry point for compare_versions CLI."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Compare two dataset versions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compare_versions.py --from v1 --to v2
  python compare_versions.py --from v1 --to v2 --output comparison.json
  python compare_versions.py --from v1 --to v2 --config config/versioning_config.yaml
        """
    )
    
    parser.add_argument(
        '--from',
        dest='from_version',
        required=True,
        help='Source version (e.g., v1)'
    )
    
    parser.add_argument(
        '--to',
        dest='to_version',
        required=True,
        help='Target version (e.g., v2)'
    )
    
    parser.add_argument(
        '--output', '-o',
        default='data/version_comparison.json',
        help='Output file for comparison report (default: data/version_comparison.json)'
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
        logger = setup_logging(config, "CompareVersions")
        
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        
        logger.info("=" * 60)
        logger.info("Data Versioning System - Compare Versions")
        logger.info("=" * 60)
        
        # Initialize version manager
        version_manager = VersionManager(args.config, logger)
        
        # Check if versions exist
        all_versions = version_manager.get_all_versions()
        
        if args.from_version not in all_versions:
            logger.error(f"Version '{args.from_version}' not found")
            print(f"ERROR: Version '{args.from_version}' not found")
            print(f"Available versions: {', '.join(all_versions)}")
            return 1
        
        if args.to_version not in all_versions:
            logger.error(f"Version '{args.to_version}' not found")
            print(f"ERROR: Version '{args.to_version}' not found")
            print(f"Available versions: {', '.join(all_versions)}")
            return 1
        
        logger.info(f"Comparing {args.from_version} and {args.to_version}")
        
        # Initialize comparator
        comparator = VersionComparator(version_manager, logger)
        
        # Perform comparison
        comparison = comparator.compare_versions(args.from_version, args.to_version)
        
        # Save comparison report
        os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
        comparator.save_comparison(comparison, args.output)
        
        # Print summary
        print_comparison_summary(comparison)
        
        print(f"Detailed comparison saved to: {args.output}")
        
        logger.info(f"Comparison completed and saved to {args.output}")
        
        return 0
    
    except FileNotFoundError as e:
        print(f"ERROR: {str(e)}")
        return 1
    except Exception as e:
        print(f"ERROR: Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
