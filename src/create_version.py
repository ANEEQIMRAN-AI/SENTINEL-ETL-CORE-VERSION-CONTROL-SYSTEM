#!/usr/bin/env python3
"""
CLI script to create a new version of a dataset.

Usage:
    python create_version.py --input data/processed/clean_dataset.csv [--quality-score 95.5]
"""

import os
import sys
import argparse
import logging

# Add src directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import setup_logging, load_config
from version_manager import VersionManager


def main():
    """Main entry point for create_version CLI."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Create a new version of a dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python create_version.py --input data/processed/clean_dataset.csv
  python create_version.py --input data/processed/clean_dataset.csv --quality-score 95.5
  python create_version.py --input data/processed/clean_dataset.csv --config config/versioning_config.yaml
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Path to the processed dataset file (CSV format)'
    )
    
    parser.add_argument(
        '--quality-score', '-q',
        type=float,
        default=None,
        help='Data quality score (0-100)'
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
        logger = setup_logging(config, "CreateVersion")
        
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        
        logger.info("=" * 60)
        logger.info("Data Versioning System - Create Version")
        logger.info("=" * 60)
        
        # Validate input file
        if not os.path.exists(args.input):
            logger.error(f"Input file not found: {args.input}")
            print(f"ERROR: Input file not found: {args.input}")
            return 1
        
        logger.info(f"Input file: {args.input}")
        
        # Validate quality score if provided
        if args.quality_score is not None:
            if not (0 <= args.quality_score <= 100):
                logger.error(f"Invalid quality score: {args.quality_score}")
                print(f"ERROR: Quality score must be between 0 and 100")
                return 1
            logger.info(f"Quality score: {args.quality_score}")
        
        # Initialize version manager
        version_manager = VersionManager(args.config, logger)
        
        # Create version
        version_name = version_manager.create_version(
            input_file=args.input,
            quality_score=args.quality_score
        )
        
        # Get metadata
        metadata = version_manager.get_version_metadata(version_name)
        
        # Print success message
        print("\n" + "=" * 60)
        print("✓ Version created successfully!")
        print("=" * 60)
        print(f"Version Name: {version_name}")
        print(f"Created At: {metadata.get('created_at')}")
        print(f"Row Count: {metadata.get('row_count')}")
        print(f"Column Count: {metadata.get('column_count')}")
        print(f"Columns: {', '.join(metadata.get('columns', [])[:5])}{'...' if len(metadata.get('columns', [])) > 5 else ''}")
        
        if args.quality_score is not None:
            print(f"Quality Score: {metadata.get('quality_score')}")
        
        print(f"File Hash: {metadata.get('file_hash', 'N/A')[:16]}...")
        print(f"File Size: {metadata.get('file_size_bytes', 0) / 1024:.2f} KB")
        print("=" * 60)
        
        logger.info(f"Version creation completed successfully: {version_name}")
        
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
