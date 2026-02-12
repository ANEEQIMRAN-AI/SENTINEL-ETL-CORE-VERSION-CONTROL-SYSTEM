"""
Version comparison logic for the data versioning system.
Compares versions and generates detailed comparison reports.
"""

import os
import csv
import logging
from typing import Dict, Any, List, Optional, Set

from utils import load_json, save_json, get_timestamp
from version_manager import VersionManager


class VersionComparator:
    """
    Compares different versions of datasets and generates comparison reports.
    """
    
    def __init__(self, version_manager: VersionManager, logger: Optional[logging.Logger] = None):
        """
        Initialize the VersionComparator.
        
        Args:
            version_manager: VersionManager instance
            logger: Logger instance
        """
        self.version_manager = version_manager
        self.logger = logger or logging.getLogger("VersionComparator")
        self.config = version_manager.config
    
    def compare_versions(self, version1: str, version2: str) -> Dict[str, Any]:
        """
        Compare two versions and generate a comparison report.
        
        Args:
            version1: Name of the first version
            version2: Name of the second version
            
        Returns:
            Dictionary containing comparison results
            
        Raises:
            FileNotFoundError: If versions don't exist
        """
        self.logger.info(f"Comparing versions {version1} and {version2}")
        
        # Get metadata for both versions
        metadata1 = self.version_manager.get_version_metadata(version1)
        metadata2 = self.version_manager.get_version_metadata(version2)
        
        # Get dataset paths
        dataset_path1 = self.version_manager.get_version_dataset_path(version1)
        dataset_path2 = self.version_manager.get_version_dataset_path(version2)
        
        comparison = {
            "comparison_timestamp": get_timestamp(),
            "version1": version1,
            "version2": version2,
            "version1_created_at": metadata1.get("created_at"),
            "version2_created_at": metadata2.get("created_at"),
        }
        
        # Compare row counts
        if self.config['comparison']['compare_row_count']:
            comparison["row_count_comparison"] = self._compare_row_counts(
                metadata1, metadata2
            )
        
        # Compare columns
        if self.config['comparison']['compare_columns']:
            comparison["column_comparison"] = self._compare_columns(
                metadata1, metadata2, dataset_path1, dataset_path2
            )
        
        # Compare data types
        if self.config['comparison']['compare_data_types']:
            comparison["data_type_comparison"] = self._compare_data_types(
                metadata1, metadata2
            )
        
        # Include sample data differences
        if self.config['comparison']['include_sample_data']:
            comparison["sample_data_comparison"] = self._compare_sample_data(
                dataset_path1, dataset_path2
            )
        
        # Add summary
        comparison["summary"] = self._generate_summary(comparison)
        
        self.logger.info(f"Comparison complete for {version1} and {version2}")
        
        return comparison
    
    def _compare_row_counts(self, metadata1: Dict[str, Any], metadata2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare row counts between two versions.
        
        Args:
            metadata1: Metadata of first version
            metadata2: Metadata of second version
            
        Returns:
            Row count comparison dictionary
        """
        row_count1 = metadata1.get("row_count", 0)
        row_count2 = metadata2.get("row_count", 0)
        
        difference = row_count2 - row_count1
        percentage_change = (difference / row_count1 * 100) if row_count1 > 0 else 0
        
        return {
            "version1_row_count": row_count1,
            "version2_row_count": row_count2,
            "difference": difference,
            "percentage_change": round(percentage_change, 2),
            "direction": "increase" if difference > 0 else ("decrease" if difference < 0 else "no change")
        }
    
    def _compare_columns(
        self,
        metadata1: Dict[str, Any],
        metadata2: Dict[str, Any],
        dataset_path1: str,
        dataset_path2: str
    ) -> Dict[str, Any]:
        """
        Compare columns between two versions.
        
        Args:
            metadata1: Metadata of first version
            metadata2: Metadata of second version
            dataset_path1: Path to first dataset
            dataset_path2: Path to second dataset
            
        Returns:
            Column comparison dictionary
        """
        columns1 = set(metadata1.get("columns", []))
        columns2 = set(metadata2.get("columns", []))
        
        added_columns = list(columns2 - columns1)
        removed_columns = list(columns1 - columns2)
        common_columns = list(columns1 & columns2)
        
        return {
            "version1_column_count": len(columns1),
            "version2_column_count": len(columns2),
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "common_columns": common_columns,
            "columns_added_count": len(added_columns),
            "columns_removed_count": len(removed_columns)
        }
    
    def _compare_data_types(
        self,
        metadata1: Dict[str, Any],
        metadata2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare data types between two versions.
        
        Args:
            metadata1: Metadata of first version
            metadata2: Metadata of second version
            
        Returns:
            Data type comparison dictionary
        """
        types1 = metadata1.get("data_types", {})
        types2 = metadata2.get("data_types", {})
        
        changed_types = {}
        
        for column in types1:
            if column in types2 and types1[column] != types2[column]:
                changed_types[column] = {
                    "version1_type": types1[column],
                    "version2_type": types2[column]
                }
        
        return {
            "changed_columns": changed_types,
            "total_changes": len(changed_types)
        }
    
    def _compare_sample_data(self, dataset_path1: str, dataset_path2: str) -> Dict[str, Any]:
        """
        Compare sample data from both versions.
        
        Args:
            dataset_path1: Path to first dataset
            dataset_path2: Path to second dataset
            
        Returns:
            Sample data comparison dictionary
        """
        sample_size = self.config['comparison']['sample_size']
        
        sample1 = self._read_csv_sample(dataset_path1, sample_size)
        sample2 = self._read_csv_sample(dataset_path2, sample_size)
        
        return {
            "version1_sample": sample1,
            "version2_sample": sample2,
            "sample_size": sample_size
        }
    
    def _read_csv_sample(self, csv_path: str, sample_size: int) -> List[Dict[str, str]]:
        """
        Read sample rows from a CSV file.
        
        Args:
            csv_path: Path to the CSV file
            sample_size: Number of rows to read
            
        Returns:
            List of sample rows as dictionaries
        """
        sample = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i >= sample_size:
                        break
                    sample.append(row)
        except Exception as e:
            self.logger.warning(f"Error reading CSV sample: {str(e)}")
        
        return sample
    
    def _generate_summary(self, comparison: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate a summary of the comparison.
        
        Args:
            comparison: Comparison dictionary
            
        Returns:
            Summary dictionary
        """
        summary = {
            "total_differences": 0,
            "key_changes": []
        }
        
        # Count row changes
        if "row_count_comparison" in comparison:
            row_diff = comparison["row_count_comparison"]["difference"]
            if row_diff != 0:
                summary["total_differences"] += 1
                summary["key_changes"].append(
                    f"Row count changed by {row_diff} "
                    f"({comparison['row_count_comparison']['percentage_change']}%)"
                )
        
        # Count column changes
        if "column_comparison" in comparison:
            col_comp = comparison["column_comparison"]
            if col_comp["columns_added_count"] > 0:
                summary["total_differences"] += 1
                summary["key_changes"].append(
                    f"{col_comp['columns_added_count']} columns added"
                )
            if col_comp["columns_removed_count"] > 0:
                summary["total_differences"] += 1
                summary["key_changes"].append(
                    f"{col_comp['columns_removed_count']} columns removed"
                )
        
        # Count data type changes
        if "data_type_comparison" in comparison:
            type_changes = comparison["data_type_comparison"]["total_changes"]
            if type_changes > 0:
                summary["total_differences"] += 1
                summary["key_changes"].append(f"{type_changes} data types changed")
        
        if summary["total_differences"] == 0:
            summary["key_changes"].append("No significant differences found")
        
        return summary
    
    def save_comparison(self, comparison: Dict[str, Any], output_file: str) -> None:
        """
        Save comparison report to a JSON file.
        
        Args:
            comparison: Comparison dictionary
            output_file: Path to save the comparison report
        """
        save_json(comparison, output_file)
        self.logger.info(f"Comparison report saved to {output_file}")
