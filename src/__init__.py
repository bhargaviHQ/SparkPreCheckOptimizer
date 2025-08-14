"""
SparkPreCheckOptimizer - A Streamlit-based Azure Spark optimizer app.

This package provides tools for analyzing Azure Data Lake files (Parquet) or Synapse tables
and Spark programs, suggesting optimal configurations, checks, and improvements using Azure OpenAI.
"""

__version__ = "1.0.0"
__author__ = "SparkPreCheckOptimizer Team"

from .metadata import (
    DataSourceMetadata, 
    get_datalake_metadata, 
    get_synapse_metadata, 
    extract_metadata_from_path, 
    get_optimized_metadata
)
from .checks import (
    SparkChecks, 
    CheckResult, 
    check_existence, 
    check_sanity, 
    check_data_quality, 
    run_pre_execution_checks
)
from .llm_agent import SparkOptimizationAgent
from .storage import (
    AzureStorageManager, 
    SynapseManager, 
    DataSourceConnector,
    upload_script_to_datalake,
    download_script_from_datalake,
    list_scripts_in_datalake,
    delete_script_from_datalake,
    validate_script_content
)

__all__ = [
    "DataSourceMetadata",
    "get_datalake_metadata",
    "get_synapse_metadata", 
    "extract_metadata_from_path",
    "get_optimized_metadata",
    "SparkChecks", 
    "CheckResult",
    "check_existence",
    "check_sanity", 
    "check_data_quality",
    "run_pre_execution_checks",
    "SparkOptimizationAgent",
    "AzureStorageManager",
    "SynapseManager", 
    "DataSourceConnector",
    "upload_script_to_datalake",
    "download_script_from_datalake",
    "list_scripts_in_datalake",
    "delete_script_from_datalake",
    "validate_script_content"
]
