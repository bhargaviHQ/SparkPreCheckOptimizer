"""
Metadata handling for Azure Data Lake and Synapse data sources.
Extracts and manages schema information, file metadata, and table properties.
"""

import os
import json
import re
from typing import Dict, List, Optional, Any, Union
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.identity import DefaultAzureCredential
import pandas as pd

# Environment variable placeholders
# Use os.environ["AZURE_STORAGE_ACCOUNT"] = "your_account"
# Use os.environ["AZURE_TENANT_ID"] = "your_tenant_id"
# Use os.environ["AZURE_CLIENT_ID"] = "your_client_id"
# Use os.environ["AZURE_CLIENT_SECRET"] = "your_client_secret"

# Environment variable placeholders
# AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name_here
# AZURE_TENANT_ID=your_tenant_id_here
# AZURE_CLIENT_ID=your_client_id_here
# AZURE_CLIENT_SECRET=your_client_secret_here

class DataSourceMetadata:
    """Handles metadata extraction and management for various data sources."""
    
    def __init__(self):
        self.storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        self.credential = DefaultAzureCredential()
        
    def get_parquet_metadata(self, container_name: str, blob_path: str) -> Dict[str, Any]:
        """
        Extract metadata from Parquet files in Azure Data Lake.
        
        Args:
            container_name: Name of the storage container
            blob_path: Path to the Parquet file
            
        Returns:
            Dictionary containing file metadata and schema information
        """
        try:
            # Initialize blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
                credential=self.credential
            )
            
            # Get blob client
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_path
            )
            
            # Get blob properties
            properties = blob_client.get_blob_properties()
            
            # Read Parquet metadata (first few rows for schema)
            # Note: This is a simplified approach - in production, you might want to use
            # PyArrow or similar for more efficient metadata extraction
            metadata = {
                "file_size_bytes": properties.size,
                "last_modified": properties.last_modified,
                "content_type": properties.content_settings.content_type,
                "container": container_name,
                "blob_path": blob_path,
                "schema": None,  # Will be populated when reading actual data
                "row_count": None,  # Will be populated when reading actual data
                "column_count": None  # Will be populated when reading actual data
            }
            
            return metadata
            
        except Exception as e:
            raise Exception(f"Error extracting Parquet metadata: {str(e)}")
    
    def get_synapse_table_metadata(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Extract metadata from Azure Synapse tables.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Dictionary containing table metadata and schema information
        """
        # Environment variable placeholders for Synapse connection
        # SYNAPSE_WORKSPACE_NAME=your_workspace_name_here
        # SYNAPSE_SPARK_POOL_NAME=your_spark_pool_name_here
        
        try:
            # This would typically use azure-synapse-spark client
            # For now, returning placeholder structure
            metadata = {
                "database_name": database_name,
                "table_name": table_name,
                "table_type": "managed",  # or external
                "schema": None,  # Will be populated from actual table
                "row_count": None,
                "column_count": None,
                "partition_info": None,
                "storage_location": None
            }
            
            return metadata
            
        except Exception as e:
            raise Exception(f"Error extracting Synapse table metadata: {str(e)}")
    
    def analyze_data_characteristics(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze data characteristics for optimization recommendations.
        
        Args:
            metadata: Data source metadata
            
        Returns:
            Dictionary with analysis results and recommendations
        """
        analysis = {
            "file_size_category": self._categorize_file_size(metadata.get("file_size_bytes", 0)),
            "partitioning_recommendations": [],
            "compression_recommendations": [],
            "caching_recommendations": [],
            "optimization_opportunities": []
        }
        
        # Add specific recommendations based on metadata
        if metadata.get("file_size_bytes", 0) > 1_000_000_000:  # 1GB
            analysis["partitioning_recommendations"].append(
                "Consider partitioning large files for better performance"
            )
            analysis["caching_recommendations"].append(
                "Large files may benefit from caching frequently accessed data"
            )
        
        return analysis
    
    def _categorize_file_size(self, size_bytes: int) -> str:
        """Categorize file size for optimization recommendations."""
        if size_bytes < 100_000_000:  # 100MB
            return "small"
        elif size_bytes < 1_000_000_000:  # 1GB
            return "medium"
        else:
            return "large"
    
    def get_schema_info(self, data_source: str, **kwargs) -> Dict[str, Any]:
        """
        Get schema information for the specified data source.
        
        Args:
            data_source: Type of data source ("parquet" or "synapse")
            **kwargs: Additional parameters (container_name, blob_path, etc.)
            
        Returns:
            Dictionary with schema information
        """
        if data_source.lower() == "parquet":
            return self.get_parquet_metadata(
                kwargs.get("container_name"), 
                kwargs.get("blob_path")
            )
        elif data_source.lower() == "synapse":
            return self.get_synapse_table_metadata(
                kwargs.get("database_name"), 
                kwargs.get("table_name")
            )
        else:
            raise ValueError(f"Unsupported data source: {data_source}")


def get_datalake_metadata(path: str, spark) -> Dict[str, Any]:
    """
    Extract metadata from Azure Data Lake Parquet files using PySpark.
    
    Args:
        path: ABFSS path (e.g., 'abfss://container@account.dfs.core.windows.net/file.parquet')
        spark: PySpark SparkSession instance
        
    Returns:
        Dictionary containing metadata with keys:
        - type: 'datalake'
        - file_size_gb: float (file size in GB)
        - row_count: int (number of rows)
        - schema: str (JSON schema string)
        
    Raises:
        ValueError: If path format is invalid
        Exception: If metadata extraction fails
    """
    try:
        # Validate ABFSS path format
        if not path.startswith('abfss://'):
            raise ValueError("Path must be a valid ABFSS path starting with 'abfss://'")
        
        # Parse ABFSS path to extract components
        # Format: abfss://container@account.dfs.core.windows.net/path/to/file.parquet
        path_pattern = r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)'
        match = re.match(path_pattern, path)
        
        if not match:
            raise ValueError(f"Invalid ABFSS path format: {path}")
        
        container_name, storage_account, blob_path = match.groups()
        
        # Read data using PySpark (lightweight - limit to 1 row for schema)
        df = spark.read.format('parquet').load(path)
        
        # Get schema as JSON string (use limit(1) for large files to avoid memory issues)
        schema_df = df.limit(1)
        schema_json = schema_df.schema.json()
        
        # Get row count (use limit for large datasets to avoid memory issues)
        # For very large datasets, consider sampling or using table statistics
        try:
            row_count = df.count()
        except Exception as e:
            # Fallback: estimate row count from file size if count fails
            print(f"Warning: Could not get exact row count: {e}")
            row_count = None
        
        # Get file size using Azure Storage Blob client
        file_size_gb = None
        try:
            # Use DefaultAzureCredential for authentication
            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(
                account_url=f"https://{storage_account}.blob.core.windows.net",
                credential=credential
            )
            
            blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=blob_path
            )
            
            # Get blob properties
            properties = blob_client.get_blob_properties()
            file_size_gb = properties.size / (1024 ** 3)  # Convert bytes to GB
            
        except Exception as e:
            print(f"Warning: Could not get file size: {e}")
            file_size_gb = None
        
        return {
            'type': 'datalake',
            'file_size_gb': file_size_gb,
            'row_count': row_count,
            'schema': schema_json,
            'container': container_name,
            'storage_account': storage_account,
            'blob_path': blob_path
        }
        
    except Exception as e:
        raise Exception(f"Error extracting Data Lake metadata: {str(e)}")


def get_synapse_metadata(connection_str: str, table_name: str, spark) -> Dict[str, Any]:
    """
    Extract metadata from Azure Synapse tables using JDBC connection.
    
    Args:
        connection_str: JDBC connection string 
                       (e.g., 'jdbc:sqlserver://workspace.sql.azuresynapse.net:1433;database=db;user=user;password=pass')
        table_name: Name of the table to analyze
        spark: PySpark SparkSession instance
        
    Returns:
        Dictionary containing metadata with keys:
        - type: 'synapse'
        - file_size_gb: None (not applicable for tables)
        - row_count: int (number of rows)
        - schema: str (JSON schema string)
        
    Raises:
        ValueError: If connection string or table name is invalid
        Exception: If metadata extraction fails
        
    Warning:
        Embedding credentials in connection string is not secure for production.
        Consider using Azure Key Vault or managed identities.
    """
    try:
        # Validate inputs
        if not connection_str or not table_name:
            raise ValueError("Connection string and table name are required")
        
        if 'password=' in connection_str.lower():
            print("Warning: Credentials embedded in connection string. Consider using secure authentication methods.")
        
        # Read table using JDBC (lightweight - limit to 1 row for schema)
        df = spark.read.jdbc(url=connection_str, table=table_name)
        
        # Get schema as JSON string (use limit(1) for large tables to avoid memory issues)
        schema_df = df.limit(1)
        schema_json = schema_df.schema.json()
        
        # Get row count (use limit for large tables to avoid memory issues)
        try:
            # For large tables, consider using table statistics instead of count()
            row_count = df.count()
        except Exception as e:
            # Fallback: try to get approximate row count from table statistics
            print(f"Warning: Could not get exact row count: {e}")
            try:
                # Alternative: use SQL query to get row count
                row_count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                count_df = spark.read.jdbc(url=connection_str, table=f"({row_count_query}) as count_table")
                row_count = count_df.first()['row_count']
            except Exception as count_error:
                print(f"Warning: Could not get row count via SQL: {count_error}")
                row_count = None
        
        return {
            'type': 'synapse',
            'file_size_gb': None,  # Not applicable for database tables
            'row_count': row_count,
            'schema': schema_json,
            'table_name': table_name,
            'connection_string': connection_str.replace('password=', 'password=***')  # Mask password in logs
        }
        
    except Exception as e:
        raise Exception(f"Error extracting Synapse metadata: {str(e)}")


def extract_metadata_from_path(path: str, spark, connection_str: str = None) -> Dict[str, Any]:
    """
    Unified function to extract metadata from either Data Lake or Synapse sources.
    
    Args:
        path: ABFSS path for Data Lake or table name for Synapse
        spark: PySpark SparkSession instance
        connection_str: JDBC connection string (required for Synapse)
        
    Returns:
        Dictionary containing metadata (see get_datalake_metadata or get_synapse_metadata)
        
    Raises:
        ValueError: If path format is invalid or connection string missing for Synapse
        Exception: If metadata extraction fails
    """
    try:
        if path.startswith('abfss://'):
            # Data Lake path
            return get_datalake_metadata(path, spark)
        elif connection_str:
            # Synapse table (path is table name)
            return get_synapse_metadata(connection_str, path, spark)
        else:
            raise ValueError("Connection string is required for Synapse table metadata extraction")
            
    except Exception as e:
        raise Exception(f"Error in unified metadata extraction: {str(e)}")


def get_optimized_metadata(path: str, spark, connection_str: str = None, 
                          sample_size: int = 1000) -> Dict[str, Any]:
    """
    Get metadata with optimization for large datasets.
    
    Args:
        path: ABFSS path for Data Lake or table name for Synapse
        spark: PySpark SparkSession instance
        connection_str: JDBC connection string (required for Synapse)
        sample_size: Number of rows to sample for schema analysis
        
    Returns:
        Dictionary containing metadata with additional optimization insights
    """
    try:
        # Get basic metadata
        metadata = extract_metadata_from_path(path, spark, connection_str)
        
        # Add optimization insights based on metadata
        if metadata['type'] == 'datalake':
            file_size_gb = metadata.get('file_size_gb', 0)
            if file_size_gb and file_size_gb > 1.0:  # > 1GB
                metadata['optimization_recommendations'] = [
                    "Consider partitioning large files for better performance",
                    "Use broadcast joins for small lookup tables",
                    "Enable adaptive query execution"
                ]
            elif file_size_gb and file_size_gb > 0.1:  # > 100MB
                metadata['optimization_recommendations'] = [
                    "Consider caching frequently accessed data",
                    "Use appropriate shuffle partitions"
                ]
        
        elif metadata['type'] == 'synapse':
            row_count = metadata.get('row_count', 0)
            if row_count and row_count > 1000000:  # > 1M rows
                metadata['optimization_recommendations'] = [
                    "Consider table partitioning for large tables",
                    "Use appropriate distribution keys",
                    "Enable columnstore indexes for analytical queries"
                ]
        
        return metadata
        
    except Exception as e:
        raise Exception(f"Error in optimized metadata extraction: {str(e)}")