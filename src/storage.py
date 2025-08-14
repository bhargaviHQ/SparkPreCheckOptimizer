"""
Azure Storage and Synapse connectivity and operations.
Handles connections to Azure Data Lake Storage and Azure Synapse Analytics.
"""

import os
from typing import Dict, List, Any, Optional, BinaryIO
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.synapse.spark import SparkClient
from azure.synapse.spark.models import SparkJobDefinition
import pandas as pd

# Environment variable placeholders
# Set os.environ["AZURE_STORAGE_ACCOUNT"] = "your_account"
# Set os.environ["AZURE_STORAGE_KEY"] = "your_key" if not using credential
# AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name_here
# AZURE_TENANT_ID=your_tenant_id_here
# AZURE_CLIENT_ID=your_client_id_here
# AZURE_CLIENT_SECRET=your_client_secret_here
# SYNAPSE_WORKSPACE_NAME=your_workspace_name_here
# SYNAPSE_SPARK_POOL_NAME=your_spark_pool_name_here

class AzureStorageManager:
    """Manages Azure Storage operations for Data Lake Storage."""
    
    def __init__(self):
        self.storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        
        if not all([self.storage_account_name, self.tenant_id, self.client_id, self.client_secret]):
            # Fallback to DefaultAzureCredential for managed identity scenarios
            self.credential = DefaultAzureCredential()
        else:
            self.credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
        
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{self.storage_account_name}.blob.core.windows.net",
            credential=self.credential
        )
    
    def list_containers(self) -> List[str]:
        """List all containers in the storage account."""
        try:
            containers = []
            for container in self.blob_service_client.list_containers():
                containers.append(container.name)
            return containers
        except Exception as e:
            raise Exception(f"Error listing containers: {str(e)}")
    
    def list_blobs(self, container_name: str, prefix: str = "") -> List[Dict[str, Any]]:
        """
        List blobs in a container with optional prefix filter.
        
        Args:
            container_name: Name of the container
            prefix: Optional prefix to filter blobs
            
        Returns:
            List of blob information dictionaries
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = []
            
            for blob in container_client.list_blobs(name_starts_with=prefix):
                blobs.append({
                    "name": blob.name,
                    "size": blob.size,
                    "last_modified": blob.last_modified,
                    "content_type": blob.content_settings.content_type if blob.content_settings else None
                })
            
            return blobs
        except Exception as e:
            raise Exception(f"Error listing blobs in container {container_name}: {str(e)}")
    
    def read_parquet_sample(self, container_name: str, blob_path: str, sample_size: int = 1000) -> pd.DataFrame:
        """
        Read a sample of data from a Parquet file.
        
        Args:
            container_name: Name of the container
            blob_path: Path to the Parquet file
            sample_size: Number of rows to sample
            
        Returns:
            Pandas DataFrame with sampled data
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_path
            )
            
            # Download blob to temporary location or read directly
            # For large files, you might want to use streaming or chunked reading
            blob_data = blob_client.download_blob()
            
            # Read Parquet data using pandas
            df = pd.read_parquet(blob_data, engine='pyarrow')
            
            # Return sample
            return df.head(sample_size)
            
        except Exception as e:
            raise Exception(f"Error reading Parquet file {blob_path}: {str(e)}")
    
    def get_blob_metadata(self, container_name: str, blob_path: str) -> Dict[str, Any]:
        """
        Get metadata for a specific blob.
        
        Args:
            container_name: Name of the container
            blob_path: Path to the blob
            
        Returns:
            Dictionary with blob metadata
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_path
            )
            
            properties = blob_client.get_blob_properties()
            
            metadata = {
                "name": properties.name,
                "size": properties.size,
                "last_modified": properties.last_modified,
                "content_type": properties.content_settings.content_type,
                "etag": properties.etag,
                "metadata": properties.metadata
            }
            
            return metadata
            
        except Exception as e:
            raise Exception(f"Error getting blob metadata for {blob_path}: {str(e)}")
    
    def upload_file(self, container_name: str, blob_path: str, file_data: BinaryIO) -> bool:
        """
        Upload a file to Azure Storage.
        
        Args:
            container_name: Name of the container
            blob_path: Path where to store the blob
            file_data: File-like object to upload
            
        Returns:
            True if upload successful
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=blob_path
            )
            
            blob_client.upload_blob(file_data, overwrite=True)
            return True
            
        except Exception as e:
            raise Exception(f"Error uploading file to {blob_path}: {str(e)}")

class SynapseManager:
    """Manages Azure Synapse Analytics operations."""
    
    def __init__(self):
        self.workspace_name = os.getenv("SYNAPSE_WORKSPACE_NAME")
        self.spark_pool_name = os.getenv("SYNAPSE_SPARK_POOL_NAME")
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        
        if not all([self.workspace_name, self.spark_pool_name, self.tenant_id, self.client_id, self.client_secret]):
            raise ValueError("Synapse configuration incomplete. Please set all required environment variables.")
        
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        # Initialize Synapse client
        self.synapse_endpoint = f"https://{self.workspace_name}.dev.azuresynapse.net"
        self.spark_client = SparkClient(
            endpoint=self.synapse_endpoint,
            credential=self.credential
        )
    
    def list_databases(self) -> List[str]:
        """List all databases in the Synapse workspace."""
        try:
            # This would typically use the Synapse client to list databases
            # For now, returning placeholder
            return ["default", "master"]
        except Exception as e:
            raise Exception(f"Error listing databases: {str(e)}")
    
    def list_tables(self, database_name: str) -> List[Dict[str, Any]]:
        """
        List tables in a database.
        
        Args:
            database_name: Name of the database
            
        Returns:
            List of table information dictionaries
        """
        try:
            # This would typically query the database metadata
            # For now, returning placeholder structure
            tables = [
                {
                    "name": "sample_table",
                    "type": "managed",
                    "location": f"abfss://container@{self.workspace_name}.dfs.core.windows.net/path"
                }
            ]
            return tables
        except Exception as e:
            raise Exception(f"Error listing tables in database {database_name}: {str(e)}")
    
    def get_table_schema(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get schema information for a table.
        
        Args:
            database_name: Name of the database
            table_name: Name of the table
            
        Returns:
            Dictionary with table schema information
        """
        try:
            # This would typically query the table metadata
            # For now, returning placeholder schema
            schema = {
                "database": database_name,
                "table": table_name,
                "columns": [
                    {"name": "id", "type": "int", "nullable": False},
                    {"name": "name", "type": "string", "nullable": True},
                    {"name": "created_date", "type": "timestamp", "nullable": True}
                ],
                "partition_columns": [],
                "storage_format": "parquet"
            }
            return schema
        except Exception as e:
            raise Exception(f"Error getting schema for table {table_name}: {str(e)}")
    
    def submit_spark_job(self, job_definition: Dict[str, Any]) -> str:
        """
        Submit a Spark job to Synapse.
        
        Args:
            job_definition: Spark job definition
            
        Returns:
            Job submission ID
        """
        try:
            # This would typically use the Spark client to submit jobs
            # For now, returning placeholder
            return "job_12345"
        except Exception as e:
            raise Exception(f"Error submitting Spark job: {str(e)}")
    
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get status of a submitted Spark job.
        
        Args:
            job_id: ID of the submitted job
            
        Returns:
            Dictionary with job status information
        """
        try:
            # This would typically query the job status
            # For now, returning placeholder
            return {
                "job_id": job_id,
                "status": "completed",
                "start_time": "2024-01-01T00:00:00Z",
                "end_time": "2024-01-01T00:05:00Z",
                "duration_seconds": 300
            }
        except Exception as e:
            raise Exception(f"Error getting job status for {job_id}: {str(e)}")

class DataSourceConnector:
    """Unified connector for different data sources."""
    
    def __init__(self):
        self.storage_manager = AzureStorageManager()
        self.synapse_manager = SynapseManager()
    
    def connect_to_data_source(self, source_type: str, **kwargs) -> Dict[str, Any]:
        """
        Connect to a data source and return connection information.
        
        Args:
            source_type: Type of data source ("storage" or "synapse")
            **kwargs: Additional connection parameters
            
        Returns:
            Dictionary with connection information
        """
        try:
            if source_type.lower() == "storage":
                return self._connect_to_storage(**kwargs)
            elif source_type.lower() == "synapse":
                return self._connect_to_synapse(**kwargs)
            else:
                raise ValueError(f"Unsupported data source type: {source_type}")
        except Exception as e:
            raise Exception(f"Error connecting to data source: {str(e)}")
    
    def _connect_to_storage(self, **kwargs) -> Dict[str, Any]:
        """Connect to Azure Storage."""
        container_name = kwargs.get("container_name")
        if not container_name:
            raise ValueError("Container name is required for storage connection")
        
        # Test connection by listing blobs
        blobs = self.storage_manager.list_blobs(container_name, limit=1)
        
        return {
            "type": "storage",
            "container": container_name,
            "connection_status": "connected",
            "available_blobs": len(blobs)
        }
    
    def _connect_to_synapse(self, **kwargs) -> Dict[str, Any]:
        """Connect to Azure Synapse."""
        database_name = kwargs.get("database_name", "default")
        
        # Test connection by listing tables
        tables = self.synapse_manager.list_tables(database_name)
        
        return {
            "type": "synapse",
            "database": database_name,
            "connection_status": "connected",
                         "available_tables": len(tables)
         }


def upload_script_to_datalake(script_content: str, filename: str, container: str, account_name: str) -> str:
    """
    Upload a Spark script to Azure Data Lake Storage.
    
    Args:
        script_content: The Python script content as a string
        filename: Name of the file (e.g., 'myscript.py')
        container: Name of the storage container
        account_name: Name of the Azure storage account
        
    Returns:
        Full ABFSS path to the uploaded script
        
    Raises:
        ValueError: If required parameters are missing
        Exception: If upload fails
        
    Example:
        >>> upload_script_to_datalake('print("Hello")', 'myscript.py', 'mycontainer', 'myaccount')
        'abfss://mycontainer@myaccount.dfs.core.windows.net/scripts/myscript.py'
        
    Note:
        Requires Azure credentials. Set environment variables:
        - AZURE_STORAGE_ACCOUNT: your storage account name
        - AZURE_STORAGE_KEY: your storage account key (if not using DefaultAzureCredential)
    """
    try:
        # Validate inputs
        if not script_content or not filename or not container or not account_name:
            raise ValueError("All parameters (script_content, filename, container, account_name) are required")
        
        # Ensure filename has .py extension
        if not filename.endswith('.py'):
            filename = f"{filename}.py"
        
        # Create blob path in scripts folder
        blob_path = f"scripts/{filename}"
        
        # Initialize blob service client
        try:
            # Try using storage account key first
            storage_key = os.getenv('AZURE_STORAGE_KEY')
            if storage_key:
                connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={storage_key};EndpointSuffix=core.windows.net"
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Fallback to DefaultAzureCredential
                credential = DefaultAzureCredential()
                blob_service_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=credential
                )
        except Exception as auth_error:
            raise Exception(f"Failed to authenticate with Azure Storage: {str(auth_error)}")
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container,
            blob=blob_path
        )
        
        # Upload script content
        try:
            blob_client.upload_blob(
                script_content,
                overwrite=True,
                content_settings=None  # Let Azure determine content type
            )
        except Exception as upload_error:
            raise Exception(f"Failed to upload script: {str(upload_error)}")
        
        # Return full ABFSS path
        abfss_path = f"abfss://{container}@{account_name}.dfs.core.windows.net/{blob_path}"
        
        return abfss_path
        
    except Exception as e:
        raise Exception(f"Error uploading script to Data Lake: {str(e)}")


def download_script_from_datalake(path: str) -> str:
    """
    Download a Spark script from Azure Data Lake Storage.
    
    Args:
        path: ABFSS path to the script (e.g., 'abfss://container@account.dfs.core.windows.net/scripts/myscript.py')
        
    Returns:
        Script content as a string
        
    Raises:
        ValueError: If path format is invalid
        Exception: If download fails
        
    Example:
        >>> download_script_from_datalake('abfss://mycontainer@myaccount.dfs.core.windows.net/scripts/myscript.py')
        'print("Hello")'
        
    Note:
        Requires Azure credentials. Set environment variables:
        - AZURE_STORAGE_ACCOUNT: your storage account name
        - AZURE_STORAGE_KEY: your storage account key (if not using DefaultAzureCredential)
    """
    try:
        # Validate ABFSS path format
        if not path.startswith('abfss://'):
            raise ValueError("Path must be a valid ABFSS path starting with 'abfss://'")
        
        # Parse ABFSS path to extract components
        # Format: abfss://container@account.dfs.core.windows.net/path/to/file.py
        import re
        path_pattern = r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)'
        match = re.match(path_pattern, path)
        
        if not match:
            raise ValueError(f"Invalid ABFSS path format: {path}")
        
        container_name, storage_account, blob_path = match.groups()
        
        # Initialize blob service client
        try:
            # Try using storage account key first
            storage_key = os.getenv('AZURE_STORAGE_KEY')
            if storage_key:
                connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_key};EndpointSuffix=core.windows.net"
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Fallback to DefaultAzureCredential
                credential = DefaultAzureCredential()
                blob_service_client = BlobServiceClient(
                    account_url=f"https://{storage_account}.blob.core.windows.net",
                    credential=credential
                )
        except Exception as auth_error:
            raise Exception(f"Failed to authenticate with Azure Storage: {str(auth_error)}")
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_path
        )
        
        # Download script content
        try:
            download_stream = blob_client.download_blob()
            script_content = download_stream.readall().decode('utf-8')
            return script_content
        except Exception as download_error:
            raise Exception(f"Failed to download script: {str(download_error)}")
        
    except Exception as e:
        raise Exception(f"Error downloading script from Data Lake: {str(e)}")


def list_scripts_in_datalake(container: str, account_name: str, prefix: str = "scripts/") -> List[Dict[str, Any]]:
    """
    List all Spark scripts stored in Azure Data Lake Storage.
    
    Args:
        container: Name of the storage container
        account_name: Name of the Azure storage account
        prefix: Optional prefix to filter scripts (default: "scripts/")
        
    Returns:
        List of script information dictionaries with keys:
        - name: str (script filename)
        - size: int (file size in bytes)
        - last_modified: datetime (last modification time)
        - abfss_path: str (full ABFSS path)
        
    Raises:
        Exception: If listing fails
        
    Example:
        >>> list_scripts_in_datalake('mycontainer', 'myaccount')
        [
            {
                'name': 'myscript.py',
                'size': 1024,
                'last_modified': datetime(2024, 1, 1, 12, 0, 0),
                'abfss_path': 'abfss://mycontainer@myaccount.dfs.core.windows.net/scripts/myscript.py'
            }
        ]
    """
    try:
        # Initialize blob service client
        try:
            # Try using storage account key first
            storage_key = os.getenv('AZURE_STORAGE_KEY')
            if storage_key:
                connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={storage_key};EndpointSuffix=core.windows.net"
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Fallback to DefaultAzureCredential
                credential = DefaultAzureCredential()
                blob_service_client = BlobServiceClient(
                    account_url=f"https://{account_name}.blob.core.windows.net",
                    credential=credential
                )
        except Exception as auth_error:
            raise Exception(f"Failed to authenticate with Azure Storage: {str(auth_error)}")
        
        # Get container client
        container_client = blob_service_client.get_container_client(container)
        
        # List blobs with prefix
        scripts = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            # Only include Python files
            if blob.name.endswith('.py'):
                scripts.append({
                    'name': blob.name.split('/')[-1],  # Get filename without path
                    'size': blob.size,
                    'last_modified': blob.last_modified,
                    'abfss_path': f"abfss://{container}@{account_name}.dfs.core.windows.net/{blob.name}"
                })
        
        return scripts
        
    except Exception as e:
        raise Exception(f"Error listing scripts in Data Lake: {str(e)}")


def delete_script_from_datalake(path: str) -> bool:
    """
    Delete a Spark script from Azure Data Lake Storage.
    
    Args:
        path: ABFSS path to the script to delete
        
    Returns:
        True if deletion was successful
        
    Raises:
        ValueError: If path format is invalid
        Exception: If deletion fails
        
    Example:
        >>> delete_script_from_datalake('abfss://mycontainer@myaccount.dfs.core.windows.net/scripts/myscript.py')
        True
    """
    try:
        # Validate ABFSS path format
        if not path.startswith('abfss://'):
            raise ValueError("Path must be a valid ABFSS path starting with 'abfss://'")
        
        # Parse ABFSS path to extract components
        import re
        path_pattern = r'abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)'
        match = re.match(path_pattern, path)
        
        if not match:
            raise ValueError(f"Invalid ABFSS path format: {path}")
        
        container_name, storage_account, blob_path = match.groups()
        
        # Initialize blob service client
        try:
            # Try using storage account key first
            storage_key = os.getenv('AZURE_STORAGE_KEY')
            if storage_key:
                connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account};AccountKey={storage_key};EndpointSuffix=core.windows.net"
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            else:
                # Fallback to DefaultAzureCredential
                credential = DefaultAzureCredential()
                blob_service_client = BlobServiceClient(
                    account_url=f"https://{storage_account}.blob.core.windows.net",
                    credential=credential
                )
        except Exception as auth_error:
            raise Exception(f"Failed to authenticate with Azure Storage: {str(auth_error)}")
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_path
        )
        
        # Delete the blob
        try:
            blob_client.delete_blob()
            return True
        except Exception as delete_error:
            raise Exception(f"Failed to delete script: {str(delete_error)}")
        
    except Exception as e:
        raise Exception(f"Error deleting script from Data Lake: {str(e)}")


def validate_script_content(script_content: str) -> Dict[str, Any]:
    """
    Validate Spark script content for basic syntax and structure.
    
    Args:
        script_content: The Python script content as a string
        
    Returns:
        Dictionary with validation results:
        - valid: bool (True if script appears valid)
        - issues: List[str] (list of validation issues)
        - warnings: List[str] (list of warnings)
        
    Example:
        >>> validate_script_content('print("Hello")')
        {'valid': True, 'issues': [], 'warnings': []}
    """
    issues = []
    warnings = []
    
    try:
        # Check if script is empty
        if not script_content or not script_content.strip():
            issues.append("Script content is empty")
            return {'valid': False, 'issues': issues, 'warnings': warnings}
        
        # Check for basic Python syntax
        try:
            compile(script_content, '<string>', 'exec')
        except SyntaxError as e:
            issues.append(f"Python syntax error: {str(e)}")
        
        # Check for Spark-specific patterns
        spark_imports = [
            'from pyspark.sql import',
            'import pyspark',
            'SparkSession',
            'spark.read'
        ]
        
        spark_patterns_found = []
        for pattern in spark_imports:
            if pattern in script_content:
                spark_patterns_found.append(pattern)
        
        if not spark_patterns_found:
            warnings.append("No Spark-specific patterns detected - ensure this is a Spark script")
        
        # Check for common Spark best practices
        if 'spark.read' in script_content and 'SparkSession' not in script_content:
            warnings.append("Using spark.read without explicit SparkSession creation")
        
        if 'collect()' in script_content:
            warnings.append("Using collect() - consider if this is necessary for large datasets")
        
        # Check for potential memory issues
        if 'cache()' in script_content and 'unpersist()' not in script_content:
            warnings.append("Using cache() without unpersist() - consider memory management")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'warnings': warnings
        }
        
    except Exception as e:
        return {
            'valid': False,
            'issues': [f"Validation failed: {str(e)}"],
            'warnings': []
        }
