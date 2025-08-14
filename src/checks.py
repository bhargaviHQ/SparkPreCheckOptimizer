"""
Spark performance and configuration validation checks.
Provides various checks for Spark applications, configurations, and data sources.
"""

import re
import json
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count

@dataclass
class CheckResult:
    """Result of a Spark check."""
    check_name: str
    status: str  # "PASS", "WARNING", "FAIL"
    message: str
    recommendation: Optional[str] = None
    severity: str = "INFO"  # "LOW", "MEDIUM", "HIGH", "CRITICAL"

class SparkChecks:
    """Collection of Spark performance and configuration checks."""
    
    def __init__(self):
        self.results: List[CheckResult] = []
    
    def analyze_spark_code(self, spark_code: str) -> List[CheckResult]:
        """
        Analyze Spark code for common issues and optimization opportunities.
        
        Args:
            spark_code: The Spark program code to analyze
            
        Returns:
            List of check results
        """
        self.results = []
        
        # Perform various checks
        self._check_basic_syntax(spark_code)
        self._check_memory_usage(spark_code)
        self._check_partitioning(spark_code)
        self._check_caching_strategy(spark_code)
        self._check_data_skew(spark_code)
        self._check_join_strategies(spark_code)
        self._check_shuffle_operations(spark_code)
        self._check_broadcast_joins(spark_code)
        
        return self.results
    
    def _check_basic_syntax(self, spark_code: str):
        """Check for basic Spark syntax and patterns."""
        # Check for common Spark imports
        if "from pyspark.sql import SparkSession" not in spark_code:
            self.results.append(CheckResult(
                check_name="SparkSession Import",
                status="WARNING",
                message="SparkSession import not found",
                recommendation="Add 'from pyspark.sql import SparkSession' at the top of your code",
                severity="MEDIUM"
            ))
        
        # Check for SparkSession creation
        if "SparkSession.builder" not in spark_code:
            self.results.append(CheckResult(
                check_name="SparkSession Creation",
                status="FAIL",
                message="SparkSession not properly initialized",
                recommendation="Initialize SparkSession using SparkSession.builder.appName().getOrCreate()",
                severity="HIGH"
            ))
    
    def _check_memory_usage(self, spark_code: str):
        """Check for memory-related configurations and patterns."""
        # Check for memory configurations
        memory_configs = [
            "spark.executor.memory",
            "spark.driver.memory",
            "spark.sql.adaptive.enabled"
        ]
        
        for config in memory_configs:
            if config not in spark_code:
                self.results.append(CheckResult(
                    check_name=f"Memory Configuration: {config}",
                    status="WARNING",
                    message=f"Memory configuration '{config}' not found",
                    recommendation=f"Consider setting {config} for better memory management",
                    severity="MEDIUM"
                ))
    
    def _check_partitioning(self, spark_code: str):
        """Check for partitioning strategies."""
        # Check for repartition/coalesce usage
        if "repartition(" in spark_code:
            self.results.append(CheckResult(
                check_name="Repartition Usage",
                status="INFO",
                message="Repartition operation detected",
                recommendation="Ensure repartition is used appropriately - consider coalesce for reducing partitions",
                severity="LOW"
            ))
        
        if "coalesce(" in spark_code:
            self.results.append(CheckResult(
                check_name="Coalesce Usage",
                status="PASS",
                message="Coalesce operation detected - good for reducing partitions",
                severity="LOW"
            ))
    
    def _check_caching_strategy(self, spark_code: str):
        """Check for caching strategies."""
        cache_operations = ["cache()", "persist()", "unpersist()"]
        
        for op in cache_operations:
            if op in spark_code:
                self.results.append(CheckResult(
                    check_name=f"Caching: {op}",
                    status="PASS",
                    message=f"Caching operation '{op}' detected",
                    recommendation="Ensure cached data is used multiple times to justify caching",
                    severity="LOW"
                ))
    
    def _check_data_skew(self, spark_code: str):
        """Check for potential data skew issues."""
        # Check for groupBy operations without proper handling
        if "groupBy(" in spark_code and "salting" not in spark_code:
            self.results.append(CheckResult(
                check_name="Data Skew Prevention",
                status="WARNING",
                message="GroupBy operation detected without explicit skew handling",
                recommendation="Consider using salting techniques or adaptive query execution for skewed data",
                severity="MEDIUM"
            ))
    
    def _check_join_strategies(self, spark_code: str):
        """Check for join strategies and optimizations."""
        join_types = ["join(", "leftJoin(", "rightJoin(", "fullOuterJoin("]
        
        for join_type in join_types:
            if join_type in spark_code:
                self.results.append(CheckResult(
                    check_name=f"Join Strategy: {join_type}",
                    status="INFO",
                    message=f"Join operation '{join_type}' detected",
                    recommendation="Consider broadcast joins for small tables and ensure proper join conditions",
                    severity="MEDIUM"
                ))
    
    def _check_shuffle_operations(self, spark_code: str):
        """Check for shuffle-heavy operations."""
        shuffle_ops = ["groupBy(", "orderBy(", "sort(", "distinct("]
        
        for op in shuffle_ops:
            if op in spark_code:
                self.results.append(CheckResult(
                    check_name=f"Shuffle Operation: {op}",
                    status="WARNING",
                    message=f"Shuffle operation '{op}' detected",
                    recommendation="Shuffle operations are expensive - consider if this operation is necessary",
                    severity="MEDIUM"
                ))
    
    def _check_broadcast_joins(self, spark_code: str):
        """Check for broadcast join opportunities."""
        if "join(" in spark_code and "broadcast(" not in spark_code:
            self.results.append(CheckResult(
                check_name="Broadcast Join Opportunity",
                status="INFO",
                message="Join operation detected without broadcast hint",
                recommendation="Consider using broadcast() for small tables to avoid shuffle",
                severity="MEDIUM"
            ))
    
    def validate_configurations(self, configs: Dict[str, str]) -> List[CheckResult]:
        """
        Validate Spark configurations for best practices.
        
        Args:
            configs: Dictionary of Spark configurations
            
        Returns:
            List of configuration validation results
        """
        config_results = []
        
        # Check for essential configurations
        essential_configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
        
        for config, recommended_value in essential_configs.items():
            if config not in configs:
                config_results.append(CheckResult(
                    check_name=f"Configuration: {config}",
                    status="WARNING",
                    message=f"Recommended configuration '{config}' not set",
                    recommendation=f"Set {config}={recommended_value} for better performance",
                    severity="MEDIUM"
                ))
            elif configs[config] != recommended_value:
                config_results.append(CheckResult(
                    check_name=f"Configuration: {config}",
                    status="WARNING",
                    message=f"Configuration '{config}' set to '{configs[config]}' instead of '{recommended_value}'",
                    recommendation=f"Consider setting {config}={recommended_value}",
                    severity="LOW"
                ))
        
        return config_results
    
    def get_performance_metrics(self, spark_code: str) -> Dict[str, Any]:
        """
        Extract potential performance metrics from Spark code.
        
        Args:
            spark_code: The Spark program code
            
        Returns:
            Dictionary with performance-related metrics and insights
        """
        metrics = {
            "estimated_shuffle_operations": 0,
            "join_operations": 0,
            "aggregation_operations": 0,
            "caching_operations": 0,
            "potential_optimizations": []
        }
        
        # Count operations
        metrics["join_operations"] = spark_code.count("join(")
        metrics["aggregation_operations"] = spark_code.count("groupBy(") + spark_code.count("agg(")
        metrics["caching_operations"] = spark_code.count("cache(") + spark_code.count("persist(")
        
        # Estimate shuffle operations
        shuffle_patterns = ["groupBy(", "orderBy(", "sort(", "distinct(", "join("]
        for pattern in shuffle_patterns:
            metrics["estimated_shuffle_operations"] += spark_code.count(pattern)
        
        # Identify potential optimizations
        if metrics["join_operations"] > 0 and "broadcast(" not in spark_code:
            metrics["potential_optimizations"].append("Consider broadcast joins for small tables")
        
        if metrics["aggregation_operations"] > 0 and "repartition(" not in spark_code:
            metrics["potential_optimizations"].append("Consider repartitioning before aggregations")
        
        return metrics


def check_existence(input_type: str, path_or_table: str, spark: SparkSession, 
                   connection_str: str = None) -> Dict[str, Any]:
    """
    Check if a data source exists and is accessible.
    
    Args:
        input_type: Type of input ('datalake' or 'synapse')
        path_or_table: ABFSS path for Data Lake or table name for Synapse
        spark: PySpark SparkSession instance
        connection_str: JDBC connection string (required for Synapse)
        
    Returns:
        Dictionary with keys:
        - exists: bool (True if data source exists and is accessible)
        - message: str (description of the check result)
        
    Raises:
        ValueError: If input_type is not supported or connection_str missing for Synapse
        Exception: If check fails due to other errors
        
    Note:
        For Synapse, connection_str should include credentials:
        'jdbc:sqlserver://workspace.sql.azuresynapse.net:1433;database=db;user=user;password=pass'
    """
    try:
        if input_type.lower() == 'datalake':
            return _check_datalake_existence(path_or_table, spark)
        elif input_type.lower() == 'synapse':
            if not connection_str:
                raise ValueError("Connection string is required for Synapse existence check")
            return _check_synapse_existence(path_or_table, connection_str, spark)
        else:
            raise ValueError(f"Unsupported input type: {input_type}. Use 'datalake' or 'synapse'")
            
    except Exception as e:
        return {
            'exists': False,
            'message': f"Existence check failed: {str(e)}"
        }


def _check_datalake_existence(path: str, spark: SparkSession) -> Dict[str, Any]:
    """Check if Data Lake path exists and is accessible."""
    try:
        # Validate ABFSS path format
        if not path.startswith('abfss://'):
            return {
                'exists': False,
                'message': f"Invalid ABFSS path format: {path}"
            }
        
        # Try to read the path (lightweight - just check if it's accessible)
        df = spark.read.format('parquet').load(path).limit(1)
        
        # If we get here, the path exists and is accessible
        return {
            'exists': True,
            'message': f"Data Lake path exists and is accessible: {path}"
        }
        
    except Exception as e:
        return {
            'exists': False,
            'message': f"Data Lake path not accessible: {str(e)}"
        }


def _check_synapse_existence(table_name: str, connection_str: str, spark: SparkSession) -> Dict[str, Any]:
    """Check if Synapse table exists and is accessible."""
    try:
        # Validate inputs
        if not table_name or not connection_str:
            return {
                'exists': False,
                'message': "Table name and connection string are required"
            }
        
        # Check for embedded credentials warning
        if 'password=' in connection_str.lower():
            print("Warning: Credentials embedded in connection string. Consider using secure authentication methods.")
        
        # Try to read the table (lightweight - just check if it's accessible)
        df = spark.read.jdbc(url=connection_str, table=table_name).limit(1)
        
        # If we get here, the table exists and is accessible
        return {
            'exists': True,
            'message': f"Synapse table exists and is accessible: {table_name}"
        }
        
    except Exception as e:
        return {
            'exists': False,
            'message': f"Synapse table not accessible: {str(e)}"
        }


def check_sanity(metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check if metadata indicates a sane/valid dataset.
    
    Args:
        metadata: Dictionary containing metadata (from get_datalake_metadata or get_synapse_metadata)
        
    Returns:
        Dictionary with keys:
        - sane: bool (True if dataset appears sane)
        - issues: List[str] (list of issues found, empty if sane)
        
    Example:
        metadata = {
            'type': 'datalake',
            'row_count': 1000,
            'schema': '{"type":"struct","fields":[...]}',
            'file_size_gb': 0.5
        }
    """
    issues = []
    
    try:
        # Check if metadata has required fields
        if not isinstance(metadata, dict):
            issues.append("Invalid metadata format")
            return {'sane': False, 'issues': issues}
        
        # Check row count
        row_count = metadata.get('row_count')
        if row_count is None:
            issues.append("Row count not available")
        elif row_count <= 0:
            issues.append("Empty dataset (row_count <= 0)")
        
        # Check schema
        schema = metadata.get('schema')
        if not schema:
            issues.append("Schema not available")
        else:
            try:
                # Validate schema JSON
                schema_dict = json.loads(schema)
                if not schema_dict.get('fields'):
                    issues.append("Schema has no fields")
                elif len(schema_dict['fields']) == 0:
                    issues.append("Schema has empty fields list")
            except json.JSONDecodeError:
                issues.append("Invalid schema JSON format")
        
        # Check file size for Data Lake
        if metadata.get('type') == 'datalake':
            file_size_gb = metadata.get('file_size_gb')
            if file_size_gb is not None:
                if file_size_gb <= 0:
                    issues.append("File size is zero or negative")
                elif file_size_gb > 100:  # 100GB threshold
                    issues.append("Very large file (>100GB) - may cause performance issues")
        
        # Check for reasonable row count
        if row_count and row_count > 1000000000:  # 1 billion rows
            issues.append("Very large dataset (>1B rows) - may cause memory issues")
        
        return {
            'sane': len(issues) == 0,
            'issues': issues
        }
        
    except Exception as e:
        return {
            'sane': False,
            'issues': [f"Sanity check failed: {str(e)}"]
        }


def check_data_quality(df: DataFrame, spark: SparkSession, sample_fraction: float = 0.1) -> Dict[str, Any]:
    """
    Check data quality of a DataFrame.
    
    Args:
        df: PySpark DataFrame to check
        spark: PySpark SparkSession instance
        sample_fraction: Fraction of data to sample for quality checks (default: 0.1 for 10%)
        
    Returns:
        Dictionary with keys:
        - quality_issues: List[str] (list of quality issues found)
        - null_percentages: Dict[str, float] (null percentage per column)
        - duplicate_count: int (number of duplicate rows)
        - basic_stats: Dict[str, Any] (basic statistics for numeric columns)
        
    Note:
        For large datasets, uses sampling to avoid memory issues.
        Environment variables for Azure configuration:
        - AZURE_STORAGE_ACCOUNT_NAME
        - AZURE_TENANT_ID
        - AZURE_CLIENT_ID
        - AZURE_CLIENT_SECRET
    """
    quality_issues = []
    null_percentages = {}
    duplicate_count = 0
    basic_stats = {}
    
    try:
        # Determine if we need to sample based on dataset size
        total_rows = df.count()
        
        if total_rows > 100000:  # Sample if more than 100k rows
            print(f"Large dataset detected ({total_rows} rows). Sampling {sample_fraction*100}% for quality checks.")
            df_sample = df.sample(fraction=sample_fraction, seed=42)
            sample_rows = df_sample.count()
        else:
            df_sample = df
            sample_rows = total_rows
        
        # Check null percentages
        columns = df_sample.columns
        for column in columns:
            try:
                null_count = df_sample.filter(col(column).isNull()).count()
                null_percentage = (null_count / sample_rows) * 100
                null_percentages[column] = null_percentage
                
                # Flag high null percentages
                if null_percentage > 50:
                    quality_issues.append(f"High null percentage ({null_percentage:.1f}%) in column '{column}'")
                elif null_percentage > 10:
                    quality_issues.append(f"Moderate null percentage ({null_percentage:.1f}%) in column '{column}'")
                    
            except Exception as e:
                print(f"Warning: Could not check nulls for column '{column}': {e}")
        
        # Check for duplicates
        try:
            distinct_count = df_sample.dropDuplicates().count()
            duplicate_count = sample_rows - distinct_count
            
            if duplicate_count > 0:
                duplicate_percentage = (duplicate_count / sample_rows) * 100
                quality_issues.append(f"Duplicates found: {duplicate_count} duplicate rows ({duplicate_percentage:.1f}%)")
                
        except Exception as e:
            print(f"Warning: Could not check duplicates: {e}")
        
        # Basic statistics for numeric columns
        try:
            numeric_columns = []
            for field in df_sample.schema.fields:
                if field.dataType.typeName() in ['integer', 'long', 'double', 'float', 'decimal']:
                    numeric_columns.append(field.name)
            
            if numeric_columns:
                # Get basic statistics
                stats_df = df_sample.select(numeric_columns).summary("count", "min", "max", "mean")
                stats_dict = {}
                
                for col_name in numeric_columns:
                    col_stats = {}
                    for stat_row in stats_df.collect():
                        stat_name = stat_row['summary']
                        if stat_name in ['count', 'min', 'max', 'mean']:
                            col_stats[stat_name] = stat_row[col_name]
                    stats_dict[col_name] = col_stats
                
                basic_stats = stats_dict
                
                # Check for potential data quality issues in numeric columns
                for col_name, stats in stats_dict.items():
                    try:
                        min_val = float(stats.get('min', 0))
                        max_val = float(stats.get('max', 0))
                        
                        # Check for negative values in columns that shouldn't be negative
                        if min_val < 0 and any(keyword in col_name.lower() for keyword in ['count', 'amount', 'price', 'quantity', 'size']):
                            quality_issues.append(f"Negative values found in column '{col_name}' (min: {min_val})")
                        
                        # Check for zero values in columns that shouldn't be zero
                        if min_val == 0 and max_val == 0 and any(keyword in col_name.lower() for keyword in ['amount', 'price', 'quantity']):
                            quality_issues.append(f"All values are zero in column '{col_name}'")
                            
                    except (ValueError, TypeError):
                        continue
                        
        except Exception as e:
            print(f"Warning: Could not compute basic statistics: {e}")
        
        # Check for empty columns
        for column in columns:
            try:
                non_null_count = df_sample.filter(col(column).isNotNull()).count()
                if non_null_count == 0:
                    quality_issues.append(f"Empty column: '{column}' has no non-null values")
            except Exception as e:
                print(f"Warning: Could not check emptiness for column '{column}': {e}")
        
        return {
            'quality_issues': quality_issues,
            'null_percentages': null_percentages,
            'duplicate_count': duplicate_count,
            'basic_stats': basic_stats,
            'total_rows_checked': sample_rows,
            'sampling_used': total_rows > 100000
        }
        
    except Exception as e:
        return {
            'quality_issues': [f"Data quality check failed: {str(e)}"],
            'null_percentages': {},
            'duplicate_count': 0,
            'basic_stats': {},
            'total_rows_checked': 0,
            'sampling_used': False
        }


def run_pre_execution_checks(input_type: str, path_or_table: str, spark: SparkSession, 
                           connection_str: str = None, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Run comprehensive pre-execution checks on Azure data sources.
    
    Args:
        input_type: Type of input ('datalake' or 'synapse')
        path_or_table: ABFSS path for Data Lake or table name for Synapse
        spark: PySpark SparkSession instance
        connection_str: JDBC connection string (required for Synapse)
        metadata: Optional metadata dictionary (if already computed)
        
    Returns:
        Dictionary containing all check results:
        - existence_check: Result from check_existence
        - sanity_check: Result from check_sanity (if metadata provided)
        - quality_check: Result from check_data_quality (if data accessible)
        - overall_status: 'PASS', 'WARNING', or 'FAIL'
        - recommendations: List of recommendations
        
    Example:
        # For Data Lake
        results = run_pre_execution_checks('datalake', 'abfss://container@account.dfs.core.windows.net/file.parquet', spark)
        
        # For Synapse
        results = run_pre_execution_checks('synapse', 'my_table', spark, 'jdbc:sqlserver://...')
    """
    results = {
        'existence_check': None,
        'sanity_check': None,
        'quality_check': None,
        'overall_status': 'UNKNOWN',
        'recommendations': []
    }
    
    try:
        # Step 1: Check existence
        results['existence_check'] = check_existence(input_type, path_or_table, spark, connection_str)
        
        if not results['existence_check']['exists']:
            results['overall_status'] = 'FAIL'
            results['recommendations'].append("Fix data source accessibility issues before proceeding")
            return results
        
        # Step 2: Check sanity (if metadata provided)
        if metadata:
            results['sanity_check'] = check_sanity(metadata)
            if not results['sanity_check']['sane']:
                results['overall_status'] = 'WARNING'
                results['recommendations'].extend(results['sanity_check']['issues'])
        
        # Step 3: Check data quality (if data is accessible)
        try:
            if input_type.lower() == 'datalake':
                df = spark.read.format('parquet').load(path_or_table)
            elif input_type.lower() == 'synapse':
                df = spark.read.jdbc(url=connection_str, table=path_or_table)
            else:
                raise ValueError(f"Unsupported input type: {input_type}")
            
            # Sample data for quality checks to avoid memory issues
            df_sample = df.limit(10000)  # Limit to 10k rows for quality checks
            results['quality_check'] = check_data_quality(df_sample, spark)
            
            if results['quality_check']['quality_issues']:
                if results['overall_status'] == 'PASS':
                    results['overall_status'] = 'WARNING'
                results['recommendations'].extend(results['quality_check']['quality_issues'])
                
        except Exception as e:
            results['quality_check'] = {
                'quality_issues': [f"Could not perform quality checks: {str(e)}"],
                'null_percentages': {},
                'duplicate_count': 0,
                'basic_stats': {},
                'total_rows_checked': 0,
                'sampling_used': False
            }
            results['overall_status'] = 'WARNING'
        
        # Determine overall status
        if results['overall_status'] == 'UNKNOWN':
            results['overall_status'] = 'PASS'
        
        # Add general recommendations
        if results['overall_status'] == 'PASS':
            results['recommendations'].append("All pre-execution checks passed successfully")
        elif results['overall_status'] == 'WARNING':
            results['recommendations'].append("Review warnings before proceeding with Spark job")
        else:
            results['recommendations'].append("Fix critical issues before proceeding with Spark job")
        
        return results
        
    except Exception as e:
        return {
            'existence_check': {'exists': False, 'message': f"Check failed: {str(e)}"},
            'sanity_check': None,
            'quality_check': None,
            'overall_status': 'FAIL',
            'recommendations': [f"Pre-execution checks failed: {str(e)}"]
        }
