"""
LLM Agent for Spark optimization using Azure OpenAI.
Provides intelligent suggestions and analysis for Spark applications.
"""

import os
from typing import Dict, List, Any, Optional
from openai import AzureOpenAI
from langchain_openai import AzureChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import ChatPromptTemplate

# Environment variable placeholders
# AZURE_OPENAI_API_KEY=your_openai_api_key_here
# AZURE_OPENAI_ENDPOINT=your_openai_endpoint_here
# AZURE_OPENAI_API_VERSION=2024-02-15-preview
# AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment_name_here

class SparkOptimizationAgent:
    """LLM agent for Spark optimization and analysis."""
    
    def __init__(self):
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")
        self.deployment_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        
        if not all([self.api_key, self.endpoint, self.deployment_name]):
            raise ValueError("Azure OpenAI credentials not properly configured. Please set environment variables.")
        
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version=self.api_version,
            azure_endpoint=self.endpoint
        )
        
        # Initialize LangChain Azure OpenAI
        self.llm = AzureChatOpenAI(
            azure_deployment=self.deployment_name,
            openai_api_version=self.api_version,
            azure_endpoint=self.endpoint,
            api_key=self.api_key,
            temperature=0.1
        )
    
    def analyze_spark_code(self, spark_code: str, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Analyze Spark code using LLM for optimization suggestions.
        
        Args:
            spark_code: The Spark program code to analyze
            metadata: Optional metadata about data sources
            
        Returns:
            Dictionary with analysis results and recommendations
        """
        try:
            # Create system prompt for Spark analysis
            system_prompt = self._create_spark_analysis_prompt(metadata)
            
            # Create user message with Spark code
            user_message = f"""
            Please analyze the following Spark code and provide optimization recommendations:
            
            ```python
            {spark_code}
            ```
            
            Please provide:
            1. Performance analysis
            2. Configuration recommendations
            3. Code optimization suggestions
            4. Best practices recommendations
            5. Potential issues and warnings
            """
            
            # Get LLM response
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message)
            ]
            
            response = self.llm.invoke(messages)
            
            # Parse and structure the response
            analysis = self._parse_llm_response(response.content)
            
            return analysis
            
        except Exception as e:
            return {
                "error": f"LLM analysis failed: {str(e)}",
                "recommendations": [],
                "warnings": [],
                "configurations": {}
            }
    
    def suggest_configurations(self, spark_code: str, data_characteristics: Dict[str, Any]) -> Dict[str, str]:
        """
        Suggest optimal Spark configurations based on code and data characteristics.
        
        Args:
            spark_code: The Spark program code
            data_characteristics: Characteristics of the data being processed
            
        Returns:
            Dictionary of suggested Spark configurations
        """
        try:
            system_prompt = """
            You are a Spark optimization expert. Based on the Spark code and data characteristics provided,
            suggest optimal Spark configurations. Return only the configuration key-value pairs in a clear format.
            
            Focus on:
            - Memory configurations (executor memory, driver memory)
            - Parallelism settings (number of executors, cores per executor)
            - Adaptive query execution settings
            - Shuffle and serialization settings
            - Dynamic allocation settings
            """
            
            user_message = f"""
            Spark Code:
            ```python
            {spark_code}
            ```
            
            Data Characteristics:
            {data_characteristics}
            
            Please suggest optimal Spark configurations for this scenario.
            """
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message)
            ]
            
            response = self.llm.invoke(messages)
            
            # Parse configurations from response
            configs = self._parse_configurations(response.content)
            
            return configs
            
        except Exception as e:
            return {
                "error": f"Configuration suggestion failed: {str(e)}",
                "spark.executor.memory": "4g",
                "spark.driver.memory": "2g",
                "spark.sql.adaptive.enabled": "true"
            }
    
    def generate_optimized_code(self, original_code: str, issues: List[str]) -> str:
        """
        Generate optimized version of Spark code based on identified issues.
        
        Args:
            original_code: The original Spark code
            issues: List of issues to address
            
        Returns:
            Optimized Spark code
        """
        try:
            system_prompt = """
            You are a Spark optimization expert. Given the original Spark code and a list of issues,
            generate an optimized version that addresses these issues while maintaining the same functionality.
            
            Focus on:
            - Performance optimizations
            - Best practices
            - Memory efficiency
            - Proper resource management
            """
            
            user_message = f"""
            Original Spark Code:
            ```python
            {original_code}
            ```
            
            Issues to Address:
            {chr(10).join(f"- {issue}" for issue in issues)}
            
            Please provide an optimized version of this code that addresses the identified issues.
            """
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_message)
            ]
            
            response = self.llm.invoke(messages)
            
            return response.content
            
        except Exception as e:
            return f"# Error generating optimized code: {str(e)}\n\n{original_code}"
    
    def _create_spark_analysis_prompt(self, metadata: Dict[str, Any] = None) -> str:
        """Create system prompt for Spark code analysis."""
        base_prompt = """
        You are an expert Spark optimization consultant with deep knowledge of:
        - Apache Spark architecture and internals
        - Performance tuning and optimization techniques
        - Memory management and resource allocation
        - Data partitioning and shuffle optimization
        - Join strategies and broadcast optimization
        - Caching strategies and persistence
        - Adaptive query execution
        - Azure Databricks and Synapse Spark specifics
        
        Analyze the provided Spark code and provide comprehensive recommendations for:
        1. Performance improvements
        2. Configuration optimizations
        3. Code structure and best practices
        4. Memory and resource management
        5. Potential bottlenecks and issues
        6. Azure-specific optimizations
        
        Provide specific, actionable recommendations with explanations.
        """
        
        if metadata:
            base_prompt += f"\n\nAdditional context about data sources:\n{metadata}"
        
        return base_prompt
    
    def _parse_llm_response(self, response: str) -> Dict[str, Any]:
        """Parse LLM response into structured format."""
        # This is a simplified parser - in production, you might want more sophisticated parsing
        analysis = {
            "summary": "",
            "recommendations": [],
            "warnings": [],
            "configurations": {},
            "performance_insights": []
        }
        
        # Simple parsing logic - extract sections based on common patterns
        lines = response.split('\n')
        current_section = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Detect sections
            if "recommendation" in line.lower() or "suggestion" in line.lower():
                current_section = "recommendations"
            elif "warning" in line.lower() or "issue" in line.lower():
                current_section = "warnings"
            elif "configuration" in line.lower() or "config" in line.lower():
                current_section = "configurations"
            elif "performance" in line.lower():
                current_section = "performance_insights"
            
            # Add content to appropriate section
            if current_section == "recommendations" and line.startswith('-'):
                analysis["recommendations"].append(line[1:].strip())
            elif current_section == "warnings" and line.startswith('-'):
                analysis["warnings"].append(line[1:].strip())
            elif current_section == "performance_insights" and line.startswith('-'):
                analysis["performance_insights"].append(line[1:].strip())
        
        return analysis
    
    def _parse_configurations(self, response: str) -> Dict[str, str]:
        """Parse configuration suggestions from LLM response."""
        configs = {}
        
        # Simple parsing for key=value patterns
        lines = response.split('\n')
        for line in lines:
            line = line.strip()
            if '=' in line and line.startswith('spark.'):
                parts = line.split('=', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip().strip('"\'')
                    configs[key] = value
        
        return configs
    
    def get_optimization_score(self, original_code: str, optimized_code: str) -> Dict[str, Any]:
        """
        Calculate optimization score and metrics.
        
        Args:
            original_code: Original Spark code
            optimized_code: Optimized Spark code
            
        Returns:
            Dictionary with optimization metrics and score
        """
        # Simple metrics calculation
        original_lines = len(original_code.split('\n'))
        optimized_lines = len(optimized_code.split('\n'))
        
        # Count various operations for complexity analysis
        original_ops = {
            'joins': original_code.count('join('),
            'groupby': original_code.count('groupBy('),
            'shuffle': original_code.count('repartition(') + original_code.count('orderBy(')
        }
        
        optimized_ops = {
            'joins': optimized_code.count('join('),
            'groupby': optimized_code.count('groupBy('),
            'shuffle': optimized_code.count('repartition(') + optimized_code.count('orderBy(')
        }
        
        # Calculate improvement scores
        shuffle_improvement = max(0, (original_ops['shuffle'] - optimized_ops['shuffle']) / max(original_ops['shuffle'], 1)) * 100
        
        return {
            "original_complexity": original_ops,
            "optimized_complexity": optimized_ops,
            "shuffle_improvement_percent": shuffle_improvement,
            "code_length_change": optimized_lines - original_lines,
            "overall_score": min(100, shuffle_improvement + 20)  # Simple scoring
        }
