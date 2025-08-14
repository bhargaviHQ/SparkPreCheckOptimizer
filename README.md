# SparkPreCheckOptimizer

SparkPreCheckOptimizer is a UI app that analyzes Azure Data Lake files (Parquet) or Synapse tables and a Spark program, suggesting configs, checks, and more using Azure OpenAI.

## Features

- **Data Source Analysis**: Connect to Azure Data Lake Storage and Azure Synapse Analytics
- **Spark Code Analysis**: Analyze Spark programs for performance optimizations
- **LLM-Powered Suggestions**: Get intelligent recommendations using Azure OpenAI
- **Configuration Optimization**: Receive optimal Spark configuration suggestions
- **Performance Checks**: Comprehensive validation of Spark applications

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up environment variables:
   ```bash
   # Azure OpenAI
   AZURE_OPENAI_API_KEY=your_openai_api_key_here
   AZURE_OPENAI_ENDPOINT=your_openai_endpoint_here
   AZURE_OPENAI_DEPLOYMENT_NAME=your_deployment_name_here
   
   # Azure Storage
   AZURE_STORAGE_ACCOUNT_NAME=your_storage_account_name_here
   AZURE_TENANT_ID=your_tenant_id_here
   AZURE_CLIENT_ID=your_client_id_here
   AZURE_CLIENT_SECRET=your_client_secret_here
   
   # Azure Synapse (optional)
   SYNAPSE_WORKSPACE_NAME=your_workspace_name_here
   SYNAPSE_SPARK_POOL_NAME=your_spark_pool_name_here
   ```

3. Run the application:
   ```bash
   streamlit run src/app.py
   ```

## Project Structure

```
SparkPreCheckOptimizer/
├── src/
│   ├── app.py              # Main Streamlit application
│   ├── metadata.py         # Data source metadata handling
│   ├── checks.py           # Spark performance checks
│   ├── llm_agent.py        # Azure OpenAI integration
│   └── storage.py          # Azure Storage and Synapse connectivity
├── requirements.txt        # Python dependencies
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Requirements

- Python 3.9+
- Azure subscription with OpenAI service
- Azure Data Lake Storage or Synapse Analytics access
- Compatible with Azure Databricks environments
