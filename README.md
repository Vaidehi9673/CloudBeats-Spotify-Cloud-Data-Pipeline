# 🎧 CloudBeats: Spotify Cloud Data Pipeline

**CloudBeats** is a cloud-native data engineering pipeline designed to ingest, transform, and analyze a decade of Spotify music trends (2015–2025). This project demonstrates a production-grade **Medallion Architecture** using AWS S3 and Databricks.

---

## Architecture Overview
The pipeline follows the industry-standard **Lakehouse** pattern:

1.  **Ingestion Layer (Landing):** A local Python script utilizing the `boto3` library extracts raw CSV data and securely uploads it to an **AWS S3 Landing Zone**.
2.  **Bronze Layer (Raw):** Databricks (PySpark) reads the raw CSV from S3, adds ingestion metadata (timestamps), and converts the data into **Delta Lake** format for optimized storage.
3.  **Silver Layer (Cleaned):** PySpark processes the Bronze data—performing schema enforcement, type casting (dates/integers), and **Feature Engineering** (e.g., converting milliseconds to minutes and categorizing track energy levels).
4.  **Gold Layer (Analytics):** (In Progress) Final business-level aggregations to provide insights into global music trends.

---

## Tech Stack
- **Languages:** Python, PySpark, SQL
- **Cloud Infrastructure:** AWS S3, IAM
- **Data Processing:** Databricks (Serverless Compute)
- **Data Governance:** Unity Catalog (External Locations)
- **Storage**: Delta Lake (ACID transactions, Time Travel)
- **Orchestration**: Databricks Workflows (Job scheduling & dependencies)
- **Visualization**: Databricks SQL Dashboards
- **DevOps/Tools:** Git, VS Code, python-dotenv

---

## Key Features & Problem Solving
- **Security-First Approach:** Implemented **Environment Variables** for local scripts and **Unity Catalog External Locations** in Databricks to eliminate hardcoded AWS credentials.
- **Scalable Processing:** Leveraged **Databricks Serverless** and Spark Session configurations to handle distributed data processing.
- **Medallion Architecture:** Built a modular pipeline that separates raw ingestion from business logic, ensuring high data quality and lineage.
- **Data Validation:** Integrated validation checks to ensure row count consistency during the transition from Landing to Silver layers.

## Data Pipeline Architecture
- Workflow DAG
## Executive Insights Dashboard
CloudBeats Dashboard


## Project Structure
```text
Spotify-Cloud-Pipeline/
├── ingestion/
│   └── loading.py               # Local to S3 script
├── notebooks/
│   ├── 01_bronze_ingestion.py   # Bronze Layer
│   ├── 02_silver_cleaning.py    # Silver Layer
│   └── 03_silver_to_gold.py      # Gold Layer (Add this too!)
├── images/                      
│   ├── workflow_dag.png         # Screenshot of your Green Job Run
│   └── final_dashboard.png      # Screenshot of your CloudBeats Dashboard
├── .gitignore                   
├── README.md                    
└── requirements.txt
