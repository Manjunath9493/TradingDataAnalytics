# Trading Data Analytics Pipeline (Raw → Bronze → Silver → Gold)

A fully modularized, end-to-end data engineering pipeline built with PySpark, Azure Databricks, Azure Data Factory (ADF), Azure KeyVault and Azure Data Lake Storage (ADLS).  
This project is structured for real-world, production-ready deployment and demonstrates advanced ETL orchestration, data transformation, and layered architecture.

# Tech Stack

- PySpark (Distributed Data Processing)
- Azure Data Factory (Pipeline Orchestration)
- Azure Data Lake Storage (Storage Layer)
- Databricks (Cluster Execution)
- GitHub (Version Control & Deployment Ready)

# Project Architecture

        ┌────────────┐        ┌─────────────┐        ┌─────────────┐        ┌────────────┐
        │ Raw Layer  │──────▶│ Bronze Layer│ ──────▶│ Silver Layer│──────▶│ Gold Layer │
        └────────────┘        └─────────────┘        └─────────────┘        └────────────┘
         (RawData.py)         (BronzeData.py)        (SilverData.py)        (GoldData.py)
               │                     │                       │                     │
       API / CSV Ingest     Quality Checks & Logs    Transformation & Joins  Business Calculations
