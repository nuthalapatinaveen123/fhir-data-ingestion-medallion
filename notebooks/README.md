# Notebooks Directory
This directory contains all PySpark notebooks and SQL scripts for the Medallion Architecture:
- `01_bronze_ingestion.py`: PySpark script for fetching raw FHIR API data with pagination and metadata.
- `02_silver_transformation.py`: PySpark script for parsing, cleaning, and SCD Type 2 logic.
- `03_gold_analytics.sql`: SQL queries and view definitions for analytical star schema modeling.
