# fhir-data-ingestion-medallion


This repository implements an enterprise-grade, end-to-end data pipeline built on Microsoft Fabric to ingest, process, transform, and model healthcare data following the FHIR (Fast Healthcare Interoperability Resources) standard. The solution relies on a Medallion Architecture across Bronze, Silver, and Gold layers to handle dynamic API schemas, change data capture, and analytical star-schema modeling using PySpark, Delta Lake, and Spark SQL.

In the Bronze layer, the pipeline uses PySpark scripts to handle resilient REST API ingestion from raw FHIR endpoints. It incorporates dynamic pagination and exponential backoff retry mechanisms to manage HTTP rate limits and transient connection drops. Raw JSON payloads are stored in an append-only Delta Lake format alongside essential extraction metadata, such as ingestion timestamps.

In the Silver layer, the transformation engine parses nested JSON structures into relational keys, extracting critical reference fields like patient identifiers. It tracks historical change data using SHA-256 row hashing to implement Slowly Changing Dimensions (SCD Type 2) with valid start dates, end dates, and current record flags. Additionally, the Silver layer leverages Delta Lake schema auto-merging to seamlessly accommodate schema evolution and missing payload fields without breaking execution jobs.

In the Gold layer, Spark SQL views curate the transformed dataset into an analytical star schema optimized for SQL querying and Power BI reporting. This model consists of active patient demographics in gold_dim_patient, clinical conditions in gold_dim_condition, and a centralized fact table in gold_fact_encounter that integrates encounters, clinical observations, and patient context.

The entire workflow is orchestrated using Microsoft Fabric Data Pipelines via pl_fhir_ingestion_medallion.json, enforcing strict linear dependency checks across all three stages. Downstream notebooks execute only after prior stages complete successfully, supported by automated failure recovery retries and centralized monitoring for complete pipeline auditability. Detailed architectural designs, implementation code, and pipeline configurations are organized across the docs/, notebooks/, and pipelines/ subdirectories.
