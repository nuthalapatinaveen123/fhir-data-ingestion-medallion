# FHIR Data Ingestion & Analytics Solution (Medallion Architecture)

## Architecture Overview
This project implements a production-grade **Medallion Architecture** (Bronze, Silver, Gold) on **Microsoft Fabric** to ingest, process, and model healthcare data following the **FHIR (Fast Healthcare Interoperability Resources)** standard.

[ Raw FHIR APIs ]
       │
       ▼
┌───────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                         │
│  ➔ Ingestion & Raw JSON Storage with Backoff Retry        │
└─────────────────────────────┬─────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────┐
│                      SILVER LAYER                         │
│  ➔ SCD Type 2 History & Schema Evolution (Delta Lake)     │
└─────────────────────────────┬─────────────────────────────┘
                              │
                              ▼
┌───────────────────────────────────────────────────────────┐
│                       GOLD LAYER                          │
│  ➔ Analytical Star Schema Views (SQL / Power BI Ready)    │
└───────────────────────────────────────────────────────────┘

## Technical Features & Implementation

### 1. Bronze Layer — Raw Ingestion
* **Notebook:** `notebooks/01_bronze_ingestion.py`
* Ingests core FHIR resources: `Patient`, `Encounter`, `Condition`, `Observation`.
* Implements exponential backoff to handle rate limits and transient network failures.
* Saves data directly into Delta format with ingestion metadata (`extraction_timestamp`).

### 2. Silver Layer — Cleanse & SCD Type 2 Engine
* **Notebook:** `notebooks/02_silver_transformation.py`
* **JSON Reference Handling:** Parses nested FHIR reference fields (e.g., `subject.reference`) dynamically into standard relational keys (`patient_id_ref`).
* **SCD Type 2 Change Data Capture:** Tracks historical record changes using SHA-256 row hashing (`row_hash`), setting `valid_from`, `valid_to`, and `is_current` boolean flags.
* **Schema Evolution:** Utilizes `spark.databricks.delta.schema.autoMerge.enabled` to seamlessly handle dynamic or missing fields across raw API payloads without job failures.

### 3. Gold Layer — Analytical Star Schema
* **Notebook:** `notebooks/03_gold_analytics.sql`
* Constructs business-ready SQL views optimized for Power BI and analytical queries:
  * `gold_dim_patient`: Active patient dimension table.
  * `gold_dim_condition`: Patient clinical conditions.
  * `gold_fact_encounter`: Core fact table linking patient encounters, clinical observations, and conditions.

---

## Orchestration Pipeline

The workflow is fully orchestrated in Microsoft Fabric via Data Pipeline **`pl_fhir_ingestion_medallion`**:

$$\text{01\_bronze\_ingestion} \xrightarrow{\text{On Success}} \text{02\_silver\_transformation} \xrightarrow{\text{On Success}} \text{03\_gold\_analytics}$$

---

## Deployment & Running Instructions

**Environment Setup:**
Import notebooks into a Microsoft Fabric Workspace connected to a Fabric Lakehouse (`fhir_lakehouse`).

**Execute Ingestion:** 
Run `01_bronze_ingestion.py` to pull raw JSON into Bronze Delta tables.

**Run Transformations:**
Execute `02_silver_transformation.py` to parse schemas and maintain SCD Type 2 history.

**Build Gold Layer:** 
Execute `03_gold_analytics.sql` to generate analytical views.

**Pipeline Automation:**
Trigger `pl_fhir_ingestion_medallion` to run all stages sequentially.
