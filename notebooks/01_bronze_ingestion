import os
import json
import logging
import time
import uuid
from datetime import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# ==========================================
# 1. LOGGING & PIPELINE CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("FHIR_Bronze_Ingestion")

BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCES = ["Patient", "Encounter", "Condition", "Observation"]
# In Microsoft Fabric, files in the Lakehouse live under /lakehouse/default/Files/
RAW_STORAGE_BASE = "/lakehouse/default/Files/raw"
MAX_PAGES = 3
PAGE_SIZE = 50
BATCH_ID = str(uuid.uuid4())

logger.info(f"Starting Bronze Ingestion Execution | Batch ID: {BATCH_ID}")

# ==========================================
# 2. RESILIENT API CLIENT WITH EXPONENTIAL BACKOFF
# ==========================================
def fetch_fhir_page(url: str, retries: int = 3, backoff_factor: int = 2) -> dict:
    """Fetches FHIR payload with backoff logic to handle API limits smoothly."""
    headers = {"Accept": "application/fhir+json"}
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                return response.json()
            logger.warning(f"HTTP {response.status_code} on {url}. Retry {attempt}/{retries}")
        except requests.RequestException as e:
            logger.warning(f"Request failed: {str(e)}. Retry {attempt}/{retries}")
        
        time.sleep(backoff_factor ** attempt)
    
    raise RuntimeError(f"Failed to fetch data from {url} after {retries} attempts.")

# ==========================================
# 3. PAGINATED INGESTION ENGINE
# ==========================================
def ingest_fhir_resource(resource_type: str) -> int:
    """
    Handles pagination, raw JSON file dumps, and Delta Table loading for FHIR resources.
    """
    url = f"{BASE_URL}/{resource_type}?_count={PAGE_SIZE}&_sort=-_lastUpdated"
    records = []
    page_count = 0
    now_utc = datetime.utcnow()
    date_path = now_utc.strftime("year=%Y/month=%m/day=%d")
    
    logger.info(f"Processing Resource: {resource_type}")

    while url and page_count < MAX_PAGES:
        logger.info(f"Fetching Page {page_count + 1} for {resource_type}...")
        data = fetch_fhir_page(url)
        entries = data.get("entry", [])
        
        for entry in entries:
            resource = entry.get("resource", {})
            res_id = resource.get("id")
            if res_id:
                records.append({
                    "resource_id": str(res_id),
                    "raw_json": json.dumps(resource),
                    "extraction_timestamp": now_utc.isoformat(),
                    "api_url_or_params": url,
                    "source_system": "HAPI_FHIR_API",
                    "ingestion_batch_id": BATCH_ID
                })

        # Extract next URL from FHIR link header array
        url = next((link["url"] for link in data.get("link", []) if link.get("relation") == "next"), None)
        page_count += 1

    if not records:
        logger.warning(f"No records retrieved for {resource_type}")
        return 0

    # Save to Raw Layer (Immutable JSON Files using Native Python OS module)
    target_dir = f"{RAW_STORAGE_BASE}/{resource_type}/{date_path}"
    os.makedirs(target_dir, exist_ok=True)
    
    raw_file_path = f"{target_dir}/batch_{BATCH_ID}.json"
    with open(raw_file_path, "w", encoding="utf-8") as f:
        json.dump([r["raw_json"] for r in records], f)
        
    logger.info(f"Raw response dumped to: {raw_file_path}")

    # Load to Bronze Layer (Delta Table)
    bronze_schema = StructType([
        StructField("resource_id", StringType(), False),
        StructField("raw_json", StringType(), False),
        StructField("extraction_timestamp", StringType(), False),
        StructField("api_url_or_params", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("ingestion_batch_id", StringType(), False)
    ])

    df_bronze = spark.createDataFrame(records, schema=bronze_schema)
    
    table_name = f"bronze_{resource_type.lower()}"
    df_bronze.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
    
    logger.info(f"Loaded {len(records)} rows into {table_name}")
    return len(records)

# ==========================================
# 4. ORCHESTRATION EXECUTION
# ==========================================
execution_summary = {}

# Execute in strictly defined sequence: Patient -> Encounter -> Condition -> Observation
for resource in RESOURCES:
    total_loaded = ingest_fhir_resource(resource)
    execution_summary[resource] = total_loaded

logger.info("=== BRONZE INGESTION COMPLETED ===")
for res, count in execution_summary.items():
    logger.info(f"Table bronze_{res.lower()}: {count} records added.")
