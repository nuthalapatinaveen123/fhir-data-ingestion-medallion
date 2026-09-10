import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, lit, expr, 
    current_timestamp, coalesce, sha2, concat_ws
)

# ==========================================
# 1. LOGGING & PIPELINE CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("FHIR_Silver_Transformation")

RESOURCES = ["Patient", "Encounter", "Condition", "Observation"]

logger.info("Starting Silver Layer SCD Type 2 Processing...")

# ==========================================
# 2. SCD TYPE 2 MERGE ENGINE
# ==========================================
def process_silver_scd2(resource_type: str, business_key: str = "resource_id"):
    """
    Parses Bronze JSON payloads, computes row hash for change detection, 
    and applies SCD Type 2 merge logic into Silver Delta tables.
    """
    bronze_table = f"bronze_{resource_type.lower()}"
    silver_table = f"silver_{resource_type.lower()}"
    
    logger.info(f"--- Processing Silver Table: {silver_table} ---")

    if not spark.catalog.tableExists(bronze_table):
        logger.warning(f"Bronze table {bronze_table} does not exist. Skipping.")
        return

    df_bronze = spark.read.table(bronze_table)
    
    if df_bronze.rdd.isEmpty():
        logger.warning(f"Bronze table {bronze_table} is empty. Skipping.")
        return

    # Infer dynamic JSON schema from raw payloads
    sample_payload = df_bronze.select("raw_json").first()[0]
    payload_schema = schema_of_json(sample_payload)

    # Parse JSON and compute hash digest for tracking changes
    parsed_df = df_bronze.withColumn("data", from_json(col("raw_json"), payload_schema)) \
        .select(
            col("resource_id"),
            col("data.*"),
            col("extraction_timestamp"),
            col("raw_json")
        ) \
        .withColumn("row_hash", sha2(col("raw_json"), 256)) \
        .withColumn("valid_from", col("extraction_timestamp")) \
        .withColumn("valid_to", lit(None).cast("string")) \
        .withColumn("is_current", lit(True))

    # Deduplicate within batch (keep latest extraction per resource)
    parsed_df = parsed_df.dropDuplicates([business_key, "row_hash"])

    # ------------------------------------------
    # CASE 1: Initial Load (Table does not exist)
    # ------------------------------------------
    if not spark.catalog.tableExists(silver_table):
        logger.info(f"Target table {silver_table} does not exist. Performing initial load...")
        parsed_df.drop("raw_json").write.format("delta").saveAsTable(silver_table)
        logger.info(f"Initial load for {silver_table} completed successfully.")
        return

    # ------------------------------------------
    # CASE 2: Incremental Load with SCD Type 2 Merge
    # ------------------------------------------
    target_delta = DeltaTable.forName(spark, silver_table)
    target_df = target_delta.toDF()

    # Identify records that changed or are new
    staged_updates = parsed_df.alias("updates").join(
        target_df.alias("target"),
        on=(col(f"updates.{business_key}") == col(f"target.{business_key}")) & col("target.is_current"),
        how="inner"
    ).filter(
        col("updates.row_hash") != col("target.row_hash")
    ).select(
        col(f"updates.{business_key}").alias("join_key"),
        col("updates.*")
    )

    # Union updates with incoming records (null join_key ensures insertion of new version)
    staged_data = parsed_df.drop("raw_json").withColumn("join_key", lit(None).cast("string")) \
        .unionByName(staged_updates.drop("raw_json"))

    # Execute Delta Merge Logic
    target_delta.alias("target").merge(
        staged_data.alias("staged"),
        f"target.{business_key} = staged.join_key AND target.is_current = true"
    ).whenMatchedUpdate(
        set={
            "is_current": "false",
            "valid_to": "staged.valid_from"
        }
    ).whenNotMatchedInsert(
        values={
            c: f"staged.{c}" for c in parsed_df.drop("raw_json").columns
        }
    ).execute()

    logger.info(f"SCD Type 2 processing complete for {silver_table}")

# ==========================================
# 3. EXECUTION FOR ALL RESOURCES
# ==========================================
for resource in RESOURCES:
    process_silver_scd2(resource)

logger.info("=== SILVER TRANSFORMATIONS COMPLETED ===")
