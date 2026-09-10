import logging
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, lit, expr, 
    sha2, element_at, split
)

# Enable Spark & Delta automatic schema evolution for merges
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FHIR_Silver_Transformation")

RESOURCES = ["Patient", "Encounter", "Condition", "Observation"]

def process_silver_scd2(resource_type: str, business_key: str = "resource_id"):
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

    sample_payload = df_bronze.select("raw_json").first()[0]
    payload_schema = schema_of_json(sample_payload)

    # Parse JSON
    parsed_df = df_bronze.withColumn("data", from_json(col("raw_json"), payload_schema)) \
        .select(
            col("resource_id"),
            col("data.*"),
            col("extraction_timestamp"),
            col("raw_json")
        )

    # Safely extract patient_id link across all resources if available
    if "subject" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("patient_id_ref", element_at(split(col("subject.reference"), "/"), -1))
    else:
        parsed_df = parsed_df.withColumn("patient_id_ref", lit(None).cast("string"))

    parsed_df = parsed_df \
        .withColumn("row_hash", sha2(col("raw_json"), 256)) \
        .withColumn("valid_from", col("extraction_timestamp")) \
        .withColumn("valid_to", lit(None).cast("string")) \
        .withColumn("is_current", lit(True)) \
        .dropDuplicates([business_key, "row_hash"])

    # CASE 1: Table does not exist -> Initial Write
    if not spark.catalog.tableExists(silver_table):
        logger.info(f"Creating initial table {silver_table}...")
        parsed_df.drop("raw_json").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        return

    # CASE 2: Incremental Load -> Schema-Evolving Delta Merge
    target_delta = DeltaTable.forName(spark, silver_table)
    target_df = target_delta.toDF()

    # Detect row changes
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

    staged_data = parsed_df.drop("raw_json").withColumn("join_key", lit(None).cast("string")) \
        .unionByName(staged_updates.drop("raw_json"), allowMissingColumns=True)

    # Execute SCD2 Merge with automatic schema matching
    target_delta.alias("target").merge(
        staged_data.alias("staged"),
        f"target.{business_key} = staged.join_key AND target.is_current = true"
    ).whenMatchedUpdate(
        set={"is_current": "false", "valid_to": "staged.valid_from"}
    ).whenNotMatchedInsertAll().execute()

    logger.info(f"SCD Type 2 processing complete for {silver_table}")

# Run process across all target resources
for resource in RESOURCES:
    process_silver_scd2(resource)

logger.info("=== SILVER TRANSFORMATIONS COMPLETED ===")
