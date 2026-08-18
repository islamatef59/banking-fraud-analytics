# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "259b2f49-bfe7-4cd2-830c-eec555762d22",
# META       "default_lakehouse_name": "lh_banking_transactions",
# META       "default_lakehouse_workspace_id": "614d5d24-d74c-4eed-837f-98a82699d6dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "259b2f49-bfe7-4cd2-830c-eec555762d22"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Read data from file

# PARAMETERS CELL ********************


job_run_id = ""
file_name=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# --- STEP 2: REST OF YOUR CODE ---
from notebookutils import mssparkutils
from pyspark.sql.functions import current_timestamp, input_file_name, lit, col

# Use the variables directly (no .get() needed)
source_folder ="Files/landing"
table_name =  "bronze.bronze_transactions"
print(f"Loading data into table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pathlib import Path
from notebookutils import mssparkutils
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit

# --- CONFIGURATION & SETUP ---
source_folder = "Files/landing"
threshold_percentage = 0.10

# Dynamic parameters with fallback defaults
#table_name = (
 #   p_table_name
  #  if ("p_table_name" in locals() and p_table_name and p_table_name.strip() != "")
  #  else "bronze_transactions"
#)
run_id = job_run_id if "job_run_id" in locals() else "local_run"

# Construct source file path safely using Path
source_file_path = str(Path(source_folder) / file_name)

# --- STEP 1: FILE EXISTENCE CHECK ---
if not any(f.name == file_name for f in mssparkutils.fs.ls(source_folder)):
  raise FileNotFoundError(
      f"CRITICAL: Required landed file '{file_name}' not found in"
      f" '{source_folder}'."
  )

# --- STEP 2: INGEST LANDED FILE ---
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(source_file_path)
)

# --- STEP 3: DATA QUALITY CHECKS ---
total_count = df_raw.count()

if total_count == 0:
  raise Exception(f"DATA QUALITY FAILURE: {file_name} is empty. Load aborted.")

null_amount_count = df_raw.filter(
    col("amount").isNull() | col("type").isNull()
).count()
null_ratio = null_amount_count / total_count

if null_ratio > threshold_percentage:
  raise Exception(
      f"DATA QUALITY FAILURE: {null_ratio*100:.2f}% nulls found in critical"
      " columns ('amount'/'type'). Load aborted."
  )
else:
     print(
          f"Data Quality Passed: {null_ratio*100:.2f}% nulls detected across"
          f" {total_count} rows."
          )

# --- STEP 4: METADATA ENRICHMENT ---
df_bronze = (
    df_raw.withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("status", lit("New"))
    .withColumn("is_pii_flag", lit(True))
    .withColumn("job_run_id", lit(run_id))
)

# --- STEP 5: WRITE TO BRONZE DELTA TABLE ---
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
(
    df_bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("type")
    .saveAsTable(table_name)
)

print(f"Table '{table_name}' successfully written.")

# --- STEP 6: DATA GOVERNANCE & COMMENTS ---
spark.sql(
    f"ALTER TABLE {table_name} ALTER COLUMN nameOrig COMMENT 'PII: Customer"
    " Name'"
)
spark.sql(
    f"ALTER TABLE {table_name} ALTER COLUMN nameDest COMMENT 'PII: Recipient"
    " Name'"
)

# --- STEP 7: OPTIMIZE & VACUUM ---
print("Starting Table Compaction & Vacuum...")
spark.sql(f"OPTIMIZE {table_name}")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")

print(
    "Ingestion, cataloging, and table maintenance completed successfully."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
