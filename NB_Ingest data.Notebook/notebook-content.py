# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b80005f4-c931-463b-bece-ff96548bb5b4",
# META       "default_lakehouse_name": "lake_project",
# META       "default_lakehouse_workspace_id": "55bbbc6a-b5da-41c6-9edf-afd398ef7970",
# META       "known_lakehouses": [
# META         {
# META           "id": "b80005f4-c931-463b-bece-ff96548bb5b4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Read data from file

# CELL ********************



from notebookutils import mssparkutils

# Create the widgets (called 'parameters' in Fabric)
mssparkutils.widgets.text("p_source_path", "", "Source Path")
mssparkutils.widgets.text("p_table_name", "", "Table Name")

# To retrieve the values later:
source_path = mssparkutils.widgets.get("p_source_path")
table_name = mssparkutils.widgets.get("p_table_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, input_file_name, lit,col

threshold_percentage = 0.10
source_folder = "Files/"
archive_folder = "Files/Archive/"
file_name = "Banking_dataset.csv"
run_id = mssparkutils.widgets.get("job_run_id") if "mssparkutils" in globals() else "local_run"
if not any(f.name == file_name for f in mssparkutils.fs.ls(source_folder)):
    print(f"ERROR: {file_name} not found in {source_folder}. Did it move to Archive?")
else:   
    df_raw=spark.read\
    .format("csv")\
    .option("header",True)\
    .option("mergeSchema", "True")\
    .option("mode", "PERMISSIVE") \
    .load(source_path)

    # --- STEP 1: DATA QUALITY CHECK (The Sanity Check) ---
    total_count = df_raw.count()
    # Count nulls in a critical column, e.g., 'amount'
    null_amount_count = df_raw.filter(col("amount").isNull()).count()
    null_ratio = null_amount_count / total_count

    if null_ratio > threshold_percentage:
        raise Exception(f"DATA QUALITY FAILURE: {null_ratio*100}% of 'amount' is NULL. Load aborted.")
    else:
      print(f"Data Quality Passed: Only {null_ratio*100:.2f}% nulls detected.")
 
    df_bronze=df_raw.withColumn("ingestion_timestamp",current_timestamp())\
                    .withColumn("source_file",input_file_name())\
                    .withColumn("status",lit("New"))\
                    .withColumn("is_pii_flag", lit(True))\
                    .withColumn("job_run_id", lit(run_id))
    df_bronze.write.format("delta")\
                .mode("overwrite")\
                .option("overwriteSchema",True)\
                .partitionBy("type")\
                .saveAsTable(table_name) 
    spark.sql(f"DESCRIBE DETAIL {table_name}").select("properties").show(truncate=False)
    # --- STEP 3: APPLY SQL COMMENTS FOR PII TAGGING ---
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN nameOrig COMMENT 'PII: Customer Name'")
    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN nameDest COMMENT 'PII: Recipient Name'")
    print(f"Table {table_name} successfully loaded and tagged.")

    # 1. Create Archive folder if it doesn't exist
    if not any(f.name == "Archive/" for f in mssparkutils.fs.ls(source_folder)):
        mssparkutils.fs.mkdirs(archive_folder)

    # 2. Move the file from Landing to Archive
    try:
        mssparkutils.fs.mv(f"{source_folder}{file_name}", f"{archive_folder}{file_name}")
        print(f"Successfully moved {file_name} to Archive.")
    except Exception as e:
        print(f"File move failed: {e}")

    # 3. Optimize the table (Compaction)
    # This reorganizes the data for much faster reading in Silver/Gold layers
    print("Starting Table Optimization...")
    spark.sql(f"OPTIMIZE {table_name}")

    # 4. Vacuum the table (Cleanup)
    # This removes data files that are older than 7 days (default) 
    # and are no longer referenced by the Delta log.
    # Note: Use with caution if you need to "Time Travel" back very far.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS") 

    print("Compaction and Vacuum complete.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
