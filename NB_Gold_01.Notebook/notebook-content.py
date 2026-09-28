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

# CELL ********************

import pyspark.sql.functions as F

def generate_dim_time_step(start_date="2025-01-01", total_steps=744):
    # 1. Generate sequence for every HOUR (step)
    df = spark.range(1, total_steps + 1).withColumnRenamed("id", "step")
    
    # 2. Calculate Timestamp dynamically (Add (step - 1) hours as seconds)
    # 3600 seconds = 1 hour
    df = df.withColumn(
    "timestamp", 
    F.to_timestamp(F.lit(start_date)) + (F.col("step") - 1) * F.expr("INTERVAL 1 HOUR")
)
    
    # 3. Extract attributes and generate Surrogate Key
    dim_date = df.select(
        # xxhash64 Surrogate Key matching fact_transactions
        F.xxhash64("step").alias("step_sk"),  
        
        # Natural Key
        F.col("step").cast("int"),
        
        # Calendar Attributes
        F.to_date("timestamp").alias("calendar_date"),
        F.date_format("timestamp", "EEEE").alias("day_name"),
        F.dayofweek("timestamp").cast("bigint").alias("day_of_week"),
        F.hour("timestamp").alias("hour_of_day"),
        
        # Temporal Risk Logic
        F.when((F.hour("timestamp") >= 0) & (F.hour("timestamp") <= 6), "High Risk")
         .when((F.hour("timestamp") >= 22), "High Risk")
         .otherwise("Low Risk").alias("temporal_risk_bucket")
    )
    
    return dim_date
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
# 4. Generate the Dimension
dim_date = generate_dim_time_step()

# 5. Save to Gold
dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_date")

print("Time Dimension Table successfully generated with step_sk.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# 1. Pull from Silver table
df_silver = spark.table("silver.silver_transactions")

# 2. Enrich with Temporal and Base Metadata
df_silver_enriched = (
    df_silver
    .withColumn("day_of_week", F.floor((F.col("step") - 1) / 24) % 7 + 1)
    .withColumn("start_date", F.current_timestamp())
    # Generate Primary Key 
    .withColumn(
        "transaction_sk",
        F.xxhash64("nameOrig_hashed","step","amount","type")
            
        
    )
    # Generate Dimension Surrogate Keys
    .withColumn("txn_type_sk", F.xxhash64("type"))
    .withColumn("step_sk", F.xxhash64("step"))
    .withColumn(
        "account_sk",
        F.xxhash64("nameOrig_hashed","start_date")
            
        
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# 1. Define Integrity Constraints
integrity_filter = (
    F.col("transaction_sk").isNotNull() & 
    (F.col("amount") > 0) & 
    F.col("nameOrig_hashed").isNotNull()
)

# 2. Separate Gold Clean Data from Quarantine Data
df_silver_filtered = df_silver_enriched.filter(integrity_filter)
df_quarantine = df_silver_enriched.filter(~integrity_filter)

# 3. Add Quarantine Reason
df_quarantine = df_quarantine.withColumn(
    "quarantine_reason", 
    F.when(F.col("transaction_sk").isNull(), "Missing Surrogate Key")
     .when(F.col("amount") <= 0, "Negative or Zero Amount")
     .when(F.col("nameOrig_hashed").isNull(), "Missing Originator Identity")
     .otherwise("Other Integrity Failure")
)
df_quarantine = df_quarantine.withColumn(
    "transaction_sk", F.col("transaction_sk").cast("string")
)
# 5. Apply Logical Financial Flags & Forensic Audit Notes
df_silver_audited = (
    df_silver_filtered
    .withColumn(
    "transaction_sk", F.col("transaction_sk").cast("string")
     )
    .withColumn(
        "has_logical_error",
        F.when(
            ((F.col("type") == "TRANSFER") & (F.col("newbalanceOrig") > F.col("oldbalanceOrg"))) |
            ((F.col("step") < 1) | (F.col("step") > 1000)),
            1
        ).otherwise(0)
    )
    .withColumn(
        "audit_note",
        F.concat_ws(
            " | ", 
            F.when((F.col("type") == "TRANSFER") & (F.col("newbalanceOrig") > F.col("oldbalanceOrg")), 
                   "WARN: Balance increased during Transfer"),
            F.when((F.col("step") < 1) | (F.col("step") > 1000), 
                   "WARN: Step value outside simulation bounds")
        )
    )
    
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# 1. Apply Sanity Filters (Z-score and Risk Score bounds)
df_gold_sanitized = df_silver_audited.withColumn(
    "is_mathematically_sane",
    F.when(
        (F.abs(F.col("z_score")) <= 50) & F.col("z_score").isNotNull(), 
        1
    ).otherwise(0)
).withColumn(
    "risk_score_out_of_bounds",
    F.when(
        (F.col("risk_score") < 0) | (F.col("risk_score") > 100), 
        1
    ).otherwise(0)
)

# 2. Separate Pure Gold Data from Mathematical Noise
df_gold_clean = df_gold_sanitized.filter(F.col("is_mathematically_sane") == 1)
df_math_errors = df_gold_sanitized.filter(F.col("is_mathematically_sane") == 0)

# 3. Audit Math Errors for Engineering Review
df_math_errors.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteschema", "true") \
    .saveAsTable("gold.audit_math_failures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# 1. Dimension: Account (SCD Type 2 Layout)
dim_account = (
    df_gold_clean.select(
        "account_sk",
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status",
        "start_date"
    )
    .distinct()
    .withColumn("is_current", F.lit(True))
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .select(
        "account_sk",
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status",
        "is_current",
        "start_date",
        "end_date"
    )
)

# 2. Dimension: Transaction Type
dim_txn_type = (
    df_gold_clean.select(
        "txn_type_sk",
        "type_risk_level", 
        "txn_category_risk", 
        "fund_direction"
    )
    .distinct()
    .select(
        "txn_type_sk",
        "type_risk_level",
        "txn_category_risk",
        "fund_direction"
    )
)



# 4. Fact: Transactions
fact_transactions = df_gold_clean.select(
    # Keys
    "transaction_sk",
    "nameOrig_hashed", 
    "account_sk", 
    "txn_type_sk",
    "step_sk",
    
    # Financial Measures
    "amount", 
    "oldbalanceOrg", 
    "newbalanceOrig", 
    "risk_score",
    "day_of_week",
    "balance_drain_ratio",
    "z_score",
    
    # Audit & Behavioral Flags
    "isFraud", 
    "is_behavioral_anomaly", 
    "is_first_action", 
    "is_round_number",
    "is_drain_event",
    "has_logical_error",
    "audit_note"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Create Gold Database
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# 2. Save Dimensions
dim_account.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_account")

dim_txn_type.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_transaction_type")

dim_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_date")

# 3. Save Fact Table Partitioned by Day
fact_transactions.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("day_of_week") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.fact_transactions")

# 4. Run Table Optimizations
spark.sql("OPTIMIZE gold.fact_transactions ZORDER BY (nameOrig_hashed, transaction_sk)")
spark.sql("OPTIMIZE gold.dim_account ZORDER BY (nameOrig_hashed)")
spark.sql("VACUUM gold.fact_transactions RETAIN 168 HOURS")

print("Gold Layer Star Schema successfully deployed and optimized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Access the Gold Fact Table
factTable = DeltaTable.forName(spark, "gold.fact_transactions")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") 
# Execute Delta MERGE with Partition Pruning
(
    factTable.alias("target")
    .merge(
        source=df_silver_enriched.alias("updates"),
        # Condition includes partition key (day_of_week) for max query pruning speed
        condition="target.transaction_sk = updates.transaction_sk AND target.day_of_week = updates.day_of_week"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if any duplicate active records currently exist
has_duplicates = (
    spark.sql("""
        SELECT 1 
        FROM gold.dim_account 
        WHERE is_current = true 
        GROUP BY nameOrig_hashed 
        HAVING COUNT(*) > 1 
        LIMIT 1
    """).count() > 0
)

# Run cleanup ONLY if corruption is detected
if has_duplicates:
    print("⚠️ Duplicate active records detected in target table. Running cleanup...")
    spark.sql("""
        MERGE INTO gold.dim_account AS target
        USING (
            SELECT DISTINCT account_sk
            FROM (
                SELECT account_sk,
                       ROW_NUMBER() OVER (
                           PARTITION BY nameOrig_hashed 
                           ORDER BY start_date DESC, account_sk DESC
                       ) as rn
                FROM gold.dim_account
                WHERE is_current = true
            )
            WHERE rn > 1
        ) AS duplicates
        ON target.account_sk = duplicates.account_sk
        WHEN MATCHED THEN
          UPDATE SET 
            target.is_current = false, 
            target.end_date = current_timestamp()
    """)
    print("✅ Target table successfully cleaned.")
else:
    print("✅ Target table healthy (no duplicate active records). Skipping cleanup.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Initialize Delta Table Reference
targetTable = DeltaTable.forName(spark, "gold.dim_account")

# 2. Strict Deduplication of incoming Silver updates
# Enforce tie-breaker ordering on step and monotonic row ID to guarantee EXACTLY 1 row per account
windowSpec = Window.partitionBy("nameOrig_hashed").orderBy(
    F.col("step").desc_nulls_last(), 
    F.monotonically_increasing_id().desc()
)

new_account_states = (
    df_silver
    .filter(F.col("nameOrig_hashed").isNotNull())
    .withColumn("row_num", F.row_number().over(windowSpec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select(
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status"
    )
)

# 3. Detect attribute changes & isolated new accounts against current target rows
staged_updates = (
    new_account_states.alias("updates")
    .join(
        targetTable.toDF().filter("is_current = true").alias("target"),
        on="nameOrig_hashed",
        how="left"
    )
    .select(
        F.col("updates.nameOrig_hashed").alias("nameOrig_hashed"),
        F.col("updates.customer_segment").alias("customer_segment"),
        F.col("updates.account_activity").alias("account_activity"),
        F.col("updates.account_audit_status").alias("account_audit_status"),
        F.col("target.nameOrig_hashed").isNotNull().alias("is_existing_account"),
        # Null-safe attribute change check
        F.when(
            (F.col("target.nameOrig_hashed").isNotNull()) & (
                (~F.col("updates.customer_segment").eqNullSafe(F.col("target.customer_segment"))) |
                (~F.col("updates.account_activity").eqNullSafe(F.col("target.account_activity"))) |
                (~F.col("updates.account_audit_status").eqNullSafe(F.col("target.account_audit_status")))
            ),
            True
        ).otherwise(False).alias("attr_changed")
    )
).cache() # Cache to optimize two downstream ops

# 4. STEP 1: EXPIRE OLD RECORDS (Merge)
# Clean 1-to-1 match: Exactly one source row per target row
records_to_expire = staged_updates.filter("attr_changed = true")

(
    targetTable.alias("target")
    .merge(
        source=records_to_expire.alias("updates"),
        condition="target.nameOrig_hashed = updates.nameOrig_hashed AND target.is_current = true"
    )
    .whenMatchedUpdate(
        set={
            "is_current": "false",
            "end_date": "current_timestamp()"
        }
    )
    .execute()
)

# 5. STEP 2: INSERT NEW & UPDATED VERSIONS (Append)
# Get brand new accounts OR new version of changed accounts
new_records_to_insert = (
    staged_updates
    .filter("(attr_changed = true) OR (is_existing_account = false)")
    .select(
        F.xxhash64(F.col("nameOrig_hashed"), F.current_timestamp().cast("string")).alias("account_sk"),
        F.col("nameOrig_hashed"),
        F.col("customer_segment"),
        F.col("account_activity"),
        F.col("account_audit_status"),
        F.lit(True).alias("is_current"),
        F.current_timestamp().alias("start_date"),
        F.lit(None).cast("timestamp").alias("end_date")
    )
)

# Append new active rows directly to Delta Lake table
(
    new_records_to_insert
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("gold.dim_account")
)

# Unpersist cache
staged_updates.unpersist()

print("SCD Type 2 processing completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
import pyspark.sql.functions as F

# 1. Ensure Table Exists with correct schema using SQL DDL
spark.sql("""
CREATE TABLE IF NOT EXISTS gold.dim_transaction_type (
    txn_type_sk BIGINT,
    type_risk_level STRING,
    txn_category_risk STRING,
    fund_direction STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")
# 2. Extract distinct transaction type combinations from Silver
new_txn_types = (
    df_silver.select(
        "type_risk_level", 
        "txn_category_risk", 
        "fund_direction"
    )
    .distinct()
    # Compute the deterministic xxhash64 Surrogate Key & append current timestamp upfront
    .withColumn(
        "txn_type_sk",
        F.xxhash64("type_risk_level","txn_category_risk","fund_direction")
            
        
    )
    .withColumn("last_updated", F.current_timestamp())
)

# 3. Access Target Gold Delta Table
targetTable = DeltaTable.forName(spark, "gold.dim_transaction_type")
# 4. Merge: Insert brand new transaction types (SCD Type 0 / Insert-Only)
(
    targetTable.alias("target")
    .merge(
        source=new_txn_types.alias("source"),
        condition="target.txn_type_sk = source.txn_type_sk"
    )
    .whenNotMatchedInsert(
        values={
            "txn_type_sk": F.col("source.txn_type_sk"),
            "type_risk_level": F.col("source.type_risk_level"),
            "txn_category_risk": F.col("source.txn_category_risk"),
            "fund_direction": F.col("source.fund_direction"),
            "last_updated": F.col("source.last_updated")
        }
    )
    .execute()
)

print("gold.dim_transaction_type updated successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_daily_fraud_summary = spark.sql("""
    SELECT 
        d.day_of_week, 
        t.type_risk_level, 
        SUM(f.amount) AS total_amount,
        COUNT(CASE WHEN f.isFraud = 1 THEN 1 END) AS fraud_count
    FROM gold.fact_transactions f
    JOIN gold.dim_date d 
        ON f.step_sk = d.step_sk
    JOIN gold.dim_transaction_type t 
        ON f.txn_type_sk = t.txn_type_sk
    GROUP BY d.day_of_week, t.type_risk_level
""")

df_daily_fraud_summary.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.agg_daily_fraud")

print("gold.agg_daily_fraud summary table successfully refreshed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# -------------------------------------------------------------------------
# 1. Feature Selection & Binary Encoding
# -------------------------------------------------------------------------
ml_feature_table = (
    df_silver_filtered.select(
        "transaction_sk",
        "amount",
        "z_score",
        "behavioral_spike_ratio",
        "txns_last_24h",
        "balance_drain_ratio",
        "avg_last_10_txns",
        "is_first_action",
        "type_risk_level", 
        "isFraud"
    )
    .withColumn(
        "is_high_risk_type", F.when(F.col("type_risk_level") == "High", 1).otherwise(0)
    )
    .drop("type_risk_level")
)

# -------------------------------------------------------------------------
# 2. Null Imputation (Default Baseline Values)
# -------------------------------------------------------------------------
ml_feature_table = ml_feature_table.fillna({
    "z_score": 0.0,
    "behavioral_spike_ratio": 1.0,
    "txns_last_24h": 0,
    "balance_drain_ratio": 0.0,
    "avg_last_10_txns": 0.0
})

# -------------------------------------------------------------------------
# 3. Save to Gold Feature Store
# -------------------------------------------------------------------------
(
    ml_feature_table.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fs_account_features")
)

# Enable Change Data Feed via TBLPROPERTIES (Correct Delta Syntax)
spark.sql("""
    ALTER TABLE gold.fs_account_features 
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("gold.fs_account_features created successfully with CDF enabled.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. LOAD DATA
df_silver = spark.read.table("silver.silver_transactions")
fact_df = spark.read.table("gold.fact_transactions")
dim_acc_df = spark.read.table("gold.dim_account")

# Try loading quarantine table if it exists
quarantine_count = 0
if spark.catalog.tableExists("gold.quarantine_transactions"):
    quarantine_count = spark.read.table("gold.quarantine_transactions").count()

# 2. OPTIMIZED AGGREGATIONS (Single pass per table)
silver_stats = df_silver.agg(
    F.count("*").alias("cnt"), 
    F.coalesce(F.sum("amount"), F.lit(0.0)).alias("sum_amt")
).first()

gold_stats = fact_df.agg(
    F.count("*").alias("cnt"), 
    F.coalesce(F.sum("amount"), F.lit(0.0)).alias("sum_amt")
).first()

silver_count, silver_sum = silver_stats["cnt"], float(silver_stats["sum_amt"])
fact_count, gold_sum = gold_stats["cnt"], float(gold_stats["sum_amt"])

# 3. REFERENTIAL INTEGRITY (Check for orphan transactions in Fact)
orphans = fact_df.join(dim_acc_df, "nameOrig_hashed", "left_anti").count()

# 4. RECONCILIATION CALCULATIONS
amount_diff = __builtins__.abs(silver_sum - gold_sum)
expected_missing = silver_count - fact_count

# 5. PRINT QUALITY REPORT
print("--- GOLD LAYER QUALITY REPORT ---")
print(f"Silver Source Rows: {silver_count}")
print(f"Fact Target Rows  : {fact_count} (Quarantined/Filtered: {expected_missing})")
print(f"Referential Integrity Check: {'PASS' if orphans == 0 else 'FAIL'} ({orphans} orphans)")
print(f"Financial Reconciliation   : Diff = ${amount_diff:,.2f}")

# 6. INVESTIGATE FILTERED ROWS
if expected_missing > 0:
    print(f"\n[INVESTIGATION] Isolated {expected_missing} rows between Silver and Fact:")
    missing_df = df_silver.join(fact_df, "transaction_sk", "left_anti")
    missing_df.select("transaction_sk", "amount", "step").show(16, truncate=False)

# 7. SAFETY STOP (Fails only on orphaned keys or negative row drops)
if orphans > 0 or fact_count > silver_count:
    print("\n!!! PIPELINE HALTED: Critical integrity issues detected.")
    raise Exception("Data Integrity Failure: Orphans detected or Fact row count exceeds Silver.")
else:
    print("\nSUCCESS: Gold Layer audit passed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
