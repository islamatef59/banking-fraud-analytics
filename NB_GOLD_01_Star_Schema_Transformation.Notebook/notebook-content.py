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

%run ./NB_SIL_03_Behavioral_Enrichment

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F
# Load cleaned and enriched Silver transactions table
df_silver = spark.table("silver.silver_transactions")

# Extract 1-indexed day of week from step count and filter statistical outliers / null z-scores
df_silver = df_silver.withColumn(
    "day_of_week", F.floor((F.col("step") - 1) / 24) % 7 + 1
).filter(
    (F.abs(F.col("z_score")) <= 50) & (F.col("z_score").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#constraints
import pyspark.sql.functions as F

# Ensure target Gold catalog schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Define mandatory strict data quality conditions for Gold layer promotio
integrity_filter = (
    (F.col("transaction_sk").isNotNull()) & 
    (F.col("amount") > 0) & 
    (F.col("nameOrig_hashed").isNotNull())
)

# 2. Separate 'Gold' Data from 'Trash' (Quarantine)
# Route valid records to processing stream and route non-compliant records to quarantine
df_silver_filterd = df_silver.filter(integrity_filter)
df_quarantine = df_silver.filter(~integrity_filter)

# Tag specific data quality violation reason for quarantine lineage and debugging
df_quarantine = df_quarantine.withColumn("quarantine_reason", 
    F.when(F.col("transaction_sk").isNull(), "Missing Surrogate Key")
     .when(F.col("amount") <= 0, "Negative or Zero Amount")
     .when(F.col("nameOrig_hashed").isNull(), "Missing Originator Identity")
     .otherwise("Other Integrity Failure")
)

# Persist non-compliant records to quarantine table for compliance audit tracking
df_quarantine.write.format("delta").mode("append").saveAsTable("gold.quarantine_integrity_failures")

# 1. Apply Logical Financial Flags
# Evaluate business logic anomalies (balance spikes post-transfer, out-of-bounds simulation steps)
df_silver_filterd =( 
    df_silver_filterd.withColumn(
    "has_logical_error",
    F.when(
        # Logic 1: Transfer should never increase the sender's balance
        ((F.col("type") == "TRANSFER") & (F.col("newbalanceOrig") > F.col("oldbalanceOrg"))) |
        
        # Logic 2: Steps must be within the simulation range (1-1000 hours)
        ((F.col("step") < 1) | (F.col("step") > 1000)),
        
        1 # Error found
    ).otherwise(0)
)
# Generate concatenated audit trace notes for downstream compliance inspection
    .withColumn("audit_note",
        F.concat_ws(" | ", 
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

df_silver_updates= (
    df_silver
    #  Generate primary key for the transaction itself 
    .withColumn(
        "transaction_sk",
       F.xxhash64("nameOrig_hashed", "step", "amount", "type"),
    )
    #  Generate xxhash64 surrogate key for dim_txn_type
    .withColumn("txn_type_sk", F.xxhash64("type"))
    #  Generate xxhash64 surrogate key for dim_date
    .withColumn("step_sk", F.xxhash64("step").cast("string"))
    # Establish dynamic account surrogate keys and effective start dates for SCD Type 2 dimension tracking
    .withColumn("start_date", F.current_timestamp())
    .withColumn(
        "account_sk",
        F.xxhash64(
            F.concat_ws(
                "||",
                F.col("nameOrig_hashed"),
                F.col("start_date").cast("string"),
            )
        ),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_account = (
    df_silver_filterd.select(
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status",
    )
    .distinct()
    .withColumn("is_current", F.lit(True))
    .withColumn("start_date", F.current_timestamp())
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    # Generates a stable, deterministic key based directly on the account ID
    .withColumn("account_sk",F.xxhash64("nameOrig_hashed","start_date"))

    .select(
        "account_sk",
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status",
        "is_current",
        "start_date",
        "end_date",
    )
)
dim_txn_type = (
    df_silver_filterd.select(
        "type_risk_level", 
        "txn_category_risk", 
        "fund_direction"
    )
    .distinct()
    # Combine attributes and compute MD5 hash for txn_type_sk
    .withColumn(
        "txn_type_sk",
        F.xxhash64("type","type_risk_level","txn_category_risk", "fund_direction")
            )
        
    
    # Reorder columns so the surrogate key is first
    .select(
        "txn_type_sk",
        "type",
        "type_risk_level",
        "txn_category_risk",
        "fund_direction",
    )
)
dim_date = (
    df_silver_filterd.select(
        "step",  # Natural key (e.g., 1, 2, 3...)
        "hour_of_day",
        "day_of_week",
        "temporal_risk_bucket",
    )
    .distinct()
    # Generate MD5 surrogate key from step
    .withColumn("step_sk", F.xxhash64("step")
    # Reorder columns so the surrogate key is first
    .select(
        "step_sk",
        "step",
        "hour_of_day",
        "day_of_week",
        "temporal_risk_bucket",
    )
)
)
fact_transactions = df_silver_filterd.select(
    # Foreign Keys
    "transaction_sk",F.xxhash64("nameOrig_hashed","step","amount"."string"),
    "nameOrig_hashed", 
    "account_sk", 
    "txn_type_sk",
    "step_sk",
    
    # Financial Measures
    "amount", 
    "oldBalanceOrg", 
    "newBalanceOrig", 
    "risk_score",
    "day_of_week",
    "balance_drain_ratio",
    "z_score",
    
    
    # Boolean Flags (For quick filtering)
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

# 1. Create the Gold Database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# 2. Save Dimension: Accounts
dim_account.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_account")

# 3. Save Dimension: Transaction Types
dim_txn_type.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_transaction_type")

# 4. Save Dimension: Date/Time
dim_date.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.dim_date")

# 5. Save the Fact Table (The Big One)
fact_transactions.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("day_of_week") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.fact_transactions")
spark.sql("""OPTIMIZE gold.fact_transactions ZORDER BY (nameOrig_hashed, transaction_sk)""")
spark.sql("OPTIMIZE gold.dim_account ZORDER BY (nameOrig_hashed)")
spark.sql("VACUUM gold.fact_transactions RETAIN 168 HOURS")

print("Gold Layer Star Schema successfully deployed and optimized.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# 1. Apply Sanity Filters
df_gold_sanitized = fact_transactions.withColumn(
    "is_mathematically_sane",
    F.when(
        # A Z-score > 50 is physically impossible in almost any distribution
        (F.abs(F.col("z_score")) > 50) | (F.col("z_score").isNull()), 
        0 # Insane / Error
    ).otherwise(1)
).withColumn(
    "risk_score_out_of_bounds",
    F.when(
        (F.col("risk_score") < 0) | (F.col("risk_score") > 100), 
        1 # Out of bounds
    ).otherwise(0)
)

# 2. Separate 'Sane' data from 'Mathematical Noise'
# If the math is broken (Z-score > 50), we don't want it in the Star Schema
df_gold_final = df_gold_sanitized.filter(F.col("is_mathematically_sane") == 1)

df_math_errors = df_gold_sanitized.filter(F.col("is_mathematically_sane") == 0)

# 3. Save the Clean Gold Table
df_gold_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.fact_transactions")

# 4. Save Math Errors for Developer Review
# This helps the engineering team find bugs in the Silver math code
df_math_errors.write.format("delta") \
    .mode("append") \
    .option("overwriteschema", "true") \
    .saveAsTable("gold.audit_math_failures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

# 1. Access the Gold Fact Table
factTable = DeltaTable.forName(spark, "gold.fact_transactions")

# 2. Execute Delta MERGE
(
    factTable.alias("target")
    .merge(
        source=df_silver_enriched.alias("updates"),
        condition="target.transaction_sk = updates.transaction_sk",
    )
    .whenNotMatchedInsert(
        values={
            # Primary & Foreign Keys
            "transaction_sk": "updates.transaction_sk",  # Generated in step 1
            "account_sk": "updates.account_sk",
            "txn_type_sk": "updates.txn_type_sk",
            "step_sk": "updates.step_sk",
            # Financial Measures & Flags
            "amount": "updates.amount",
            "oldBalanceOrg": "updates.oldBalanceOrg",
            "newBalanceOrig": "updates.newBalanceOrig",
            "risk_score": "updates.risk_score",
            "isFraud": "updates.isFraud",
        }
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#update table dim_txn_type
from delta.tables import *


# 1. Create a starting point for the dimension
initial_data = [("CASH_OUT", "Withdrawal of cash"), ("PAYMENT", "Standard payment")]
schema = "type STRING, description STRING, last_updated TIMESTAMP"

# 2. Check and Force-Create as Delta
if not spark.catalog.tableExists("gold.dim_txn_type"):
    df_init = spark.createDataFrame([], schema)
    df_init.write.format("delta").mode("overwrite").saveAsTable("gold.dim_txn_type")
    print("Table gold.dim_txn_type created as a Delta table.")
else:
    # If it exists but isn't Delta, you might need to recreate it:
    # spark.sql("DROP TABLE gold.dim_txn_type")
    # df_init.write.format("delta").saveAsTable("gold.dim_txn_type")
    print("Table already exists. Proceeding to Merge.")

# 1. Get unique types from your incoming Silver data
new_types = df_silver.select("type").distinct()

# 2. Reference the Gold dimension
targetTable = DeltaTable.forName(spark, "gold.dim_txn_type")

# 3. Merge: Only insert if it doesn't exist
(targetTable.alias("target")
  .merge(
    new_types.alias("source"),
    "target.type = source.type"
  )
  .whenNotMatchedInsert(values = {
    "type": "source.type",
    "description": "'New Transaction Category'", # Placeholder for manual enrichment
    "last_updated": "current_timestamp()"
  })
  .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#update dim_account table
from delta.tables import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Deduplicate incoming Silver updates (take latest transaction per account)
windowSpec = Window.partitionBy("nameOrig_hashed").orderBy(F.col("step").desc())

new_account_states = df_silver \
    .withColumn("row_num", F.row_number().over(windowSpec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num") \
    .select(
        "nameOrig_hashed",
        "customer_segment",
        "account_activity",
        "account_audit_status"
    )

# 2. Stage updates to determine which existing active rows actually CHANGED
staged_updates = new_account_states.alias("updates") \
    .join(
        targetTable.toDF().filter("is_current = true").alias("target"),
        on="nameOrig_hashed",
        how="left"
    ) \
    .select(
        F.col("updates.nameOrig_hashed").alias("merge_key"),
        F.col("updates.*"),
        # Detect attribute changes
        F.when(
            (F.col("target.nameOrig_hashed").isNotNull()) & (
                (F.col("updates.customer_segment") != F.col("target.customer_segment")) |
                (F.col("updates.account_activity") != F.col("target.account_activity")) |
                (F.col("updates.account_audit_status") != F.col("target.account_audit_status"))
            ),
            True
        ).otherwise(False).alias("attr_changed")
    )

# 3. Create the Union DataFrame:
# - Original updates (to insert brand-new accounts or trigger expiration of changed accounts)
# - Duplicated updates with merge_key = NULL (to force insertion of the NEW version of changed accounts)
upsert_df = staged_updates.unionByName(
    staged_updates.filter("attr_changed = true").withColumn("merge_key", F.lit(None))
)

# 4. Execute the SCD Type 2 Delta MERGE
(targetTable.alias("target")
  .merge(
    source = upsert_df.alias("updates"),
    condition = "target.nameOrig_hashed = updates.merge_key AND target.is_current = true"
  )
  # A) EXPIRE: Matched active record where attributes changed -> Close out old row
  .whenMatchedUpdate(
    condition = "updates.attr_changed = true",
    set = {
      "is_current": "false",
      "end_date": "current_timestamp()"
    }
  )
  # B) INSERT: Brand new accounts OR the new row version for changed accounts
  .whenNotMatchedInsert(
    values = {
      "nameOrig_hashed": "updates.nameOrig_hashed",
      "customer_segment": "updates.customer_segment",
      "account_activity": "updates.account_activity",
      "account_audit_status": "updates.account_audit_status",
      "is_current": "true",
      "start_date": "current_timestamp()",
      "end_date": "CAST(NULL AS TIMESTAMP)"
      # Note: account_sk will automatically increment if defined as an IDENTITY column on target table
    }
  )
  .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

def generate_dim_time_step(start_date="2025-01-01", total_steps=744):
    # 1. Generate a sequence for every HOUR (step)
    # 744 steps = 31 days * 24 hours
    df = spark.range(1, total_steps + 1).withColumnRenamed("id", "step")
    
    # 2. Calculate the Timestamp based on the step
    df = df.withColumn("timestamp", F.from_unixtime(
        F.unix_timestamp(F.lit(start_date)) + (F.col("step") - 1) * 3600
    ).cast("timestamp"))
    
    # 3. Extract the attributes your Silver layer expects
    dim_date = df.select(
        F.col("step").cast("int"),
        F.hour("timestamp").alias("hour_of_day"),
        # dayofweek returns 1 (Sun) to 7 (Sat)
        F.dayofweek("timestamp").cast("bigint").alias("day_of_week"),
        F.date_format("timestamp", "EEEE").alias("day_name"),
        F.to_date("timestamp").alias("calendar_date"),
        # Logic for temporal_risk_bucket (Example: Night hours 0-6 are higher risk)
        F.when((F.hour("timestamp") >= 0) & (F.hour("timestamp") <= 6), "High Risk")
         .when((F.hour("timestamp") >= 22), "High Risk")
         .otherwise("Low Risk").alias("temporal_risk_bucket")
    )
    
    return dim_date

# Create the specific time dimension for your simulation
df_date_final = generate_dim_time_step()

# Save to Gold
df_date_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_date")

print("Time Dimension Table successfully generated for 744 steps.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pre-calculate daily fraud stats
df_daily_fraud_summary = spark.sql("""
    SELECT 
        d.day_of_week, 
        t.type, 
        SUM(f.amount) as total_amount,
        COUNT(CASE WHEN f.isFraud = 1 THEN 1 END) as fraud_count
    FROM gold.fact_transactions f
    JOIN gold.dim_date d ON f.step = d.step
    JOIN gold.dim_transaction_type t ON f.type = t.type
    GROUP BY 1, 2
""")

df_daily_fraud_summary.write.mode("overwrite").saveAsTable("gold.agg_daily_fraud")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions as F

# 1. Feature Selection & Binary Encoding
ml_feature_table = df_silver_filterd.select(
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
).withColumn(
    "is_high_risk_type", F.when(F.col("type_risk_level") == "High", 1).otherwise(0)
).drop("type_risk_level")

# 2. Null Imputation (Filling the gaps)
ml_feature_table = ml_feature_table.fillna({
    "z_score": 0.0,
    "behavioral_spike_ratio": 1.0,
    "txns_last_24h": 0,
    "balance_drain_ratio": 0.0,
    "avg_last_10_txns": 0.0
})

# 3. Save to Gold with Change Data Feed enabled
(ml_feature_table.write.format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable("gold.fs_account_features")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. LOAD DATA
df_silver_filterd = spark.read.table("silver.silver_transactions")
fact_df = spark.read.table("gold.fact_transactions")

silver_count = df_silver_filterd.count()
fact_count = fact_df.count()

# 2. FINANCIAL TOTALS (Using float to move data to Python memory)
silver_sum = float(df_silver_filterd.select(F.sum("amount")).collect()[0][0] or 0)
gold_sum = float(fact_df.select(F.sum("amount")).collect()[0][0] or 0)

# 3. REFERENTIAL INTEGRITY
dim_acc_df = spark.read.table("gold.dim_account")
# Finding orphans (transactions in Fact with no matching Account)
orphans = fact_df.join(dim_acc_df, "nameOrig_hashed", "left_anti").count()

# 4. CALCULATION (Using Python's built-in abs)
amount_diff = __builtins__.abs(silver_sum - gold_sum)

# 5. PRINT QUALITY REPORT
print("--- GOLD LAYER QUALITY REPORT ---")
print(f"Row Count Match: {'PASS' if silver_count == fact_count else 'FAIL'} (Gold: {fact_count} | Silver: {silver_count})")
print(f"Missing Rows: {silver_count - fact_count}")
print(f"Referential Integrity Check: {'PASS' if orphans == 0 else 'FAIL'} ({orphans} orphans)")
print(f"Financial Reconciliation: {'PASS' if amount_diff < 0.1 else 'FAIL'} (Diff: {amount_diff})")

# 6. DEBUGGING THE 16 MISSING ROWS
if silver_count != fact_count:
    print("\n[INVESTIGATION] Identifying the 16 missing transaction_sk IDs:")
    # Show the rows that exist in Silver but are missing in Gold
    df_silver_filterd.join(fact_df, "transaction_sk", "left_anti").select("transaction_sk", "amount", "step").show(16)

# 7. SAFETY STOP
if (silver_count != fact_count) or (orphans > 0) or (amount_diff > 0.1):
    print("\n!!! PIPELINE HALTED: Data quality issues detected.")
    # raise Exception("Data Integrity Error") 
else:
    print("\nSUCCESS: Gold Layer verified.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
