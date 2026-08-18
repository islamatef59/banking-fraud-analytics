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

# Run dependency notebook to compute balance discrepancies and produce `v_audited_transactions` view
%run ./NB_SIL_02_Data_Integrity_Audit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Window
import pyspark.sql.functions as F

# 1. LOAD AUDITED DATA
# We pick up the data that passed the integrity checks in Notebook 02
df_audited = spark.read.table("v_audited_transactions")
account_history = Window.partitionBy("nameOrig_hashed").orderBy("step")
window_type = Window.partitionBy("type")
account_window = Window.partitionBy("nameOrig_hashed").orderBy("transaction_date")
historical_window = (
    Window.partitionBy("nameOrig_hashed")
    .orderBy("step")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
global_rank_window = Window.partitionBy("type").orderBy(F.col("amount").desc())

velocity_window = (
    Window.partitionBy("nameOrig_hashed")
    .orderBy("step")
    .rangeBetween(-23, 0) 
)
sliding_window_10 = (Window.partitionBy("nameOrig_hashed") 
                          .orderBy("step") 
                          .rowsBetween(-10, -1) # Exclude the current row
                    )
historical_window = Window.partitionBy("nameOrig_hashed") \
                          .orderBy("step") \
                          .rowsBetween(Window.unboundedPreceding, Window.currentRow)                    

df_silver = (
    df_audited
    .withColumn("temporal_risk_bucket",
                 F.when((F.col("step") % 24 <= 4) | (F.col("step") % 24 >= 23), "Late Night")
                 .otherwise("Standard Hours")
            )
    
    .withColumn("fund_direction", 
                when(col("type").isin(['CASH_OUT', 'TRANSFER', 'DEBIT']), "Outbound")
                .when(col("type").isin(['CASH_IN', 'PAYMENT']), "Inbound")
                .otherwise("Internal")
               )
    .withColumn("customer_segment", 
               F.when(F.col("amount") > 500000, "Platinum")
               .when(F.col("amount") > 100000, "Gold")
               .otherwise("Standard")
               )      
    .withColumn("type_risk_level",          
    F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), "High")
     .otherwise("Low")    
        )    
    .withColumn("spark_job_id", F.lit(spark.sparkContext.applicationId))
    .withColumn("partition_id", spark_partition_id())
    .withColumn("balance_drain_ratio", 
       when(col("oldbalanceOrg") > 0, (col("amount") / col("oldbalanceOrg")) * 100)
        .otherwise(0)        
    )
    .withColumn("first_digit", F.substring(F.col("amount").cast("string"), 1, 1).cast("int"))
    .withColumn("prev_step", F.lag("step").over(account_history))
    .withColumn("hours_since_last_txn",F.col("step") - F.col("prev_step"))
    .withColumn("txns_last_24h", F.count("transaction_sk").over(velocity_window))
    .withColumn("days_since_last_txn",F.when(F.col("txns_last_24h") > 3, "High Velocity").otherwise("Normal"))
    .withColumn("avg_historical_amount", F.avg("amount").over(account_history))
    .withColumn("behavioral_spike_ratio", 
    F.when(F.col("avg_historical_amount") > 0, F.col("amount") / F.col("avg_historical_amount"))
    .otherwise(1)
    )
)
df_silver=(
    df_silver
    .withColumn("avg_amount", F.avg("amount").over(window_type)) 
    .withColumn("stddev_amount", F.stddev("amount").over(window_type))
    .withColumn("running_total_outflow", F.sum("amount").over(account_history))
    .withColumn("z_score",(F.col("amount") - F.col("avg_amount")) / F.col("stddev_amount"))
    .withColumn("drainage_ratio",
    # Drainage Ratio: Is this one transaction a huge % of their total history?
      F.round((F.col("amount") / F.col("running_total_outflow")) * 100, 2)      
    )
    .withColumn(
    "prev_step", F.lag("step").over(account_history)
    ).withColumn(
    "prev_amount", F.lag("amount").over(account_history)
     ).withColumn(
    "time_since_last_txn", F.col("step") - F.col("prev_step")
    )
    .withColumn(
    "historical_avg_amount", 
     F.avg("amount").over(historical_window)
    )

    .withColumn("behavioral_spike_ratio", 
    F.when(F.col("historical_avg_amount") > 0, F.col("amount") / F.col("historical_avg_amount"))
    .otherwise(1)
    )
    .withColumn("behavioral_anomaly", 
    F.when(F.col("behavioral_spike_ratio") > 5, "Abnormal").otherwise("Normal")
    )
    .withColumn("rank_in_type", F.rank().over(global_rank_window))
    .withColumn("transaction_rank_history", F.row_number().over(historical_window)
    )
    .withColumn("account_activity", 
    F.when(F.col("transaction_rank_history") <= 5, "New Account").otherwise(" Legacy Account.")
    )
    .withColumn("avg_last_10_txns", F.avg("amount").over(sliding_window_10)
    )
    .withColumn(
    "action_rank", F.row_number().over(account_history)
    )
    .withColumn("is_math_correct",
    F.when(F.abs((F.col("oldBalanceOrg") - F.col("amount")) - F.col("newBalanceOrig")) < 0.01, 1)
     .otherwise(0)
    )
    .withColumn("cumulative_integrity_failures", 
    F.sum("is_math_correct").over(Window.partitionBy("nameOrig_hashed").orderBy("step"))
    )
    .withColumn("account_audit_status", 
    F.when(F.col("cumulative_integrity_failures") > 2, "Flagged for Audit").otherwise("Clean")
    )
    .withColumn("cumulative_outflow", 
    F.sum("amount").over(historical_window)
    )
    .withColumn("pct_of_total_outflow", 
    (F.col("amount") / F.col("cumulative_outflow")) * 100
    )
)

# 2. DEFINE ANALYTICAL WINDOWS
# We partition by origin_hash to look at each customer's specific history

df_silver=(
    df_silver
            .withColumn("is_round_number",
            F.when(F.col("amount") % 100 == 0, 1).otherwise(0)
            )
            .withColumn("is_drain_event", when(col("balance_drain_ratio") > 90, 1).otherwise(0)
            )
            .withColumn("is_behavioral_anomaly", 
            F.when(F.col("behavioral_spike_ratio") > 5, 1).otherwise(0)
            )
            .withColumn("is_round_number", 
            F.when(F.col("amount") % 100 == 0, 1).otherwise(0)
            )
            .withColumn(
            "is_first_action", F.when(F.col("action_rank") == 1, 1).otherwise(0)
             )
)  
display(df_silver)                     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import Window
import pyspark.sql.functions as F

# =========================================================================
# 1. LOAD DATA
# =========================================================================
df_audited = spark.read.table("v_audited_transactions")

# =========================================================================
# 2. DEFINE ANALYTICAL WINDOWS
# =========================================================================
# User-Specific History (by Step)
account_history = Window.partitionBy("nameOrig_hashed").orderBy("step")

# Global Context by Transaction Type
window_type = Window.partitionBy("type")

# Range-based Velocity (Last 24 hours)
velocity_window = (
    Window.partitionBy("nameOrig_hashed")
    .orderBy("step")
    .rangeBetween(-23, 0)
)

# Moving Average (Last 10 transactions, excluding current)
sliding_window_10 = (
    Window.partitionBy("nameOrig_hashed")
    .orderBy("step")
    .rowsBetween(-10, -1)
)

# Full Historical Cumulative Window
historical_window = (
    Window.partitionBy("nameOrig_hashed")
    .orderBy("step")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# Global Ranking for Amount within Type
global_rank_window = Window.partitionBy("type").orderBy(F.col("amount").desc())

# First Action Window (to identify account opening)
first_action_window = Window.partitionBy("nameOrig_hashed").orderBy("step")

# =========================================================================
# 3. DATA TRANSFORMATIONS (SILVER LAYER)
# =========================================================================
df_silver = (
    df_audited
    # --- Category & Risk Labeling ---
    .withColumn("temporal_risk_bucket",
        F.when((F.col("step") % 24 <= 4) | (F.col("step") % 24 >= 23), "Late Night")
        .otherwise("Standard Hours")
    )
    .withColumn("fund_direction", 
        F.when(F.col("type").isin(['CASH_OUT', 'TRANSFER', 'DEBIT']), "Outbound")
        .when(F.col("type").isin(['CASH_IN', 'PAYMENT']), "Inbound")
        .otherwise("Internal")
    )
    .withColumn("customer_segment", 
        F.when(F.col("amount") > 500000, "Platinum")
        .when(F.col("amount") > 100000, "Gold")
        .otherwise("Standard")
    )
    .withColumn("type_risk_level",          
        F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), "High").otherwise("Low")
    )

    # --- System Metadata ---
    .withColumn("spark_job_id", F.lit(spark.sparkContext.applicationId))
    .withColumn("partition_id", F.spark_partition_id())

    # --- Basic Math & Time Analytics ---
    .withColumn("balance_drain_ratio", 
        F.when(F.col("oldbalanceOrg") > 0, (F.col("amount") / F.col("oldbalanceOrg")) * 100).otherwise(0)
    )
    .withColumn("first_digit", F.substring(F.col("amount").cast("string"), 1, 1).cast("int"))
    .withColumn("prev_step", F.lag("step").over(account_history))
    .withColumn("hours_since_last_txn", F.col("step") - F.col("prev_step"))
    .withColumn("time_since_last_txn", F.col("step") - F.col("prev_step")) # Keep both as per your request
    .withColumn("txns_last_24h", F.count("amount").over(velocity_window))
    .withColumn("days_since_last_txn", F.when(F.col("txns_last_24h") > 3, "High Velocity").otherwise("Normal"))

    # --- Statistical & Historical Metrics ---
    .withColumn("avg_amount", F.avg("amount").over(window_type)) 
    .withColumn("stddev_amount", F.stddev("amount").over(window_type))
    .withColumn("running_total_outflow", F.sum("amount").over(account_history))
    .withColumn("z_score", (F.col("amount") - F.col("avg_amount")) / F.col("stddev_amount"))
    .withColumn("drainage_ratio", F.round((F.col("amount") / F.col("running_total_outflow")) * 100, 2))
    .withColumn("prev_amount", F.lag("amount").over(account_history))
    .withColumn("historical_avg_amount", F.avg("amount").over(historical_window))
    .withColumn("avg_historical_amount", F.avg("amount").over(account_history))
    .withColumn("avg_last_10_txns", F.avg("amount").over(sliding_window_10))

    # --- Behavioral Flags ---
    .withColumn("behavioral_spike_ratio", 
        F.when(F.col("historical_avg_amount") > 0, F.col("amount") / F.col("historical_avg_amount")).otherwise(1)
    )
    .withColumn("behavioral_anomaly", 
        F.when(F.col("behavioral_spike_ratio") > 5, "Abnormal").otherwise("Normal")
    )
    .withColumn("is_behavioral_anomaly", 
        F.when(F.col("behavioral_spike_ratio") > 5, 1).otherwise(0)
    )
    .withColumn("is_round_number", F.when(F.col("amount") % 100 == 0, 1).otherwise(0))
    .withColumn("is_drain_event", F.when(F.col("balance_drain_ratio") > 90, 1).otherwise(0))

    # --- Ranking & History Management ---
    .withColumn("rank_in_type", F.rank().over(global_rank_window))
    .withColumn("transaction_rank_history", F.row_number().over(historical_window))
    .withColumn("action_rank", F.row_number().over(first_action_window))
    .withColumn("is_first_action", F.when(F.col("action_rank") == 1, 1).otherwise(0))
    .withColumn("account_activity", 
        F.when(F.col("transaction_rank_history") <= 5, "New Account").otherwise("Established Account")
    )

    # --- Integrity Audits ---
    .withColumn("is_math_correct",
        F.when(F.abs((F.col("oldbalanceOrg") - F.col("amount")) - F.col("newbalanceOrig")) < 0.01, 1).otherwise(0)
    )
    .withColumn("cumulative_integrity_failures", 
        F.sum("is_math_correct").over(account_history)
    )
    .withColumn("account_audit_status", 
        F.when(F.col("cumulative_integrity_failures") > 2, "Flagged for Audit").otherwise("Clean")
    )
    .withColumn("cumulative_outflow", F.sum("amount").over(historical_window))
    .withColumn("pct_of_total_outflow", (F.col("amount") / F.col("cumulative_outflow")) * 100)
)

# =========================================================================
# 4. VIEW RESULTS
# =========================================================================


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# --- 1. CONFIGURATION ---
target_schema = "silver"
target_table = "silver_transactions"
full_table_path = f"{target_schema}.{target_table}"

# Ensure schema exists and is active
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE {target_schema}")

# --- 2. ENSURE SURROGATE KEY EXISTS ---
# The Merge needs a unique ID. If 'transaction_sk' isn't in your df_silver yet, we create it.
if "transaction_sk" not in df_silver.columns:
    df_silver = df_silver.withColumn("transaction_sk", F.sha2(F.concat_ws("||", "origin_hash", "step", "amount", "txn_type"), 256))

# --- 3. UPSERT LOGIC WITH ERROR HANDLING ---
try:
    # Check if table exists and is actually a Delta table
    is_delta = False
    if spark.catalog.tableExists(target_table):
        # Inspect table metadata to confirm format
        table_details = spark.sql(f"DESCRIBE DETAIL {full_table_path}").collect()[0]
        is_delta = (table_details["format"] == "delta")

    if is_delta:
        # Professional Upsert (Merge)
        print(f"--> Valid Delta table found. Performing UPSERT (Merge) on {target_table}...")
        dt = DeltaTable.forName(spark, full_table_path)
        
        start_count = spark.read.table(full_table_path).count()
        
        dt.alias("target").merge(
            df_silver.alias("source"),
            "target.transaction_sk = source.transaction_sk"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
        end_count = spark.read.table(full_table_path).count()
        print(f"Successfully Merged. Rows before: {start_count}, Rows after: {end_count}")

    else:
        # Initial Load or Repair (If table existed but was NOT Delta)
        print(f"--> Initializing/Repairing table {target_table} as a clean Delta table...")
        df_silver.write.format("delta").mode("overwrite").saveAsTable(full_table_path)
        print(f"Initial Silver Table {target_table} Created.")

except Exception as e:
    print(f"Critical Error during materialization: {e}")
    # If the catalog is totally corrupted, uncomment the line below to reset:
    # spark.sql(f"DROP TABLE IF EXISTS {full_table_path}")

# --- 4. PERFORMANCE & METADATA ---
print(f"--> Optimizing {target_table} storage layout...")
spark.sql(f"OPTIMIZE {full_table_path} ZORDER BY (step)")

spark.sql(f"""
    ALTER TABLE {full_table_path} SET TBLPROPERTIES (
        'comment' = 'Refined banking transactions with PII masking and risk scoring',
        'project' = 'Banking Analytics 2026',
        'data_owner' = 'Data Engineering Team',
        'table_status' = 'Production'
    )
""")

print("Final Materialization Step Complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
