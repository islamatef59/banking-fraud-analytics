# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import col, sha2, current_timestamp, to_date, when, abs,spark_partition_id
source_table = "bronze_transactions"
df_bronze = spark.read.table(source_table)


df_silver_refined = df_bronze.select(
    # 1. STEP: Convert 'Step' (hours) into Time-of-Day buckets
    F.col("step"),
    F.when((F.col("step") % 24 <= 4) | (F.col("step") % 24 >= 23), "Late Night")
     .otherwise("Standard Hours").alias("temporal_risk_bucket"),

    # 2. NAME_ORIG & NAME_DEST: Masking PII for Security
    F.sha2(F.col("nameOrig").cast("string"), 256).alias("origin_hash"),
    F.sha2(F.col("nameDest").cast("string"), 256).alias("dest_hash"),

    # 3. AMOUNT: Statistical and Forensic Checks
    F.col("amount").cast("double"),
    F.when(F.col("amount") % 100 == 0, 1).otherwise(0).alias("is_round_number"),

    # 4. BALANCES: Integrity Audit (Math check)
    F.col("oldBalance").cast("double"),
    F.col("newBalance").cast("double"),
    F.when(F.abs((F.col("oldBalance") - F.col("amount")) - F.col("newBalance")) < 0.01, 1)
     .otherwise(0).alias("is_math_correct"),

    # 5. TYPE: Logic-based Risk Scoring
    F.col("type").alias("txn_type"),
    F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), "High")
     .otherwise("Low").alias("type_risk_level")

).withColumn(
    # 6. CUMULATIVE: Account Drainage Ratio
    # (What % of their total history is this one transaction?)
    "running_total_outflow", F.sum("amount").over(account_history)
).withColumn(
    "drainage_ratio", (F.col("amount") / F.col("running_total_outflow")) * 100
).withColumn(
    # 7. DESTINATION: Is this a "New" destination for this user?
    "dest_appearance_rank", F.row_number().over(Window.partitionBy("origin_hash", "dest_hash").orderBy("step"))
).withColumn(
    "is_new_destination", F.when(F.col("dest_appearance_rank") == 1, 1).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from pyspark.sql.functions import col, sha2, current_timestamp, to_date, when, abs,spark_partition_id

# --- CONFIGURATION ---
# Using the exact table name you provided
source_table = "bronze_transactions"
target_table = "silver_banking_transactions"

# --- 1. LOAD BRONZE DATA ---
df_bronze = spark.read.table(source_table)

# --- 2. PROFESSIONAL TRANSFORMATIONS ---
df_silver=df_bronze.filter(col("amount") >0)
df_silver_invalid=df_bronze.filter(col("amount") <0)
df_silver_invalid_count=df_silver_invalid.count()
if df_silver_invalid_count >0 :
   print(f"Invalid Rows Number :   {df_silver_invalid_count}")

df_silver = df_bronze.select(
    col("transaction_id"),
    # PII MASKING: Hashing account names/numbers for security
    sha2(col("nameOrig").cast("string"), 256).alias("origin_account_hash"),
    sha2(col("nameDest").cast("string"), 256).alias("dest_account_hash"),
    # CASTING: Ensuring numeric and date types are correct for analytics
    col("amount").cast("double"),
    col("oldbalanceOrg").cast("double"),
    col("newbalanceOrig").cast("double"),
    to_date(col("transaction_date")).alias("transaction_date"),
    col("type").alias("transaction_type"),
    when(col("amount")<0 & col("transaction_type")=="TRANSFER",Lit(1))
    .otherwise(lit(0)).alias("is_suspicious_flag"),
    when((col("type") == 'TRANSFER') & (col("amount") > 200000), "High Risk")
    .when(col("amount") > 100000, "Medium Risk")
    .otherwise("Low Risk").alias("risk_score"),
    when((col("amount") > 100000) & (col("newbalanceOrig") == 0), "Critical - Account Emptied")
   .otherwise("Normal").alias("fraud_pattern")
    current_timestamp().alias("silver_load_at"),
    .col("prev_amount", F.lag("amount").over(windowSpec)) \
                     .withColumn("amount_diff", F.col("amount") - F.col("prev_amount")) \
                     .fillna(0, subset=["prev_amount", "amount_diff"]) # Fill only specific columns
    # AUDIT COLUMN: Tracking when this record was processed
    current_timestamp().alias("silver_load_at")
).filter(col("transaction_id").isNotNull())
df_silver = df_silver.withColumn("prev_amount", F.coalesce(F.lag("amount").over(windowSpec), F.lit(0))) \
                     .withColumn("amount_diff", F.col("amount") - F.col("prev_amount")) \
                     .fillna(0, subset=["prev_amount", "amount_diff"]) # Fill only specific columns
df_silver = df_silver.withColumn("customer_segment", 
    F.when(F.col("amount") > 500000, "Platinum")
    .when(F.col("amount") > 100000, "Gold")
    .otherwise("Standard")
)  
          
df_silver = df_silver.withColumn("is_balance_consistent", 
    F.when(F.abs((F.col("oldbalanceOrg") - F.col("amount")) - F.col("newbalanceOrig")) < 0.01, True)
    .otherwise(False)
)

df_silver = df_silver.withColumn("transaction_hour", hour(col("ingestion_timestamp"))) \
    .withColumn("time_bucket", 
        when((col("transaction_hour") >= 23) | (col("transaction_hour") <= 4), "Late Night")
        .when((col("transaction_hour") >= 5) & (col("transaction_hour") <= 11), "Morning")
        .when((col("transaction_hour") >= 12) & (col("transaction_hour") <= 17), "Afternoon")
        .otherwise("Evening")
    )

df_silver = df_silver.withColumn("segment", 
    when(col("amount") < 1000, "Retail")
    .when((col("amount") >= 1000) & (col("amount") < 50000), "Professional")
    .when((col("amount") >= 50000) & (col("amount") < 500000), "Institutional")
    .otherwise("Ultra High Net Worth")
)   
df_silver = df_silver.withColumn("balance_drain_ratio", 
    when(col("oldbalanceOrg") > 0, (col("amount") / col("oldbalanceOrg")) * 100)
    .otherwise(0)
) \
.withColumn("is_drain_event", when(col("balance_drain_ratio") > 90, 1).otherwise(0)) 

# Identify the direction of funds
df_silver = df_silver.withColumn("fund_direction", 
    when(col("type").isin(['CASH_OUT', 'TRANSFER', 'DEBIT']), "Outbound")
    .when(col("type").isin(['CASH_IN', 'PAYMENT']), "Inbound")
    .otherwise("Internal")
)  


# 1. Define a window to calculate stats across the whole dataset by Type
window_type = Window.partitionBy("transaction_type")

# 2. Calculate Mean and StdDev for each transaction type
df_silver = df_silver.withColumn("avg_amount", F.avg("amount").over(window_type)) \
                     .withColumn("stddev_amount", F.stddev("amount").over(window_type))

# 3. Calculate the Z-Score. Anything > 3 is a statistical anomaly.
df_silver = df_silver.withColumn("z_score", 
    (F.col("amount") - F.col("avg_amount")) / F.col("stddev_amount")
).withColumn("is_statistical_outlier", F.when(F.abs(F.col("z_score")) > 3, 1).otherwise(0))


df_silver = df_silver.withColumn("first_digit", F.substring(F.col("amount").cast("string"), 1, 1).cast("int"))

df_silver = df_silver.withColumn("spark_job_id", F.lit(spark.sparkContext.applicationId)) \
                     .withColumn("partition_id", spark_partition_id())

# Define a window: Group by account, order by date
account_window = Window.partitionBy("origin_account_hash").orderBy("transaction_date")

# 1. Get the timestamp of the PREVIOUS transaction
df_silver = df_silver.withColumn("prev_transaction_date", F.lag("transaction_date").over(account_window))

# 2. Calculate the "Velocity" (Days between transactions)
# If the difference is 0, it means multiple transactions on the same day
df_silver = df_silver.withColumn("days_since_last_txn", 
    F.datediff(F.col("transaction_date"), F.col("prev_transaction_date"))
)

# 3. Flag "High Velocity" (e.g., more than 3 transactions in 1 day)
df_silver = df_silver.withColumn("velocity_flag", 
    F.when(F.col("days_since_last_txn") == 0, "High Velocity").otherwise("Normal")
)                 
# --- 3. DEDUPLICATION ---
# Ensures we only have one unique record per transaction ID
dq_summary = df_silver.select([
    (F.count(when(F.col(c).isNull(), c)) / F.count(lit(1))).alias(c) 
    for c in ["transaction_id", "origin_account_hash", "amount"]
])
print("Data Quality Null Ratio Audit:")
dq_summary.show()


window_dest = Window.partitionBy("dest_account_hash").orderBy("transaction_date")

df_silver = df_silver.withColumn("destination_rank", F.row_number().over(window_dest)) \
                     .withColumn("is_new_destination", F.when(F.col("destination_rank") == 1, 1).otherwise(0))


df_silver = df_silver.withColumn("avg_historical_amount", F.avg("amount").over(running_avg_window))

# Calculate the "Spike Ratio"
df_silver = df_silver.withColumn("behavioral_spike_ratio", 
    F.when(F.col("avg_historical_amount") > 0, F.col("amount") / F.col("avg_historical_amount"))
    .otherwise(1)
)

# Flag if the transaction is 5x larger than their normal behavior
df_silver = df_silver.withColumn("behavioral_anomaly", 
    F.when(F.col("behavioral_spike_ratio") > 5, "Abnormal").otherwise("Normal")
)

# Check if the amount is a "Perfect Round Number"
df_silver = df_silver.withColumn("is_round_number", 
    F.when(F.col("amount") % 100 == 0, 1).otherwise(0)
)

# Rank transactions globally by amount but within each transaction type
global_rank_window = Window.partitionBy("transaction_type").orderBy(F.col("amount").desc())

df_silver = df_silver.withColumn("rank_in_type", F.rank().over(global_rank_window))

# Create a unique fingerprint for the row's data
df_silver = df_silver.withColumn("row_fingerprint", 
    F.sha2(F.concat_ws("||", *["transaction_id", "origin_account_hash", "amount"]), 256)
)

df_silver = df_silver.withColumn("total_risk_score", 
    (col("is_account_emptied_flag") * 40) +  # High weight for emptying account
    (col("is_behavioral_anomaly") * 30) +    # Medium weight for unusual spikes
    (when(col("risk_score") == "High Risk", 20).otherwise(0)) + 
    (when(col("is_round_number") == 1, 10).otherwise(0))
)

df_silver = df_silver.withColumn("time_diff_minutes", 
    (col("transaction_date").cast("long") - F.lag(col("transaction_date").cast("long")).over(window_session)) / 60
)

# Create a 'New Session' flag if the gap is > 60 minutes
df_silver = df_silver.withColumn("new_session_flag", 
    when(col("time_diff_minutes") > 60, 1).otherwise(0)
)

# Running sum of flags creates a unique Session ID per account
df_silver = df_silver.withColumn("session_id", 
    F.sum("new_session_flag").over(window_session.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# Rank transactions for each user (1st, 2nd, 3rd...)
df_silver = df_silver.withColumn("transaction_rank_history", 
    F.row_number().over(historical_window)
)

# Flag "New Account Activity" (First 5 transactions)
df_silver = df_silver.withColumn("account_activity", 
    F.when(F.col("transaction_rank_history") <= 5, "New Account").otherwise(" Legacy Account.")
)

# Define a "Sliding Window" of the last 10 transactions
sliding_window_10 = Window.partitionBy("origin_account_hash") \
                          .orderBy("transaction_date") \
                          .rowsBetween(-10, -1) # Exclude the current row

# 1. Calculate the average of the last 10 transactions
df_silver = df_silver.withColumn("avg_last_10_txns", 
    F.avg("amount").over(sliding_window_10)
)

# 2. Identify "Spending Spikes" (e.g., 3x higher than their 10-txn average)
df_silver = df_silver.withColumn("is_spending_spike", 
    F.when(F.col("amount") > (F.col("avg_last_10_txns") * 3), 1).otherwise(0)
)


# Create a weighted score based on all your advanced analyses
df_silver = df_silver.withColumn("cumulative_risk_index", 
    (F.col("is_spending_spike") * 30) + 
    (F.col("is_new_account_activity") * 20) + 
    (F.col("is_account_emptied_flag") * 50) # Assuming you have this from previous steps
)

# 1. Global Window for the entire dataset by Type
global_type_window = Window.partitionBy("transaction_type")

# 2. Calculate the global average for that type
df_silver = df_silver.withColumn("global_avg_type_amount", F.avg("amount").over(global_type_window))

# 3. Create a 'Relative Volume Index' (How many times larger is this than the global average?)
df_silver = df_silver.withColumn("relative_volume_index", 
    F.round(F.col("amount") / F.col("global_avg_type_amount"), 2)
)

# Define a window for the last 30 days (assuming transaction_date is cast to Long/Unix)
# Using rowsBetween for a 30-record lookback as a proxy for time-series volatility
volatility_window = Window.partitionBy("origin_account_hash") \
                          .orderBy("transaction_date") \
                          .rowsBetween(-30, -1)

# Calculate the Standard Deviation of historical amounts
df_silver = df_silver.withColumn("historical_stddev", F.stddev("amount").over(volatility_window))

# Flag 'Volatility Spikes'
df_silver = df_silver.withColumn("is_volatility_spike", 
    F.when(F.col("amount") > (F.col("historical_stddev") * 3), 1).otherwise(0)
)

# 1. Find the FIRST transaction date for this account
df_silver = df_silver.withColumn("first_account_activity", F.min("transaction_date").over(Window.partitionBy("origin_account_hash")))

# 2. Calculate Account Age in days at the time of THIS transaction
df_silver = df_silver.withColumn("account_age_days", 
    F.datediff(F.col("transaction_date"), F.col("first_account_activity"))
)

# 3. Flag 'Infant Account' high-risk period (First 14 days)
df_silver = df_silver.withColumn("is_infant_account_risk", 
    F.when(F.col("account_age_days") <= 14, 1).otherwise(0)
)

# Cumulative count of math failures for this specific account
df_silver = df_silver.withColumn("cumulative_integrity_failures", 
    F.sum("is_math_correct").over(Window.partitionBy("origin_account_hash").orderBy("transaction_date"))
)

# Flag account as 'High Audit Risk' if they have more than 2 failures
df_silver = df_silver.withColumn("account_audit_status", 
    F.when(F.col("cumulative_integrity_failures") > 2, "Flagged for Audit").otherwise("Clean")
)
# Define a window partitioned by account, ordered by date
# This looks at ALL rows from the beginning of time up to the CURRENT row
historical_window = Window.partitionBy("origin_account_hash") \
                          .orderBy("transaction_date") \
                          .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 1. Cumulative Sum of all money sent by this account
df_silver = df_silver.withColumn("cumulative_outflow", 
    F.sum("amount").over(historical_window)
)

# 2. Percentage of Total Outflow represented by this specific transaction
df_silver = df_silver.withColumn("pct_of_total_outflow", 
    (F.col("amount") / F.col("cumulative_outflow")) * 100
)


# Create a Data Quality Summary for the recruiter to see
print("### FINAL DATA QUALITY SUMMARY ###")
summary_df = df_silver.select(
    round(F.mean("amount"), 2).alias("Avg_Txn_Amount"),
    F.sum("is_drain_event").alias("Total_Drain_Events"),
    F.count(when(col("risk_score") == "High Risk", 1)).alias("High_Risk_Count")
)
summary_df.show()


df_final = df_silver.dropDuplicates(["transaction_id"])

anomaly_count = df_final.filter(col("amount") < 0).count()

if anomaly_count > 0:
    # In a real project, you might send an alert or move these to a 'quarantine' table
    print(f"WARNING: Detected {anomaly_count} negative transactions. Investigating...")
else:
    print("Data Quality Check Passed: No negative amounts found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

if spark.catalog.tableExists(target_table):
    # Professional Upsert: Update if ID exists, Insert if it is new
    dt = DeltaTable.forName(spark, target_table)
    start_count=spark.read.table("target_table").count()
    dt.alias("target").merge(
        df_final.alias("source"),
        "target.transaction_id = source.transaction_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    end_count=spark.read.table("target_table").count()
    print(f"Successfully Merged into {target_table}")
else:
    # Initial load if table doesn't exist yet
    df_final.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Initial Silver Table {target_table} Created.")
    print(f" New Rows added : {end_count-start_count}")

# --- 4. PERFORMANCE OPTIMIZATION ---
# Z-Ordering by date makes your Power BI reports run 10x faster
spark.sql(f"OPTIMIZE {target_table} ZORDER BY (transaction_date)")

spark.sql(f"""
    ALTER TABLE {target_table} SET TBLPROPERTIES (
        'comment' = 'Refined banking transactions with PII masking and risk scoring',
        'project' = 'Banking Analytics 2024',
        'data_owner' = 'Data Engineering Team'
    )
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
