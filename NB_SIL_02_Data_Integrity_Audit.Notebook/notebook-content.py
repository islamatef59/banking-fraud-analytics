# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a93fdddf-f2f6-4b97-9ede-0a31911e78d2",
# META       "default_lakehouse_name": "lh_banking_transactions",
# META       "default_lakehouse_workspace_id": "55d949cb-dda8-4e9a-b467-70a86a061a7b",
# META       "known_lakehouses": [
# META         {
# META           "id": "a93fdddf-f2f6-4b97-9ede-0a31911e78d2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run ./NB_SIL_01_Transformation_Security_and_Masking


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. FETCH DATA FROM PREVIOUS STEP
# Load cleaned Silver view generated in preceding transformation step
df_security = spark.table("v_security_cleaned")

# 2. CALCULATE MATHEMATICAL DISCREPANCIES (Gaps)
# Calculate absolute balance variances across origin and destination accounts
df_with_gaps = df_security.withColumn(
    "origin_gap", 
    F.abs((F.col("oldbalanceOrg") - F.col("amount")) - F.col("newbalanceOrig"))
).withColumn(
    "dest_gap",
    F.abs((F.col("oldbalanceDest") + F.col("amount")) - F.col("newbalanceDest"))
)
# 3. APPLY THEORETICAL DATA QUALITY CATEGORIES
# Categorize balance discrepancies, flag untracked endpoints, and assign risk tiers
df_audited = df_with_gaps.withColumn(
    "quality_tier",
    F.when(F.col("origin_gap") < 0.01, "A_PERFECT_LEDGER")        # Math is perfect
    .when(F.col("origin_gap") < 1.00, "B_ROUNDING_ERROR")      # Tiny rounding noise
    .when(F.col("origin_gap") == F.col("amount"), "C_GHOST_TRANSFER") # Total math failure
    .otherwise("D_SYSTEM_ANOMALY")                              # Other unexplained gaps
).withColumn(
    "is_untracked_destination", 
    (F.col("oldbalanceDest") == 0) & (F.col("newbalanceDest") == 0)
).withColumn(
    "is_ledger_balanced",
    F.when((F.col("origin_gap") < 0.01) & (F.col("dest_gap") < 0.01), True).otherwise(False)
).withColumn(
    "relative_origin_error",
    F.when(F.col("amount") != 0, F.round(F.col("origin_gap") / F.col("amount"))).otherwise(0)
).withColumn( # <--- Added the missing withColumn here
    "risk_score",
    F.when((F.col("type") == 'TRANSFER') & (F.col("amount") > 200000), "High Risk")
    .when(F.col("amount") > 100000, "Medium Risk")
    .otherwise("Low Risk")
)

# 4. GENERATE THE AUDIT SUMMARY TABLE
# Aggregate quality metrics across tiers and validate critical key null rates
audit_report = df_audited.groupBy("quality_tier").agg(
    F.count("*").alias("record_count"),
    F.avg("amount").alias("average_transfer_val"),
    F.max("origin_gap").alias("max_discrepancy")
).orderBy("quality_tier")

dq_summary = df_audited.select([
    (F.count(when(F.col(c).isNull(), c)) / F.count(lit(1))).alias(c) 
    for c in ["transaction_sk", "nameOrig_hashed", "amount"]
])
print("Data Quality Null Ratio Audit:")
dq_summary.show()

# Register audited DataFrame as temp view for Gold aggregation stage
print("--- Data Integrity And Audit are applied ---")
df_audited.createOrReplaceTempView("v_audited_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
