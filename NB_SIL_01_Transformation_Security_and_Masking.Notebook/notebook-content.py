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

# #### **Securtity,Masking and Trasformation Notebook****

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType, BooleanType, TimestampType

# Read source Bronze layer data
df_bronze = spark.read.table("bronze.bronze_transactions")

# Log bad records (negative amounts) before downstream transformations
df_silver_invalid=df_bronze.filter(col("amount") <0)
df_silver_invalid_count=df_silver_invalid.count()
if df_silver_invalid_count >0 :
   print(f"Invalid Rows Number :   {df_silver_invalid_count}")

# 1. Define a secret salt for Hashing (Security Best Practice)
# Pepper salt into PII hashes to block dictionary/rainbow table lookups
SALT = "FIN_SECURE_2026"

def transform_to_silver(df):
    return df.select(
        # --- CLEANING & STANDARDISATION ---
        # Standardize types and clean key string fields
        F.col("step").cast("int"),
        F.upper(F.trim(F.col("type"))).alias("type"),
        F.col("status").cast("string"),
        # --- FINANCIAL PRECISION  ---
        # Preserve monetary exactness (avoids float precision errors)
        F.col("amount").cast(DecimalType(18, 2)),
        F.col("oldBalanceOrg").cast(DecimalType(18, 2)),
        F.col("newBalanceOrig").cast(DecimalType(18, 2)),
        F.col("oldBalanceDest").cast(DecimalType(18, 2)),
        F.col("newBalanceDest").cast(DecimalType(18, 2)),
        
        # --- SECURITY & MASKING ---
        # Mask customer IDs and generate surrogate key for transaction granularity
        F.sha2(F.concat(F.upper(F.trim(F.col("nameOrig"))), F.lit(SALT)), 256).alias("nameOrig_hashed"),
        F.sha2(F.concat(F.upper(F.trim(F.col("nameDest"))), F.lit(SALT)), 256).alias("nameDest_hashed"),
        F.sha2(F.concat_ws("&",F.col("step").cast("string"),F.col("nameOrig_hashed"),F.col("amount").cast("string")),256).alias("transaction_sk"),
        F.col("nameDest").startswith("M").alias("is_merchant_dest"),
        
        # Rule-based fraud & risk categorization
        F.when((col("type") == 'TRANSFER') & (col("amount") > 200000), "High Risk")
        .when(col("amount") > 100000, "Medium Risk")
        .otherwise("Low Risk").alias("material_risk_level"),
        F.when((col("amount") > 100000) & (col("newbalanceOrig") == 0), "Critical - Account Emptied")
        .otherwise("Normal").alias("fraud_pattern"),
        current_timestamp().alias("silver_load_at"),
        
        # Standardize flags to boolean types
        F.col("isFraud").cast(BooleanType()),
        F.col("isFlaggedFraud").cast(BooleanType()),
        F.col("is_pii_flag").cast(BooleanType()),
        
        # --- METADATA & AUDIT ---
        # Lineage metadata tracking
        F.to_timestamp(F.col("ingestion_timestamp")).alias("ingestion_timestamp"),
        F.col("source_file"),
        F.col("job_run_id")
    )
def advanced_silver_transform(df):
      return df.withColumn(
        # Identify zero-balance destination accounts receiving funds
        "is_new_destination", (F.col("oldBalanceDest") == 0) & (F.col("newBalanceDest") == 0) & (F.col("amount") > 0)
        
      ).withColumn(
      # Extract hourly cycle from continuous step counter
      "hour_of_day", F.col("step") % 24
      ).withColumn(
        # Derive day index (1 to 7) for weekly pattern analysis
        "day_of_week", (F.floor(F.col("step") / 24) % 7) + 1  # 1=Monday, etc.
        
      ).withColumn(
        # 5. OUTLIER & IMPOSSIBLE FLAGS
        # Catch transactions exceeding available sender balance
        "is_overdraft", F.col("amount") > F.col("oldBalanceOrg")
      ).withColumn(
        # 6. CATEGORICAL CONSOLIDATION (Risk Profiling)
        # Segment transaction channels into risk buckets
        "txn_category_risk", F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), "HIGH_RISK")
                         .otherwise("LOW_RISK")
      )
# Apply the transformation
# Execute transformations and cleanup raw PII references
df_security = transform_to_silver(df_bronze)
df_security= advanced_silver_transform(df_security)
# Optional: Drop original PII columns if masking is successful
df_security = df_security.drop("nameOrig", "newDest")

# Reorder schema to place transaction primary key at column index 0
target_col = "transaction_sk"

# 2. Create a list of all OTHER columns (this removes it from the center)
other_cols = [c for c in df_security.columns if c != target_col]

# 3. Combine them: Target first, then the rest
ordered_cols = [target_col] + other_cols

# 4. Apply the selection
# Register temp view for SQL-based downstream queries
df_security = df_security.select(*ordered_cols)

df_security.createOrReplaceTempView("v_security_cleaned")

print("Notebook 01: Trasformations and Security Masking Complete. Data is now PII-Compliant and Transformed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
