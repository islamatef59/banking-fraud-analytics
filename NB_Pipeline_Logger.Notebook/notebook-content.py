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

# PARAMETERS CELL ********************

# 1. Simply define the variable with a default value
p_status_message = "No message received"

# 2. Use the variable directly (no .get() needed)
print(f"PIPELINE STATUS: {p_status_message}")

# 3. Your Pro Tip logic remains the same
# spark.sql(f"INSERT INTO audit_logs VALUES (current_timestamp(), '{p_status_message}')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
