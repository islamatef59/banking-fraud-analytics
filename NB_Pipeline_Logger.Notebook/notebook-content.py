# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
