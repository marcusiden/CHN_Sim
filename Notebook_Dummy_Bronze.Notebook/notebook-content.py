# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bb4d4cae-c0c5-47f0-adde-472e7e14a105",
# META       "default_lakehouse_name": "LH_Test",
# META       "default_lakehouse_workspace_id": "dbf1b9ba-3e30-4af4-b189-5c59cce4666f",
# META       "known_lakehouses": [
# META         {
# META           "id": "bb4d4cae-c0c5-47f0-adde-472e7e14a105"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, expr, rand
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# NOTE:
# To persist this DataFrame, the notebook must have a default Lakehouse attached.
# Attach a Lakehouse in the notebook settings (left panel) if you haven't already.

# Parameters for dummy data
row_count = 100

# Example: create a simple DataFrame with id 0..row_count-1
base_df = spark.range(0, row_count).withColumnRenamed("id", "customer_id")

# Add some dummy columns
dummy_df = (
    base_df
    .withColumn("age", expr("20 + cast(rand()*30 as int)"))  # 20–49
    .withColumn("country", expr("CASE WHEN rand() < 0.5 THEN 'USA' ELSE 'UK' END"))
    .withColumn("spend", (rand() * 1000).cast(DoubleType()))  # 0–1000
)

display(dummy_df.limit(10))  # show a sample of the dummy data

# Write the dummy data directly to the attached Lakehouse Files area
target_files_path = "Files/customer_dummy_parquet"
dummy_df.write.mode("overwrite").format("delta").saveAsTable("bronze_customers")

print("Dummy data written to table 'bronze_customers'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
