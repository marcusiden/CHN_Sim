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

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, upper, current_timestamp

# --- Silver layer: read bronze, clean, enrich, write ---

# 1. Read the bronze table (resolves to the attached lakehouse)
bronze_df = spark.read.table("bronze_customers")

print(f"Bronze row count: {bronze_df.count()}")

# 2. Transformations
silver_df = (
    bronze_df
    # Standardise country codes to uppercase (defensive, in case of mixed casing)
    .withColumn("country", upper(col("country")))
    # Derive an age band for downstream analytics
    .withColumn(
        "age_band",
        when(col("age") < 30, "18-29")
        .when(col("age") < 45, "30-44")
        .otherwise("45+")
    )
    # Flag high-value customers (business rule example)
    .withColumn("is_high_value", col("spend") > 500)
    # Round spend to 2 decimals for reporting cleanliness
    .withColumn("spend", col("spend").cast("decimal(10,2)"))
    # Add a processing timestamp for lineage
    .withColumn("silver_loaded_at", current_timestamp())
    # Drop any rows with a null country (basic data-quality gate)
    .filter(col("country").isNotNull())
)

# 3. Write to the silver table (overwrite for idempotent re-runs)
silver_df.write.mode("overwrite").format("delta").saveAsTable("silver_customers")

print(f"Silver row count: {silver_df.count()}")
display(silver_df.limit(10))


# flow test 2 - edited from VS Cod
#rep 1
#rep 2
#rep 3
#rep 4
#rep 5

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
