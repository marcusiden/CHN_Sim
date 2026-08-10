# Fabric notebook source


# CELL ********************

# MAGIC %%configure
# MAGIC 
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": {
# MAGIC             "variableName": "$(/**/Variables_Test/Lakehouse_Name)"
# MAGIC         }
# MAGIC     }
# MAGIC }

# CELL ********************

from pyspark.sql.functions import col, when, upper, current_timestamp


import notebookutils

from pyspark.sql.functions import lit


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


vl = notebookutils.variableLibrary.getLibrary("Variables_Test")
env = vl.Environment_Name
print(f"Kjører i miljø: {env}")

# skriv den inn i dataene så du ser det i tabellen etterpå
silver_df = silver_df.withColumn("env_stamp", lit(env))


# 3. Write to the silver table (overwrite for idempotent re-runs)
silver_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("silver_customers")

print(f"Silver row count: {silver_df.count()}")
display(silver_df.limit(10))


# flow test 2 - edited from VS Cod
#rep 1
#rep 2
#rep 3
#rep 4
#rep 5
#test


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
