# Databricks notebook source
# MAGIC %md
# MAGIC 1. first create a burner cell and place aws key and credentials and then delete it so it will be stored into secret vault.
# MAGIC 2. but can't use vault in free edition so using the spark conf and then before pushing to git we will remove this code
# MAGIC 3. but in catalog explorer created location and then connected databricks directly with aws using personal aws key

# COMMAND ----------

# Use the direct S3 path since it's now an External Location
display(dbutils.fs.ls("s3://spotify-cloudbeat-analytics/landing/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Create Bronze layer (Raw to Delta)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 1. Read the CSV from the Landing Zone
# We use header=True because your Spotify CSV has column names
df_raw = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("s3://spotify-cloudbeat-analytics/landing/spotify_data.csv"))

# 2. Add Ingestion Metadata (Industry standard for tracking)
df_bronze = df_raw.withColumn("ingested_at", current_timestamp())

# 3. Write to the Bronze folder in S3 as a Delta Table
# Using .mode("overwrite") allows you to re-run this while testing
(df_bronze.write
 .format("delta")
 .mode("overwrite")
 .save("s3://spotify-cloudbeat-analytics/bronze/spotify_raw_data"))

print("Bronze Layer successfully created!")

# COMMAND ----------

# MAGIC %md
# MAGIC so here we have stored raw data to bronze layer in delta format