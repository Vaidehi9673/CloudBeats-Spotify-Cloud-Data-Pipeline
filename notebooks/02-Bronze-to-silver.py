# Databricks notebook source
# MAGIC %md
# MAGIC 1. store the bronze layer to dataframe

# COMMAND ----------

# 1. Read from the Bronze Delta Table (Fast & Reliable)
bronze_df = spark.read.format("delta").load("s3://spotify-cloudbeat-analytics/bronze/spotify_raw_data")

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Performing cleaning process

# COMMAND ----------

from pyspark.sql.functions import col, lower, round, when, current_timestamp

silver_df = (bronze_df
    .select(
        col("track_id"),
        col("track_name"),
        col("artist_name"),
        # 1. Clean Text: Lowercase the genre for consistent filtering
        lower(col("genre")).alias("genre"),
        # 2. Date Cast: Ensure release_date is a proper Date type
        col("release_date").cast("date"),
        # 3. Unit Conversion: Convert milliseconds to minutes
        round(col("duration_ms") / 60000, 2).alias("duration_mins"),
        col("popularity"),
        col("stream_count"),
        # 4. Business Logic: Categorize Energy levels
        when(col("energy") > 0.8, "High Energy")
        .when(col("energy") > 0.5, "Medium Energy")
        .otherwise("Low Energy").alias("energy_vibe"),
        # 5. Metadata: Track when this cleaning happened
        current_timestamp().alias("processed_at")
    )
    .filter(col("track_id").isNotNull()) # Data Quality: Remove rows with no ID
)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Write to the Silver folder in S3 as Delta

# COMMAND ----------

(silver_df.write
 .format("delta")
 .mode("overwrite")
 .save("s3://spotify-cloudbeat-analytics/silver/spotify_processed_data"))