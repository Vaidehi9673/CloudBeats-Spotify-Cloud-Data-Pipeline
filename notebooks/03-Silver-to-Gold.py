# Databricks notebook source
# MAGIC %md
# MAGIC 1. Since Silver data is already cleaned and saved as a Delta table, reading it is incredibly fast.

# COMMAND ----------

# Read the refined Silver data from S3
silver_df = spark.read.format("delta").load("s3://spotify-cloudbeat-analytics/silver/spotify_processed_data")

silver_df.printSchema()
display(silver_df.limit(10))

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Transformation 1 — The "Artist Leaderboard"

# COMMAND ----------

# MAGIC %md
# MAGIC Identify which artists are driving the most traffic. We will use a Window Function to rank them.

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, desc, rank
from pyspark.sql.window import Window

artist_metrics = (silver_df
    .groupBy("artist_name", "genre")
    .agg(
        sum("stream_count").alias("total_streams"),
        avg("popularity").alias("avg_popularity"),
        count("track_id").alias("total_tracks")
    )
)
# Rank artists by stream count within their genre
window_spec = Window.partitionBy("genre").orderBy(desc("total_streams"))
gold_artist_rankings = artist_metrics.withColumn("rank", rank().over(window_spec))

# Save to Gold Layer
(gold_artist_rankings.write.format("delta").mode("overwrite")
 .save("s3://spotify-cloudbeat-analytics/gold/artist_rankings"))

# COMMAND ----------

gold_artist_rankings.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Transformation 2 — "Global Vibe Trends"

# COMMAND ----------

# MAGIC %md
# MAGIC This identifies which countries prefer "High Energy" music vs "Chill" music. This is exactly what Spotify uses to recommend playlists by region.

# COMMAND ----------

from pyspark.sql.functions import col, sum, avg, count, desc, rank

# Calculate average music characteristics per country
country_vibe_metrics = (silver_df
    .groupBy("country")
    .agg(
        avg("danceability").alias("avg_danceability"),
        avg("stream_count").alias("stream_count"),
        count("track_id").alias("song_count")
    )
    .orderBy(desc("stream_count"))
)

# Save to Gold Layer
(country_vibe_metrics.write.format("delta").mode("overwrite")
 .save("s3://spotify-cloudbeat-analytics/gold/country_vibe_metrics"))

# COMMAND ----------

country_vibe_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation 3 — "Label Performance"

# COMMAND ----------

# MAGIC %md
# MAGIC This table is for the "Business Side." It tells us which record label (Universal, Sony, etc.) has the most "clean" vs "explicit" tracks and their market share.

# COMMAND ----------

label_metrics = (silver_df
    .groupBy("label")
    .agg(
        sum("stream_count").alias("total_streams"),
        avg("explicit").alias("explicit_ratio"),
        count("track_id").alias("total_catalog_size")
    )
    .orderBy(desc("total_streams"))
)

# Save to Gold Layer
(label_metrics.write.format("delta").mode("overwrite")
 .save("s3://spotify-cloudbeat-analytics/gold/label_performance"))

# COMMAND ----------

label_metrics.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Validation checks

# COMMAND ----------

# Check if the Top Artist in the ranking table has more than 0 streams
top_artist_streams = gold_artist_rankings.orderBy("rank").select("total_streams").first()[0]

if top_artist_streams > 0:
    print(f"Gold Validation Passed: Top artist has {top_artist_streams} streams.")
else:
    raise Exception("Gold Validation Failed: Aggregations resulted in zero streams for top artist.")