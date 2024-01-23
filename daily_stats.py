import argparse
import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
        .appName("WebtrafficStats_daily") \
        .master("local[2]").getOrCreate()

# Load data
df_all = spark.read.csv("data/sample_data/", header=True, inferSchema=True)

# Daily statistics for the requested grouping id
daily_stats = (
    df_all.groupBy("page_id")
    .agg(
        F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd").alias('date'),
        F.count(F.when(F.col("event_type") == 0, 1)).alias("n_views"),
        F.sum("event_type").alias("n_clicks"),
        F.countDistinct("user_id").alias("distinct_users")
    )
)

daily_stats.show()

spark.stop()
