import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PlacementStats").getOrCreate()

# Load data
df = spark.read.csv("data/sample_data/sample_data_01.csv", header=True, inferSchema=True)

# Hourly and daily statistics for each placement_id
placement_stats = (
    # df.groupBy("Placement_id", F.hour(F.from_unixtime("Timestamp")).alias("hour"))
    df.groupBy("Placement_id")
    .agg(
        F.count(F.when(F.col("Event_type") == 0, 1)).alias("views"),
        F.sum("Event_type").alias("clicks"),
        F.countDistinct("User_id").alias("distinct_users")
    )
)

placement_stats.show()
