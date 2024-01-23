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

# # parse command line args
# parser = argparse.ArgumentParser(
#                 description='Compute stats for an event batch file.')

# parser.add_argument('-i', '--input-file', default="data/sample_data/sample_data_01.csv",
#                     help="Event batch input file (%(default)s)")

# args = parser.parse_args()

# # Check if input file exists, exit if not
# if not os.path.isfile(args.input_file):
#     logging.error(f"Input file {args.input_file} not found!") 
#     raise FileNotFoundError

spark = SparkSession.builder \
        .appName("WebtrafficStats_daily") \
        .master("local[2]").getOrCreate()

# Load data
df_all = spark.read.csv("data/sample_data/", header=True, inferSchema=True)

# Daily statistics for the requested grouping id
placement_stats = (
    # df.groupBy("Placement_id", F.hour(F.from_unixtime("Timestamp")).alias("hour"))
    df_all.groupBy("page_id")
    .agg(
        F.count(F.when(F.col("event_type") == 0, 1)).alias("n_views"),
        F.sum("event_type").alias("n_clicks"),
        F.countDistinct("user_id").alias("distinct_users")
    )
)

placement_stats.show()

spark.stop()
