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

# parse command line args
parser = argparse.ArgumentParser(
                description='Compute stats for an event batch file.')

parser.add_argument('-i', '--input-file', default="data/sample_data/sample_data_01.csv",
                    help="Event batch input file (%(default)s)")

args = parser.parse_args()

# Check if input file exists, exit if not
if not os.path.isfile(args.input_file):
    logging.error(f"Input file {args.input_file} not found!") 
    raise FileNotFoundError

spark = SparkSession.builder.appName("WebsiteTrafficStats").getOrCreate()

# Load data
df = spark.read.csv(args.input_file, header=True, inferSchema=True)

# Hourly and daily statistics for each placement_id
placement_stats = (
    # df.groupBy("Placement_id", F.hour(F.from_unixtime("Timestamp")).alias("hour"))
    df.groupBy("placement_id")
    .agg(
        F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd'T'HH:00:00").alias('start_time_utc'),
        F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd'T'HH:59:59").alias('end_time_utc'),
        F.count(F.when(F.col("event_type") == 0, 1)).alias("views"),
        F.sum("event_type").alias("clicks"),
        F.countDistinct("user_id").alias("distinct_users")
    )
)

placement_stats.show()

spark.stop()