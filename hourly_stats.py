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

def get_hourly_stats(df, grouping_field):
    hourly_stats = (
        df.groupBy(grouping_field)
        .agg(
            F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd'T'HH:00:00").alias('start_time_utc'),
            F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd'T'HH:59:59").alias('end_time_utc'),
            F.count(F.when(F.col("event_type") == 0, 1)).alias("views"),
            F.sum("event_type").alias("clicks"),
            F.countDistinct("user_id").alias("distinct_users")
        )
    )

    return hourly_stats


if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    description='Compute stats for an hourly event batch file.')

    parser.add_argument('-i', '--input', default="data/sample_data/sample_data_01.csv",
                        help="Event batch input file (default: %(default)s)")
    parser.add_argument('-g', '--grouping-field', default="placement_id",
                        help="Event table field to compute statistics for (default: %(default)s)")

    args = parser.parse_args()
    input_file = args.input
    # Check if input file exists, exit if not
    if not os.path.isfile(input_file):
        logging.error(f"Input file {input_file} not found!") 
        raise FileNotFoundError

    spark = SparkSession.builder.appName("WebsiteTrafficStats_hourly").getOrCreate()

    # Load data
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Hourly and daily statistics for each placement_id
    hourly_stats = get_hourly_stats(df, grouping_field=args.grouping_field)
    hourly_stats.show()

    spark.stop()