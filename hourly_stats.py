import argparse
from datetime import datetime
import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from utils.schemas import event_schema, DbLogger
from utils.funcs import set_logging

# logging.basicConfig(
#     format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
#     level=logging.INFO,
# )
# logger = logging.getLogger(__name__)

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

    # set logging
    console_logger, db_logger = set_logging(log_file="data/logs/ad_stats_processing.log", overwrite_file_handler=False)

    input_file = args.input
    # Check if input file exists, exit if not
    if not os.path.isfile(input_file):
        console_logger.error(f"Input file {input_file} not found!")
        db_logger.error(DbLogger(status='ERROR', 
                                 message='Batch file not found', 
                                 timestamp=datetime.now().strftime("%Y-%m-%d"), 
                                 batch_type="daily", 
                                 datetime_log=datetime.now().strftime("%Y-%m-%d")))
        raise FileNotFoundError

    console_logger.info(f"Starting Spark session")
    spark = SparkSession.builder.appName("WebsiteTrafficStats_hourly").getOrCreate()

    # Load data
    df = spark.read.csv(input_file, header=True, schema=event_schema)

    # Hourly and daily statistics for each placement_id
    hourly_stats = get_hourly_stats(df, grouping_field=args.grouping_field)
    hourly_stats.show()

    spark.stop()