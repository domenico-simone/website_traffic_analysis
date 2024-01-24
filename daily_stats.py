import argparse
import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from utils.schemas import event_schema, DbLogger
from utils.funcs import set_logging

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

def get_daily_stats(df_all, grouping_field):
    daily_stats = (
        df_all.groupBy(grouping_field)
        .agg(
            F.date_format(F.from_unixtime(F.any_value("timestamp")), "yyyy-MM-dd").alias('date'),
            F.count(F.when(F.col("event_type") == 0, 1)).alias("n_views"),
            F.sum("event_type").alias("n_clicks"),
            F.countDistinct("user_id").alias("distinct_users")
        )
    )

    return daily_stats

if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    description='Compute stats for an hourly event batch file.')

    parser.add_argument('-i', '--input-folder', default="data/sample_data/",
                        help="Folder with event batch input files (default: %(default)s)")
    parser.add_argument('-g', '--grouping-field', default="placement_id",
                        help="Event table field to compute statistics for (default: %(default)s)")

    args = parser.parse_args()
    input_folder = args.input_folder

    # set logging
    console_logger, db_logger = set_logging(log_file="data/logs/ad_stats_processing.log", overwrite_file_handler=False)

    spark = SparkSession.builder \
            .appName("WebtrafficStats_daily") \
            .master("local[2]").getOrCreate()

    # Load data
    df_all = spark.read.csv(input_folder, header=True, schema=event_schema)

    # Daily statistics for the requested grouping id
    daily_stats = get_daily_stats(df_all, args.grouping_field)

    daily_stats.show()

    spark.stop()
