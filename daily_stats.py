import argparse
import logging
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from utils.schemas import event_schema

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
    # set up the logger for printing to the screen (console)
    console_logger = logging.getLogger('console_logger')
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(funcName)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    console_logger.addHandler(console_handler)
    console_logger.setLevel(logging.INFO)

    # set up the logger for producing logs to a database
    # create log folder if it does not exist
    os.makedirs("data/logs", exist_ok=True)
    db_logger = logging.getLogger('db_logger')
    db_handler = logging.FileHandler('data/logs/hourly.log')
    db_logger.addHandler(db_handler)
    db_logger.setLevel(logging.INFO)
    
    spark = SparkSession.builder \
            .appName("WebtrafficStats_daily") \
            .master("local[2]").getOrCreate()

    # Load data
    df_all = spark.read.csv(input_folder, header=True, schema=event_schema)

    # Daily statistics for the requested grouping id
    daily_stats = get_daily_stats(df_all, args.grouping_field)

    daily_stats.show()

    spark.stop()
