import argparse
from datetime import datetime
import logging
import traceback
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from utils.schemas import event_schema, DbLogger, datetime_log_format_hourly, datetime_log_format_filename
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

    parser.add_argument('-i', '--input', required=True,
                        help="Event batch input file (default: %(default)s)")
    parser.add_argument('-g', '--grouping-field', default="placement_id",
                        help="Event table field to compute statistics for (default: %(default)s)")
    parser.add_argument('-t', '--log-timestamp', default="today",
                        choices=["yesterday", "today", "tomorrow"],
                        help="Date to use in logging (for testing purposes). Timezone: UTC")

    args = parser.parse_args()

    # set logging
    console_logger, db_logger = set_logging(log_file="data/logs/ad_stats_processing.log", overwrite_file_handler=False)

    input_file = args.input
    # assume file name template is YYYY-MM-DD_hh-mm-ss_sample_data.csv
    date_time_string = 'T'.join(os.path.basename(input_file).split("_")[:2])
    date_time = datetime.strptime(date_time_string, datetime_log_format_filename).strftime(datetime_log_format_hourly)
    # Check if input file exists, exit if not
    if not os.path.isfile(input_file):
        console_logger.error(f"Input file {input_file} not found!")
        db_logger.error(DbLogger(status='ERROR', 
                                 message=f'Batch file {input_file} not found', 
                                 timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                                 batch_type="hourly", 
                                 datetime_log=date_time))
        raise FileNotFoundError(input_file)

    console_logger.info(f"Starting Spark session")
    try:
        spark = SparkSession.builder.appName("WebsiteTrafficStats_hourly").getOrCreate()

        # Load data
        df = spark.read.csv(input_file, header=True, schema=event_schema)

        # Hourly statistics for each element in grouping_id
        hourly_stats = get_hourly_stats(df, grouping_field=args.grouping_field)
        db_logger.info(DbLogger(status='SUCCESS', 
                            message=f'Hourly report for {date_time_string}: DONE',
                            # timestamp is when the command is run
                            timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                            batch_type="hourly",
                            # datetime_log is the date of the event
                            datetime_log=date_time))
        
        # write stats to file
        # create folder if it doesn't exist
        out_folder_stats = f"data/stats/hourly/{args.grouping_field}"
        out_file = os.path.join(out_folder_stats, f"{os.path.basename(input_file)}_stats.csv")
        os.makedirs(out_folder_stats, exist_ok=True)
        hourly_stats.coalesce(1).write.csv(out_file, header=True, mode="overwrite")
        console_logger.info(f"Hourly report for {date_time_string} written to {out_file}")
        hourly_stats.show()
    except Exception as e:
        traceback_str = traceback.format_exc()
        db_logger.info(DbLogger(status='ERROR', 
                            message=f'Hourly report for {date_time_string}: failed with traceback:\n{traceback_str}',
                            # timestamp is when the command is run
                            timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                            batch_type="hourly",
                            # datetime_log is the date of the event
                            datetime_log=date_time))
                            # datetime_log=date_time_string.strftime("%Y-%m-%d")))

        raise RuntimeError(f"An error occurred: {e}\nTraceback:\n{traceback_str}")

    spark.stop()