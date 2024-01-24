import argparse
import logging
import os
import pyspark.sql.functions as F
import traceback
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

from utils.schemas import event_schema, DbLogger, datetime_log_format_daily, datetime_log_format_hourly
from utils.funcs import set_logging

# logging.basicConfig(
#     format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
#     level=logging.INFO,
# )
# logger = logging.getLogger(__name__)

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

def check_files_for_daily_stats(batch_folder: str,
                                date: datetime):
    """Check if 24 files are available for daily reporting. If not, will send a warning log.

    :param batch_folder: folder with batch files, defaults to None
    :type batch_folder: str
    :param date: date for which the report has to be created, defaults to yesterday since the daily report
      is supposed to run the day after data has been generated
    :type date: datetime
    """
    n_files = len(os.listdir(batch_folder))
    if n_files != 24:
        console_logger.warning(f"Batch file folder contains {n_files} files instead of 24. Daily report could be incomplete.")
        db_logger.warning(DbLogger(status='WARNING', 
                                 message=f'{n_files}/24 batch files found',
                                 # timestamp is when the command is run
                                 timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                                 batch_type="daily",
                                 # datetime_log is the date of the events
                                 datetime_log=date.strftime(datetime_log_format_daily)))
        
if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser(
                    description='Compute stats for a daily equivalent of batch files.')

    parser.add_argument('-i', '--input-folder', default="data/sample_data/",
                        help="Folder with event batch input files (default: %(default)s)")
    parser.add_argument('-g', '--grouping-field', default="placement_id",
                        help="Event table field to compute statistics for (default: %(default)s)")
    parser.add_argument('-t', '--log-timestamp', default="today",
                        choices=["yesterday", "today", "tomorrow"],
                        help="Date to use in logging (for testing purposes). Timezone: UTC")

    args = parser.parse_args()
    input_folder = args.input_folder

    # set logging
    console_logger, db_logger = set_logging(log_file="data/logs/ad_stats_processing.log", overwrite_file_handler=False)

    # check if number of files is the expected
    if args.log_timestamp == "today":
        date = datetime.utcnow()
    elif args.log_timestamp == "tomorrow":
        date = datetime.utcnow() + timedelta(days=1)
    else:
        date = datetime.utcnow() - timedelta(days=1)
    
    check_files_for_daily_stats(batch_folder=input_folder, date=date) 

    spark = SparkSession.builder \
            .appName("WebtrafficStats_daily") \
            .master("local[2]").getOrCreate()

    # Daily statistics for the requested grouping id
    try:
        # Load data
        df_all = spark.read.csv(input_folder, header=True, schema=event_schema)
        # raise Exception
        daily_stats = get_daily_stats(df_all, args.grouping_field)
        log_date = daily_stats.select("date").first()["date"]
        console_logger.info(f"Daily report for {log_date}: DONE")
        db_logger.info(DbLogger(status='SUCCESS', 
                                 message=f'Daily report for {log_date}: DONE',
                                 # timestamp is when the command is run
                                 timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                                 batch_type="daily",
                                 # datetime_log is the date of the events
                                 datetime_log=date.strftime(datetime_log_format_daily)))
        daily_stats.show()
    except Exception as e:
        traceback_str = traceback.format_exc()
        db_logger.info(DbLogger(status='ERROR', 
                            message=f'Daily report for {date}: failed with traceback:\n{traceback_str}',
                            # timestamp is when the command is run
                            timestamp=datetime.now().strftime(datetime_log_format_hourly), 
                            batch_type="daily",
                            # datetime_log is the date of the events
                            datetime_log=date.strftime(datetime_log_format_daily)))

        raise RuntimeError(f"An error occurred: {e}\nTraceback:\n{traceback_str}")

    spark.stop()
