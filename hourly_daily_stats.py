from datetime import datetime, timedelta
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import countDistinct, sum, col
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import time
import yaml

from utils.generate_sample_data import generate_sample_data_hourly, generate_user_id_list
from utils.funcs import parse_conf

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# parse configuration file
# if needed values are not provided in conf.yaml,
# they will be parsed from utils/defaults.yaml
conf = parse_conf('conf.yaml')

# set an argparse here to set number of days to simulate
# so far we simulate 3 days starting from current time
n_days = conf['n_days']
hour_range = range(1, n_days*24+1)
start_time = datetime.utcnow()

# Initialize Spark session
spark = SparkSession.builder.appName("AdStatsProcessingByPlacement").getOrCreate()

### Set up schemas
hourly_stats_schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    # users to be added
    StructField("distinct_users", IntegerType(), True),
])

daily_df_schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("views", IntegerType(), True),
    StructField("clicks", IntegerType(), True),
    # users to be added
    # StructField("distinct_users", IntegerType(), True),
])

# accumulate users for placements,
# will be added to each daily report
mid_page_id_users_schema = StructType([
    StructField("page_id", StringType(), True),
    StructField("user_id", StringType(), True),
])

###

# cumulative_stats_df = spark.createDataFrame([], schema=hourly_stats_schema)

# generate list of users to use through the whole process
# use default value of 20000
user_id_list = generate_user_id_list()

daily_df = spark.createDataFrame([], schema=daily_df_schema)
mid_page_id_users = spark.createDataFrame([], schema=mid_page_id_users_schema)

# Loop through the hours range and compute hourly stats
for hour in hour_range:

    # generate current_df with generate_sample_data function
    current_df = spark.createDataFrame(generate_sample_data_hourly(users=user_id_list,
                                                                   start_time=start_time))
    # logging.info(current_df.show(truncate=10))
    # Hourly and daily statistics for each page_id
    hourly_stats_df = (
        # df.groupBy("page_id", F.hour(F.from_unixtime("Timestamp")).alias("hour"))
        current_df.groupBy("page_id")
        .agg(
            F.count(F.when(F.col("event_type") == 0, 1)).alias("views"),
            F.sum("event_type").alias("clicks"),
            F.countDistinct("user_id").alias("distinct_users")
        )
        .sort("page_id")
    )

    # append page_id and user_id to mid_page_id_users
    mid_page_id_users = mid_page_id_users.union(current_df.select("page_id", "user_id"))
    # logging.info(mid_page_id_users.show(truncate=10))

    # show hourly stats
    logging.info(f"Hourly stats for time range: {(start_time-timedelta(hours=1)).strftime('%Y-%m-%d, %H:%M:%S')} - {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")
    hourly_stats_df.show()

    # accumulate statistics to the daily DataFrame
    daily_df = daily_df.union(hourly_stats_df.drop(col("distinct_users")))

    # show daily stats every 24 hours
    if hour % 24 == 0:
        # generate daily user count per placement
        mid_page_id_users_count = (
            mid_page_id_users.groupBy("page_id")
            .agg(
                F.countDistinct("user_id").alias("distinct_users")
            )
        )
        logging.info(f"Daily stats for time range: {(start_time-timedelta(hours=24)).strftime('%Y-%m-%d, %H:%M:%S')} - {start_time.strftime('%Y-%m-%d, %H:%M:%S')}")
        daily_stats_df = (
            # df.groupBy("page_id", F.hour(F.from_unixtime("Timestamp")).alias("hour"))
            daily_df.groupBy("page_id")
            .agg(
                F.sum("views").alias("views"),
                F.sum("clicks").alias("clicks"),
                # F.countDistinct("user_id").alias("distinct_users")
            )
            # .join(mid_page_id_users_count, col("page_id") == mid_page_id_users_count.page_id)
            .join(mid_page_id_users_count, on = "page_id", how = "left")
            .sort("page_id")
        )
        daily_stats_df.show()

        # reset dataframes to accumulate intermediate data
        daily_df = spark.createDataFrame([], schema=daily_df_schema)
        mid_page_id_users = spark.createDataFrame([], schema=mid_page_id_users_schema)

    
    # cumulative_stats_df = cumulative_stats_df.union(hourly_stats_df)

    # Set new time to generate stats
    start_time = start_time + timedelta(hours=1)

    time.sleep(1)

# # Compute cumulative statistics
# final_cumulative_stats = (
#     cumulative_stats_df.groupBy("column_name")  # Replace with your grouping column
#     .agg(
#         sum("distinct_users").alias("cumulative_distinct_users"),
#         sum("views").alias("cumulative_views"),
#         # Add other cumulative statistics as needed
#     )
# )

# final_cumulative_stats.show()
