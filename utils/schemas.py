import json
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

event_schema = StructType([
    StructField("timestamp", IntegerType(), True),
    StructField("event_type", IntegerType(), True),
    StructField("banner_id", StringType(), True),
    StructField("placement_id", StringType(), True),
    StructField("page_id", StringType(), True),
    StructField("user_id", StringType(), True),
])

class DbLogger:
    def __init__(self, status: str, message: str, log_timestamp: str, 
                 batch_type: str, grouping_id: str, batch_timestamp: str):
        self.status          = status
        self.message         = message
        # log_timestamp is when this log is generated
        self.log_timestamp   = log_timestamp
        self.batch_type      = batch_type
        self.grouping_id     = grouping_id
        # batch_timestamp is the date of the event batch
        self.batch_timestamp = batch_timestamp
    
    def __str__(self):
        return json.dumps(vars(self))

datetime_log_format_filename = "%Y-%m-%dT%H-%M-%S"
datetime_log_format_hourly   = "%Y-%m-%dT%H:%M:%S"
datetime_log_format_daily    = "%Y-%m-%dT00:00:00"