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
    def __init__(self, status, message, timestamp, batch_type, datetime_log):
        self.status = status
        self.message = message
        self.timestamp = timestamp
        self.batch_type = batch_type
        self.datetime = datetime_log
    
    def __str__(self):
        return json.dumps(vars(self))