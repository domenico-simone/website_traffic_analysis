from pyspark.sql.types import StructType, StringType, StructField, IntegerType

event_schema = StructType([
    StructField("timestamp", IntegerType(), True),
    StructField("event_type", IntegerType(), True),
    StructField("banner_id", StringType(), True),
    StructField("placement_id", StringType(), True),
    StructField("page_id", StringType(), True),
    StructField("user_id", StringType(), True),
])
