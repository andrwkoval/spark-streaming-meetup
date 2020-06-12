import pyspark.sql.functions as F
from pyspark.sql.functions import struct, col
from core.config import SERVERS

def process_task_1(df):
    records = df.select(
        struct(
            struct(
                col("event_name"),
                col("event_id"),
                col("time")
            ).alias("event"),
            col("group_city"),
            col("group_country"),
            col("group_id"),
            col("group_name"),
            col("name").alias("group_state")
        ).alias("result")
    ).select(F.to_json("result").alias("value")).writeStream \
        .format("kafka")\
        .option("kafka.bootstrap.servers", ",".join(SERVERS)) \
        .option("topic", "US-meetups")

    return records
