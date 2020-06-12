import pyspark.sql.functions as F
from pyspark.sql.functions import struct
from core.config import SERVERS


def process_task_2(df):
    records = df \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
        F.window("timestamp", "1 minute", "1 minute")
    ).agg(
        struct(
            F.month('window.end').alias('month'),
            F.dayofmonth('window.end').alias('day_of_the_month'),
            F.hour('window.end').alias('hour'),
            F.minute('window.end').alias("minute"),
            F.collect_list('group_city').alias('cities')
        ).alias("result")
    ).select(F.to_json("result").alias("value")).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(SERVERS)) \
        .option("topic", "US-cities-every-minute")

    return records
