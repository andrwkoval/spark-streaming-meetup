import pyspark.sql.functions as F
from pyspark.sql.functions import struct, col
from core.config import SERVERS


def process_task_3(df):
    topics = F.array([F.lit("Big Data"), F.lit("Python"), F.lit("Machine Learning")])

    records = df.select(
        struct(
            struct(
                col("event_name"),
                col("event_id"),
                col("time")
            ).alias("event"),
            col("topic_name").alias("group_topics"),
            col("group_city"),
            col("group_country"),
            col("group_id"),
            col("group_name"),
            col("name").alias("group_state")
        ).alias("result")
    ).filter(F.arrays_overlap("result.group_topics", topics)) \
        .select(F.to_json("result").alias("value")).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(SERVERS)) \
        .option("topic", "Programming-meetups")

    return records
