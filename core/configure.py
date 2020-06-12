import findspark
import os
import pyspark
from auxiliary.preprocess import preprocess_data

from tasks.task_1 import process_task_1
from tasks.task_2 import process_task_2
from tasks.task_3 import process_task_3

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'

config = pyspark.SparkConf().setAll([('spark.executor.memory', '1000m'),
                                     ('spark.executor.cores', '1')])
spark = pyspark.sql.SparkSession.builder.config(conf=config).master("spark:/<ip>:7077").getOrCreate()

df = preprocess_data(spark)

process_task_1(df).start()
process_task_2(df).start()
process_task_3(df).start()
