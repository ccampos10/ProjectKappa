from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, FloatType, LongType
import logging

logging.getLogger("py4j").setLevel(logging.WARN)
logging.getLogger("pyspark").setLevel(logging.WARN)

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType() \
    .add('pc_id', StringType()) \
    .add('cpu_usage', FloatType()) \
    .add('ram_usage', FloatType()) \
    .add('packets_per_second', FloatType()) \
    .add('time', LongType()) \

spark = SparkSession.builder \
    .appName("TestSpark") \
    .getOrCreate()

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos") \
    .option("startingOffsets", "latest") \
    .load()

df_json = df_raw.selectExpr("CAST(value AS STRING) as json_str")
df_parsed = df_json.select(from_json(col('json_str'), schema).alias('data')).select('data.*')

query = df_parsed.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

query.awaitTermination()
