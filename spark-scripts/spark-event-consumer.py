import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql.functions import window, from_unixtime, count, date_trunc, from_json, col, sum, avg
from pyspark.sql.types import *

from time import sleep


dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("DibimbingStreaming").setMaster(spark_host))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Define New Schema
new_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', IntegerType(), True),
    StructField('furniture', StringType(), True),
    StructField('color', StringType(), True),
    StructField('price', IntegerType(), True),
    StructField('ts', IntegerType(), True),
])

# Create Updated Dataframe using the Dataframe from Kafka
updated_df = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .withColumn("value",from_json(col("value"),new_schema))
    .select(col("value.*"))
)

# Add Regular Timestamp to the Dataframe
updated_df = (
    updated_df
    .withColumn("regular_timestamp", from_unixtime(updated_df["ts"]).cast("timestamp"))
)

# Add Hourly Timestamp to the Dataframe
updated_df = (
    updated_df
    .withColumn("hourly_timestamp", date_trunc("hour", col("regular_timestamp")))
)

# Test Print Dataframe (Worked)
# updated_df = (
#     updated_df
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )

# Test Group By Dataframe (Does not work)
# updated_df = (
#     updated_df
#     .groupBy("furniture").count()
#     .writeStream.format("console")
#     .outputMode("complete")
#     .start()
# )

# Test 2 Group By 
# test_count = (
#     updated_df
#     .groupBy("furniture")
#     .count()
# )

# test_query = (
#     test_count
#     .writeStream
#     .outputMode("complete")
#     .format("console")
#     .option("checkpointLocation", "checkpoint")
#     .start()
# )

# test_query.awaitTermination()

# Add aggregation values to Hourly Timestamp and Furniture Type
furniture_query = (
    updated_df
    .withWatermark("regular_timestamp", "1 hour")
    .groupBy(
        window(updated_df.regular_timestamp, "1 hour"),
        "furniture"
    )
    .agg(
        count("furniture").alias("furniture_sold"),
        sum("price").alias("total_revenue"),
        avg("price").alias("average_revenue")
    )
    .writeStream
    .format("console")
    .option("checkpointLocation", "checkpoint/checkpoint_dir")
    .outputMode("complete")
    .start()
    .awaitTermination()
)



# Default Example
# (
#     stream_df.selectExpr("CAST(value AS STRING)")
#     .writeStream.format("console")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )
