from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from configs import kafka_config
import os

# Пакет для роботи з Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("AggregatedStreaming")
         .master("local[*]")
         .getOrCreate())

# Схема для даних із Kafka
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),  # Timestamp as epoch time
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Читання потоку даних із Kafka
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
      .option("subscribe", "oholodetskyi_building_sensors")
      .option("startingOffsets", "earliest")
      .option("kafka.security.protocol", kafka_config['security_protocol'])
      .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
      .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
      .load())

# Десеріалізація даних із Kafka
deserialized_df = (df.selectExpr("CAST(value AS STRING) as json_data")
                   .select(from_json(col("json_data"), schema).alias("data"))
                   .select("data.*")
                   .withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp")))  # Convert epoch to timestamp

# Додавання Watermark і виконання обчислень
aggregated_df = (deserialized_df
                 .withWatermark("timestamp", "30 seconds")  # Ensure late data is handled
                 .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
                 .agg(
                     avg("temperature").alias("avg_temperature"),
                     avg("humidity").alias("avg_humidity")
                 ))

# Виведення результатів обчислення
print("=== Результати агрегування ===")
query = (aggregated_df.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
