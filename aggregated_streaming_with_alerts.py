from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, from_json, lit, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from configs import kafka_config
import os

# Пакет для роботи з Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("StreamingWithAlerts")
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

# Агрегування даних
aggregated_df = (deserialized_df
                 .withWatermark("timestamp", "30 seconds")  # Ensure late data is handled
                 .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
                 .agg(
                     avg("temperature").alias("avg_temperature"),
                     avg("humidity").alias("avg_humidity")
                 ))

# Зчитування параметрів алертів із CSV
alerts_conditions_path = "alerts_conditions.csv"
alerts_df = spark.read.option("header", "true").csv(alerts_conditions_path)

# Приведення даних у CSV до числового формату
alerts_df = (alerts_df.withColumn("humidity_min", col("humidity_min").cast("double"))
                     .withColumn("humidity_max", col("humidity_max").cast("double"))
                     .withColumn("temperature_min", col("temperature_min").cast("double"))
                     .withColumn("temperature_max", col("temperature_max").cast("double")))

# Визначення алертів
alerts_with_data = (aggregated_df.crossJoin(alerts_df)
                    .filter(((col("avg_temperature") >= col("temperature_min")) & (col("avg_temperature") <= col("temperature_max"))) |
                            ((col("avg_humidity") >= col("humidity_min")) & (col("avg_humidity") <= col("humidity_max")))))

# Підготовка алертів для Kafka
generate_alerts = (alerts_with_data.select(
    to_json(struct(
        struct(col("window.start").alias("start"), col("window.end").alias("end")).alias("window"),
        col("avg_temperature"),
        col("avg_humidity"),
        col("code"),
        col("message"),
        current_timestamp().alias("timestamp")
    )).alias("value")
))

# Запис алертів у Kafka
generate_alerts.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("topic", "oholodetskyi_alerts") \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("checkpointLocation", "alerts_checkpoint") \
    .start() \
    .awaitTermination()
