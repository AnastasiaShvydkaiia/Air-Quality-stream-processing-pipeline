from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,  DoubleType,TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/aq_db")
KAFKA_TOPIC = "sensor-data"
WINDOW_SIZE = "5 minutes"
EXPECTED_SAMPLES = 20  # expected number of measurements per window: 1 measurement every 15 seconds
THRESHOLD = 0.75 # completeness threshold

spark = SparkSession.builder \
    .appName("Air Quality Streaming") \
    .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-10.3.0.jar") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .getOrCreate()
    
# JSON schema for incoming Kafka messages
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("PM10_ug_m3", DoubleType(), True),
    StructField("PM2_5_ug_m3", DoubleType(), True),
    StructField("bme_pressure", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("CO_ppm", DoubleType(), True),
    StructField("NO2_ppm", DoubleType(), True),
    StructField("O3_ppb", DoubleType(), True)
])

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
df_parsed = df.selectExpr("CAST(value AS STRING) AS json_value") \
    .select(F.from_json("json_value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))

# Write raw data to Mongo
raw_query=df_parsed.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/raw_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("database", "aq_db") \
    .option("collection", "raw_collection") \
    .outputMode("append") \
    .start()

# Clean data
clean = lambda c: F.when(F.isnan(F.col(c)) | F.col(c).isNull(), None).otherwise(F.col(c))

df_clean = df_parsed.select(
    "event_time",
    clean("PM10_ug_m3").alias("PM10_ug_m3"),
    clean("PM2_5_ug_m3").alias("PM2_5_ug_m3"),
    clean("bme_pressure").alias("bme_pressure"),
    clean("temperature").alias("temperature"),
    clean("humidity").alias("humidity"),
    clean("CO_ppm").alias("CO_ppm"),
    clean("NO2_ppm").alias("NO2_ppm"),
    clean("O3_ppb").alias("O3_ppb")
)

# Aggregate data
valid = lambda c: F.when(F.col(c).isNotNull(), 1).otherwise(0)

df_agg = df_clean \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(F.window("event_time", WINDOW_SIZE)) \
    .agg(
        F.avg("PM2_5_ug_m3").alias("PM2_5_mean"),
        F.sum(valid("PM2_5_ug_m3")).alias("PM2_5_count"),

        F.avg("PM10_ug_m3").alias("PM10_mean"),
        F.sum(valid("PM10_ug_m3")).alias("PM10_count"),

        F.avg("CO_ppm").alias("CO_ppm_mean"),
        F.sum(valid("CO_ppm")).alias("CO_count"),

        F.avg("NO2_ppm").alias("NO2_ppm_mean"),
        F.sum(valid("NO2_ppm")).alias("NO2_count"),

        (F.avg("O3_ppb") / 1000).alias("O3_ppm_mean"),
        F.sum(valid("O3_ppb")).alias("O3_count"),

        F.avg("temperature").alias("temp_mean"),
        F.avg("humidity").alias("humidity_mean"),
        F.avg("bme_pressure").alias("pressure_mean")
    )

# Breakpoints for AQI
pm25_breakpoints = [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
                    (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400),
                    (350.5, 500.4, 401, 500)]

pm10_breakpoints = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
                    (255, 354, 151, 200), (355, 424, 201, 300), (425, 504, 301, 400),
                    (505, 604, 401, 500)]

co_breakpoints = [(0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
                  (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 40.4, 301, 400),
                  (40.5, 50.4, 401, 500)]

no2_breakpoints = [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
                   (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 1649, 301, 400),
                   (1650, 2049, 401, 500)]

o3_breakpoints = [(0.000, 0.054, 0, 50), (0.055, 0.070, 51, 100), (0.071, 0.085, 101, 150),
                  (0.086, 0.105, 151, 200), (0.106, 0.200, 201, 300)]

# UDF for AQI
def compute_aqi(value, breakpoints):
    if value is None:
        return None
    for (C_low, C_high, I_low, I_high) in breakpoints:
        if C_low <= value <= C_high:
            return ((I_high - I_low) / (C_high - C_low)) * (value - C_low) + I_low
    return None

udf_aqi_pm25 = F.udf(lambda x: compute_aqi(x, pm25_breakpoints), DoubleType())
udf_aqi_pm10 = F.udf(lambda x: compute_aqi(x, pm10_breakpoints), DoubleType())
udf_aqi_co = F.udf(lambda x: compute_aqi(x, co_breakpoints), DoubleType())
udf_aqi_no2 = F.udf(lambda x: compute_aqi(x, no2_breakpoints), DoubleType())
udf_aqi_o3 = F.udf(lambda x: compute_aqi(x, o3_breakpoints), DoubleType())

min_count = int(EXPECTED_SAMPLES * THRESHOLD)

# Calculate valid AQI values per each pollutant
df_aqi = df_agg \
    .withColumn("AQI_PM2_5",F.when(F.col("PM2_5_count") >= min_count, udf_aqi_pm25("PM2_5_mean"))) \
    .withColumn("AQI_PM10",F.when(F.col("PM10_count") >= min_count,udf_aqi_pm10("PM10_mean"))) \
    .withColumn("AQI_CO",F.when(F.col("CO_count") >= min_count,udf_aqi_co("CO_ppm_mean"))) \
    .withColumn("AQI_NO2",F.when(F.col("NO2_count") >= min_count,udf_aqi_no2("NO2_ppm_mean"))) \
    .withColumn("AQI_O3",F.when(F.col("O3_count") >= min_count,udf_aqi_o3("O3_ppm_mean"))) \
    .withColumn("AQI_Max",F.greatest("AQI_PM2_5", "AQI_PM10", "AQI_CO", "AQI_NO2", "AQI_O3"))

# Write aggregated data to Mongo
agg_query=df_aqi.writeStream \
    .format("mongodb") \
    .option("collection", "agg_collection") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/aqi") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .outputMode("append") \
    .start()

# Wait for both streams to finish
raw_query.awaitTermination()
agg_query.awaitTermination()