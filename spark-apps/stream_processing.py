from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType
from pyspark.sql import functions as F
import os
import logging
from datetime import datetime

# Set up logging for validdation
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/aq_db")
KAFKA_TOPIC = "sensor-data"
WINDOW_SIZE = "5 minutes"
EXPECTED_SAMPLES = 20  # expected number of measurements per window: 1 measurement every 15 seconds
THRESHOLD = 0.75  # completeness threshold

# Value ranges for each sensor
VALID_RANGES = {
    "PM10_ug_m3": (0, 1000),
    "PM2_5_ug_m3": (0, 500),
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "CO_ppm": (0, 50),
    "NO2_ppm": (0, 1),
    "O3_ppb": (0, 500),
    "bme_pressure": (800, 1100)
}

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

def compute_aqi(value, breakpoints):
    """Computes AQI value for a pollutant"""
    if value is None:
        return None
    for (C_low, C_high, I_low, I_high) in breakpoints:
        if C_low <= value <= C_high:
            return ((I_high - I_low) / (C_high - C_low)) * (value - C_low) + I_low
    return None

def range_check(col, min_val, max_val):
    """Checks value range and returns value if in range"""
    return F.when(
        F.col(col).isNotNull() & 
        (~F.isnan(F.col(col))) & 
        (F.col(col) >= min_val) & 
        (F.col(col) <= max_val), 
        F.col(col))

def log_batch_stats(df, batch_id):
    """Validates data schema for each batch"""
    try:
        total_count = df.count() # Count total records
        if total_count > 0:
            stats = {}   
            for field, (min_val, max_val) in VALID_RANGES.items():
                null_count = df.filter(F.col(field).isNull()).count() # Count missing values
                # Count values that are out of range
                out_of_range_count = df.filter(
                    F.col(field).isNotNull() & 
                    ((F.col(field) < min_val) | (F.col(field) > max_val))
                ).count()
                
                nan_count = df.filter(F.isnan(F.col(field))).count() # Count NaN values
                
                stats[field] = {
                    'null': null_count,
                    'out_of_range': out_of_range_count,
                    'nan': nan_count,
                    'total_invalid': null_count + out_of_range_count + nan_count}
            
            # Log stats 
            logger.info("=" * 50)
            logger.info(f"BATCH {batch_id} - VALIDATION")
            logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Total records: {total_count}")
            
            # Print records that didn't pass validation
            problem_fields = {k: v for k, v in stats.items() if v['total_invalid'] > 0}
            
            if problem_fields:
                logger.warning("Violations found in the data:")
                for field, field_stats in problem_fields.items():
                    issues = []
                    if field_stats['null'] > 0:
                        issues.append(f"{field_stats['null']} missing")
                    if field_stats['nan'] > 0:
                        issues.append(f"{field_stats['nan']} NaN")
                    if field_stats['out_of_range'] > 0:
                        issues.append(f"{field_stats['out_of_range']} out of range")      
                    logger.warning(f"  {field}: {', '.join(issues)}")
                
                logger.info("Rejected records:")
                problem_field = list(problem_fields.keys())[0]
                sample = df.filter(
                    F.col(problem_field).isNull() | 
                    F.isnan(F.col(problem_field)) |
                    (F.col(problem_field) < VALID_RANGES[problem_field][0]) |
                    (F.col(problem_field) > VALID_RANGES[problem_field][1])
                ).select("event_time", problem_field).limit(3).collect()
                
                for row in sample:
                    value = row[problem_field]
                    status = "NULL" if value is None else "NaN" if value != value else "OUT_OF_RANGE"
                    logger.warning(f"    {row['event_time']} - {problem_field}: {value} ({status})")
            else:
                logger.info("All records are valid!")    
            logger.info("=" * 50)
            
    except Exception as e:
        logger.error(f"Error in batch logging: {str(e)}")

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

# Log the raw data
logging_query = df_parsed.writeStream \
    .foreachBatch(log_batch_stats) \
    .outputMode("append") \
    .start()

# Write raw data to Mongo
raw_query = df_parsed.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/raw_checkpoint") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("database", "aq_db") \
    .option("collection", "raw_collection") \
    .outputMode("append") \
    .start()

# Apply range check
df_clean = df_parsed.select(
    "event_time",
    range_check("PM10_ug_m3", *VALID_RANGES["PM10_ug_m3"]).alias("PM10_ug_m3"),
    range_check("PM2_5_ug_m3", *VALID_RANGES["PM2_5_ug_m3"]).alias("PM2_5_ug_m3"),
    range_check("bme_pressure", *VALID_RANGES["bme_pressure"]).alias("bme_pressure"),
    range_check("temperature", *VALID_RANGES["temperature"]).alias("temperature"),
    range_check("humidity", *VALID_RANGES["humidity"]).alias("humidity"),
    range_check("CO_ppm", *VALID_RANGES["CO_ppm"]).alias("CO_ppm"),
    range_check("NO2_ppm", *VALID_RANGES["NO2_ppm"]).alias("NO2_ppm"),
    range_check("O3_ppb", *VALID_RANGES["O3_ppb"]).alias("O3_ppb")
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

udf_aqi_pm25 = F.udf(lambda x: compute_aqi(x, pm25_breakpoints), DoubleType())
udf_aqi_pm10 = F.udf(lambda x: compute_aqi(x, pm10_breakpoints), DoubleType())
udf_aqi_co = F.udf(lambda x: compute_aqi(x, co_breakpoints), DoubleType())
udf_aqi_no2 = F.udf(lambda x: compute_aqi(x, no2_breakpoints), DoubleType())
udf_aqi_o3 = F.udf(lambda x: compute_aqi(x, o3_breakpoints), DoubleType())

min_count = int(EXPECTED_SAMPLES * THRESHOLD)

# Calculate valid AQI values per each pollutant
df_aqi = df_agg \
    .withColumn("AQI_PM2_5", F.when(F.col("PM2_5_count") >= min_count, udf_aqi_pm25("PM2_5_mean"))) \
    .withColumn("AQI_PM10", F.when(F.col("PM10_count") >= min_count, udf_aqi_pm10("PM10_mean"))) \
    .withColumn("AQI_CO", F.when(F.col("CO_count") >= min_count, udf_aqi_co("CO_ppm_mean"))) \
    .withColumn("AQI_NO2", F.when(F.col("NO2_count") >= min_count, udf_aqi_no2("NO2_ppm_mean"))) \
    .withColumn("AQI_O3", F.when(F.col("O3_count") >= min_count, udf_aqi_o3("O3_ppm_mean"))) \
    .withColumn("AQI_Max", F.greatest("AQI_PM2_5", "AQI_PM10", "AQI_CO", "AQI_NO2", "AQI_O3"))

# Write aggregated data to Mongo
agg_query = df_aqi.writeStream \
    .format("mongodb") \
    .option("collection", "agg_collection") \
    .option("checkpointLocation", "/tmp/spark_checkpoint/aqi") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .outputMode("append") \
    .start()

# Start all streams
logger.info("=" * 50)
logger.info("Starting Air Quality Streaming Pipeline")
logger.info("=" * 50)
logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
logger.info(f"MongoDB URI: {MONGO_URI}")
logger.info(f"Window Size: {WINDOW_SIZE}")
logger.info(f"Expected Samples per Window: {EXPECTED_SAMPLES}")
logger.info(f"Completeness Threshold: {THRESHOLD}")
logger.info(f"Minimum Samples for AQI: {min_count}")

# Wait for all streams to finish
raw_query.awaitTermination()
agg_query.awaitTermination()
logging_query.awaitTermination()
