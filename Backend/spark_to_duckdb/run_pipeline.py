import os
import sys
import json
import threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, element_at, when, mean as spark_mean, sum as spark_sum, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, FloatType
import paho.mqtt.client as mqtt
import duckdb
import pyarrow as pa
import configparser

config_file = "config.ini"

config = configparser.ConfigParser()
config.read(config_file)
broker = config['mqtt']['broker_address']
topic = config['mqtt']['topic']
port = int(config['mqtt']['port'])
mqtt_username = config['mqtt']['username']
mqtt_password = config['mqtt']['password']
batch_size = int(config['batch']['size'])

motherduck_token = config['motherduck']['token']
is_remote_database = config['database']['remote']


if is_remote_database == "yes":
    con = duckdb.connect(
        f'md:[sensor_database]?[motherduck_token={motherduck_token}]&saas_mode=true')
else:
    duckdb_file = '../warehouse/sensor_data.duckdb'


# Spark configs (should be run before session is created)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SmartAgricultureMonitoring") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Define the schema for the combined data
schema = StructType([
    StructField("sensor_data", StructType([
        StructField("local_time", StringType(), True),
        StructField("site_name", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("probe_id", StringType(), True),
        StructField("probe_measure", StringType(), True),
        StructField("soil_value", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("json_featuretype", StringType(), True),
        # Keeping as StringType in schema
        StructField("date", StringType(), True),
        StructField("time", StringType(), True)
    ]), True),
    StructField("weather_data", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("utc_offset_seconds", IntegerType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_abbreviation", StringType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("daily_units", StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m_mean", StringType(), True),
            StructField("precipitation_sum", StringType(), True),
            StructField("et0_fao_evapotranspiration", StringType(), True)
        ]), True),
        StructField("daily", StructType([
            StructField("time", StringType(), True),
            StructField("temperature_2m_mean", StringType(), True),
            StructField("precipitation_sum", StringType(), True),
            StructField("et0_fao_evapotranspiration", StringType(), True)
        ]), True)
    ]), True)
])


def process_batch(messages):
    rdd = spark.sparkContext.parallelize(messages)
    df = spark.read.json(rdd, schema=schema)

    # Select all fields from sensor_data and weather_data
    df = df.select(
        "sensor_data.*",
        col("weather_data.latitude"),
        col("weather_data.longitude"),
        col("weather_data.generationtime_ms"),
        col("weather_data.utc_offset_seconds"),
        col("weather_data.timezone"),
        col("weather_data.timezone_abbreviation"),
        col("weather_data.elevation"),
        col("weather_data.daily_units.time").alias("daily_units_time"),
        col("weather_data.daily_units.temperature_2m_mean").alias(
            "daily_units_temperature_2m_mean"),
        col("weather_data.daily_units.precipitation_sum").alias(
            "daily_units_precipitation_sum"),
        col("weather_data.daily_units.et0_fao_evapotranspiration").alias(
            "daily_units_et0_fao_evapotranspiration"),
        col("weather_data.daily.time").alias("daily_time"),
        col("weather_data.daily.temperature_2m_mean").alias(
            "daily_temperature_2m_mean"),
        col("weather_data.daily.precipitation_sum").alias(
            "daily_precipitation_sum"),
        col("weather_data.daily.et0_fao_evapotranspiration").alias(
            "daily_et0_fao_evapotranspiration")
    )

    # Convert the necessary columns from string to numeric
    df = df.withColumn("daily_et0_fao_evapotranspiration", from_json(col("daily_et0_fao_evapotranspiration"), ArrayType(FloatType()))) \
           .withColumn("daily_precipitation_sum", from_json(col("daily_precipitation_sum"), ArrayType(FloatType()))) \
           .withColumn("daily_temperature_2m_mean", from_json(col("daily_temperature_2m_mean"), ArrayType(FloatType())))

    df = df.withColumn("daily_et0_fao_evapotranspiration", element_at(col("daily_et0_fao_evapotranspiration"), 1)) \
           .withColumn("daily_precipitation_sum", element_at(col("daily_precipitation_sum"), 1)) \
           .withColumn("daily_temperature_2m_mean", element_at(col("daily_temperature_2m_mean"), 1))

    # Convert date column to date type
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # Transformations
    field_capacity = 100  # Example value, adjust as needed
    base_temperature = 12

    # Add soil_moisture_content and soil_salinity columns
    df = df.withColumn("soil_moisture_content", when(col("unit") == "%VWC", col("soil_value").cast(FloatType()))) \
           .withColumn("soil_salinity", when(col("unit") == "µS/cm", col("soil_value").cast(FloatType())))

    # Add soil_moisture_deficit column
    df = df.withColumn("soil_moisture_deficit",
                       field_capacity - col("soil_value").cast(FloatType()))

    # Add GDD column
    df = df.withColumn(
        "GDD", (col("daily_temperature_2m_mean") - base_temperature).cast(FloatType()))

    # Add water_stress_index column
    df = df.withColumn("water_stress_index", (col(
        "soil_value").cast(FloatType()) / field_capacity))

    # Add soil_moisture_depletion_rate column
    df = df.withColumn("soil_moisture_depletion_rate", col("soil_value").cast(FloatType(
    )) - expr("lag(soil_value, 1, 0) over (partition by site_id order by local_time)"))

    # Aggregations
    avg_soil_moisture_df = df.groupBy("site_id", "local_time").agg(
        spark_mean("soil_value").alias("average_soil_moisture_content"))
    cumulative_etc_df = df.groupBy("site_id", "local_time").agg(spark_sum(
        "daily_et0_fao_evapotranspiration").alias("cumulative_evapotranspiration"))
    cumulative_rainfall_df = df.groupBy("site_id", "local_time").agg(
        spark_sum("daily_precipitation_sum").alias("cumulative_rainfall"))

    # Join aggregated data
    df = df.join(avg_soil_moisture_df, on=["site_id", "local_time"], how="left") \
           .join(cumulative_etc_df, on=["site_id", "local_time"], how="left") \
           .join(cumulative_rainfall_df, on=["site_id", "local_time"], how="left")

    df.show(truncate=False)

    # Convert Spark DataFrame to Arrow Table
    arrow_table = pa.Table.from_pandas(df.toPandas(), preserve_index=False)

    # Save to DuckDB
    if is_remote_database == "no":
        con = duckdb.connect(duckdb_file)
    table_name = "sensor_data"
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM arrow_table LIMIT 0")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_table")
    con.close()

# MQTT Callback functions


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(topic)
    else:
        print("Failed to connect, return code %d\n", rc)


def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode('utf-8'))
    userdata['messages'].append(data)
    if len(userdata['messages']) >= batch_size:
        process_batch(userdata['messages'])
        userdata['messages'].clear()


# Start MQTT Client
client = mqtt.Client(userdata={'messages': []})
client.username_pw_set(mqtt_username, mqtt_password)
client.on_connect = on_connect
client.on_message = on_message


def consume_message():
    client.connect(broker, port, 60)
    client.loop_start()
    while True:
        if len(client._userdata['messages']) >= batch_size:
            process_batch(client._userdata['messages'])
            client._userdata['messages'].clear()
    client.loop_stop()


consume_thread = threading.Thread(target=consume_message)
consume_thread.start()

# Make sure the Spark session is properly closed
consume_thread.join()
spark.stop()
