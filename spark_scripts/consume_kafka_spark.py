from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from xgboost.spark import SparkXGBRegressorModel
import time 
from datetime import date, timedelta
import json

def json_to_df(json_list):
	data = json.loads(json_list)
	rows = [Row(**row) for row in data]
	reconstrued_df = spark.createDataFrame(rows)
	return reconstrued_df

	
def write_to_cassandra(df, epoch_id):
	print("Writting to cassandra")
	df.write \
		.format("org.apache.spark.sql.cassandra") \
		.mode("append") \
		.option("keyspace", "bda_project") \
		.option("table", "predictions") \
		.save()
	
	
spark = SparkSession.builder \
    .appName("CassandraStreamingExample") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

kafka_server = "localhost:9092"
topic_vehicles = "InputVehicles"
topic_weather = "InputWeather"
windowDuration = "10 seconds"
lat_center = 51.94342036741668
lon_center = 15.508601586647911
today = date.today().strftime("%Y_%m_%d")
stops = spark.read.parquet("hdfs:///user/stops/stops.parquet")

schema = StructType().add("key", StringType()).add("value", StringType())

json_schema = ArrayType(StructType([StructField("id", StringType(), True), \
			StructField("label", StringType(), True), \
			StructField("lineLabel", StringType(), True), \
			StructField("lat", DoubleType(), True), \
			StructField("lon", DoubleType(), True), \
			StructField("time", IntegerType(), True), \
			StructField("depid", IntegerType(), True), \
			StructField("type", StringType(), True), \
			StructField("timestamp", StringType(), True)]))

json_schema_weather = ArrayType(StructType([StructField("last_updated_epoch", IntegerType(), True), \
			StructField("last_updated", StringType(), True), \
			StructField("condition", StringType(), True), \
			StructField("temp_c", DoubleType(), True), \
			StructField("temp_f", DoubleType(), True), \
			StructField("wind_mph", DoubleType(), True), \
			StructField("wind_kph", DoubleType(), True), \
			StructField("wind_degree", DoubleType(), True), \
			StructField("wind_dir", StringType(), True), \
			StructField("pressure_mb", DoubleType(), True), \
			StructField("pressure_in", DoubleType(), True), \
			StructField("precip_mm", DoubleType(), True), \
			StructField("precip_in", DoubleType(), True), \
			StructField("humidity", IntegerType(), True), \
			StructField("cloud", IntegerType(), True), \
			StructField("feelslike_c", DoubleType(), True), \
			StructField("feelslike_f", DoubleType(), True), \
			StructField("vis_km", DoubleType(), True), \
			StructField("vis_miles", DoubleType(), True), \
			StructField("uv", DoubleType(), True), \
			StructField("gust_mph", DoubleType(), True), \
			StructField("timestamp", StringType(), True)]))

df_vehicles = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", kafka_server) \
	.option("subscribe", topic_vehicles) \
	.option("startingOffsets", "earliest") \
	.load() \
	.selectExpr("CAST(key as STRING)", "CAST(value AS STRING)") \
	.withColumn("value2", from_json("value", json_schema)) \
	.select("key", explode("value2").alias("vals")) \
	.select("vals.*") \
	.withColumn("join_key", lit(1)) \
	.withColumn("timestamp_vehicles",to_timestamp("timestamp")) \
	.withColumn("window_vehicles", window("timestamp_vehicles", windowDuration)) \
	.withColumn("rounded_timestamp", round(col("timestamp_vehicles").cast("long") / 10) * 10) \
	.where(col("lineLabel").isin([0, 5, 6, 8, 12, 17, 19, 20]))

df_weather = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", kafka_server) \
	.option("subscribe", topic_weather) \
	.load() \
	.selectExpr("CAST(key as STRING)", "CAST(value AS STRING)") \
	.withColumn("value2", from_json("value", json_schema_weather)) \
	.select("key", explode("value2").alias("vals")) \
	.select("vals.*") \
	.withColumn("join_key", lit(1)) \
	.withColumn("timestamp_weather",to_timestamp("timestamp")) \
	.drop("timestamp", "id")\
	.withColumn("window_weather", window("timestamp_weather", windowDuration)) \
	.withColumn("rounded_timestamp", round(col("timestamp_weather").cast("long") / 10) * 10)
	
	
join_condition = [
	df_vehicles["rounded_timestamp"] == df_weather["rounded_timestamp"]
]

merged_df = df_vehicles.join(df_weather, join_condition, "inner") \
		.select("timestamp_weather", "id", "lat", "lon", "lineLabel", "time", "temp_c", "wind_kph", "pressure_in", "humidity", "cloud", "vis_km") \
		.withColumn("lineLabel",col("lineLabel").cast("int")).\
	withColumn("hour", hour(col("timestamp_weather"))).\
	withColumn("minute", minute(col("timestamp_weather"))).\
	withColumnRenamed("timestamp_weather", "timestamp").\
	withColumn("lat_end", lit(lat_center)).\
	withColumn("lon_end", lit(lon_center)).\
	withColumnRenamed("lat", "lat_start").\
	withColumnRenamed("lon", "lon_start")
	

model = SparkXGBRegressorModel.load("/user/model/xgboost-pyspark")
predicted_df = model.transform(merged_df).\
		withColumnRenamed("lineLabel", "linelabel")

query = predicted_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .start()

