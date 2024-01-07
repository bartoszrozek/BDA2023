from xgboost.spark import SparkXGBRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import subprocess, re

def listdir_parquet(path):
    files = str(subprocess.check_output('/home/hadoop/hadoop/bin/hdfs dfs -ls ' + path, shell=True))
    return [re.search(' (/.+)', i).group(1) for i in str(files).split("\\n") if re.search(' (/.+)', i) and re.search('.parquet', i)]


spark = SparkSession.builder \
    .appName("CreateModelDataSet") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

tables_paths = listdir_parquet('/user/model_data/')
df = 0
for file_name in tables_paths:
    if df == 0:
        df = spark.read.parquet("hdfs://" + file_name)
    else:
        df = df.union(spark.read.parquet("hdfs://" + file_name))
    print("Read table" +  file_name)        

target_name = "time"
feature_names = [x for x in df.columns if x != target_name]

reg_est = SparkXGBRegressor(
	features_col=feature_names,
	label_col = target_name,
	num_workers = 2, 
	device = "cuda"
)

model = reg_est.fit(df)
model.write().overwrite().save("/user/model/xgboost-pyspark")

predictions = model.transform(df)
		
model_results = predictions.withColumn("difference", abs(col("prediction") - col("time")))\
		.groupBy().agg(
    avg("difference").alias("difference_avg"),
    count("*").alias("observations"),
    current_timestamp().alias("timestamp")
)

model_results.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("keyspace", "bda_project") \
    .option("table", "model_results") \
    .save()
