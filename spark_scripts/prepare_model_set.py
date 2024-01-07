import subprocess, re
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql import Window, Row, SparkSession
from pyspark.sql.types import LongType
import pandas as pd 
import numpy as np

def listdir(path):
    files = str(subprocess.check_output('/home/hadoop/hadoop/bin/hdfs dfs -ls ' + path, shell=True))
    return [re.search(' (/.+)', i).group(1) for i in str(files).split("\\n") if re.search(' (/.+)', i)]

def zipindexdf(df):
    schema_new = df.schema.add("index", LongType(), False)
    return df.rdd.zipWithIndex().map(lambda l: list(l[0]) + [l[1]]).toDF(schema_new)


spark = SparkSession.builder.appName("createModelDataSet").getOrCreate()    

#only 0, 5, 6, 8, 12, 17, 19, 20

today = (date.today() - timedelta(days=1)).strftime("%Y_%m_%d")
tables_paths = listdir('/user/vehicles/' + today)

df = 0
for file_name in tables_paths:
    if df == 0:
        df = spark.read.parquet("hdfs://" + file_name)
    else:
        df = df.union(spark.read.parquet("hdfs://" + file_name))
    print("Read table" +  file_name)        

df = df.withColumn("timestamp",to_timestamp("timestamp")).\
	where(col("lineLabel").isin([0, 5, 6, 8, 12, 17, 19, 20]))

windowSpec = Window.partitionBy("id").orderBy(rand())

random_row = df.withColumn("random_number", row_number().over(windowSpec))

random_start = random_row.filter(random_row["random_number"] == 1).\
            select("id", "lat", "lon", "timestamp", "lineLabel", "type").\
            withColumnRenamed("lat", "lat_start").\
            withColumnRenamed("lon", "lon_start").\
            withColumnRenamed("timestamp", "timestamp_start")
random_end = random_row.filter(random_row["random_number"] == 2).\
            select("id", "lat", "lon", "timestamp").\
            withColumnRenamed("lat", "lat_end").\
            withColumnRenamed("lon", "lon_end").\
            withColumnRenamed("timestamp", "timestamp_end")

random_set = random_start.join(random_end, on = "id", how = "inner").\
                withColumn("time",
                    abs(col("timestamp_start").cast("long") - col("timestamp_end").cast("long"))
                )
                
# weather data
tables_paths = listdir('/user/weather/' + today)
df = 0
for file_name in tables_paths:
    if df == 0:
        df = spark.read.parquet("hdfs://" + file_name)
    else:
        df = df.union(spark.read.parquet("hdfs://" + file_name))
    print("Read table" +  file_name)    

df = df.withColumn("timestamp",to_timestamp("timestamp")).select("temp_c", "wind_kph", "pressure_in", "humidity", "cloud", "vis_km", "timestamp")
    
availabile_ts = np.array(df.select("timestamp").collect()).reshape(-1)
ats_int = np.array([int(y.timestamp()) for y in availabile_ts])
actual_ts = np.array(random_set.select("timestamp_start").collect()).reshape(-1)
timestamps_tojoin = [availabile_ts[np.argmin(np.abs(ats_int - int(x.timestamp())))] for x in actual_ts]

to_join = spark.createDataFrame(pd.DataFrame({"timestamp": timestamps_tojoin}))
to_join = zipindexdf(to_join)
random_set = zipindexdf(random_set)

random_set = random_set.join(to_join, "index", "inner")

random_set = random_set.join(df, "timestamp", "inner").\
		select("timestamp", "id", "lat_start", "lon_start", "lineLabel", "lat_end", "lon_end", "time", "temp_c", "wind_kph", "pressure_in", "humidity", "cloud", "vis_km")


random_set = random_set.withColumn("lineLabel",col("lineLabel").cast("int")).\
	withColumn("hour", hour(col("timestamp"))).\
	withColumn("minute", minute(col("timestamp"))).\
	drop("timestamp", "id")

random_set.write.save('hdfs:///user/model_data/', format="parquet", mode = 'append')

spark.stop()
