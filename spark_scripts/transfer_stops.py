from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("App") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

stops = spark.read.parquet("hdfs:///user/stops/stops.parquet")

stops.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("keyspace", "bda_project") \
    .option("table", "stops") \
    .save()
