#!/bin/bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 spark_scripts/consume_kafka_spark.py
