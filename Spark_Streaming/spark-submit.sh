#!/bin/bash

spark-submit \
  --master local[4] \
  --jars ./config-1.3.2.jar \
  --class ru.otus.sparkstreaming.SparkMLStreamingFromKafka \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 \
  ./target/scala-2.11/spark-streaming-scala_2.11-0.1.0-SNAPSHOT.jar