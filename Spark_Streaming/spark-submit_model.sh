#!/bin/bash

spark-submit \
  --master local[4] \
  --jars ./config-1.3.2.jar \
  --class ru.otus.sparkstreaming.SparkMLModelTraining \
  ./target/scala-2.11/spark-streaming-scala_2.11-0.1.0-SNAPSHOT.jar