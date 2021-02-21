# Spark ML homework #1
## How to start:
1. Upload data from https://cloud.mail.ru/public/A9cF/bVSWJCJgt/ 
2. From the root directory of this repository run command: cd ./SparkML_Homework#1/zeppelin
3. Copy uploaded data to the ./data directory
4. Run command: docker-compose up
5. Start address http://localhost:8080 in your browser
6. Import Zeppelin notebook from ./notebooks/Spark ML Homework #1.zpln
# Spark streaming homework
## Requirements
* Python 3.7
* Scala 2.11.12
* Spark 2.4.7
## How to start:
1. Upload data from https://cloud.mail.ru/public/A9cF/bVSWJCJgt/
2. Copy uploaded data to the ./data directory
3. From the root directory of this repository run command: cd ./Spark_Streaming
4. From sbt shell run 'compile' and then run 'package'
### Kafka producer
Check command line arguments and run script: python/kafka_producer_run.sh
### Model training
Check spark-submit arguments and run script: ./spark-submit_model.sh
### Streaming
Check spark-submit arguments and run script: ./spark-submit.sh
