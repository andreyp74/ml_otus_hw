# Домашнее задание Spark Streaming
## Использование Spark Streaming для потокового обучения моделей
Цель: В данном проекте вы попрактикуетесь в использовании Spark Streaming для предобработки данных и обучения модели линейной регрессии
1. Подготовить эмуляцию проигрывания датасета с домашнего задания модуля 2
2. Реализовать подготовку признаков для модели
3. Реализовать обновление коэффициентов линейной регрессии в потоке с дополнительной регуляризацией на отклонение от базы

Note:
В текущей версии не реализовано "обновление коэффициентов линейной регрессии в потоке"
## Requirements
* Python 3.7
* Scala 2.11.12
* Spark 2.4.7
## How to start:
1. Upload data from https://cloud.mail.ru/public/A9cF/bVSWJCJgt/
2. Copy uploaded data to the ./data directory
3. From sbt shell run 'compile' and then run 'package'
### Kafka producer
Check command line arguments and run script: python/kafka_producer_run.sh
### Model training
Check spark-submit arguments and run script: ./spark-submit_model.sh
### Streaming
Check spark-submit arguments and run script: ./spark-submit.sh
