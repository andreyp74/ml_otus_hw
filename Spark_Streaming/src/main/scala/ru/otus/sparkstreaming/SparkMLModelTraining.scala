package ru.otus.sparkstreaming

import com.typesafe.config.{Config, ConfigFactory}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.slf4j.{Logger, LoggerFactory}

object SparkMLModelTraining {

    val LOGGER: Logger = LoggerFactory.getLogger(SparkMLModelTraining.getClass.getName)

    def main(args: Array[String]): Unit = {

        val config: Config = ConfigFactory.load("application.json")

        val SPARK_MASTER = config.getString("spark.master")
        val SPARK_APP_NAME = config.getString("ml.app_name")
        val DATASET_PATH = config.getString("ml.dataset_path")
        val MODEL_NAME = config.getString("ml.model_name")
        val MODEL_SAVING_LOCATION = config.getString("ml.model_location")

        val spark = SparkSession
            .builder
            .master(SPARK_MASTER)
            .appName(SPARK_APP_NAME)
            .getOrCreate()

        import spark.implicits._

        val data = spark.read.parquet(DATASET_PATH)

        val df = data.na.fill(0)

        //val allColumns = df.columns.toSeq
        val numericColumns = df.dtypes
            .filter(!_._2.equals("StringType"))
            .filter(!_._2.equals("ArrayType(StringType,true)"))
            .filter(!_._2.equals("DateType"))
            .map(d => d._1).toSeq

        val dfWithTarget = df.withColumn("Liked", when(array_contains($"feedback", "Liked"), 1).otherwise(0))
        
        val assembler = new VectorAssembler()
            .setInputCols(numericColumns.toArray)
            .setOutputCol("features")

        val transformed_data = assembler.transform(dfWithTarget)
            //.drop(numericColumns:_*)

        LOGGER.info("Training LinearRegression Model")

        val lr: LinearRegression = new LinearRegression()
                .setFeaturesCol("features")
                .setLabelCol("Liked")
                .setRegParam(0.0)
                .setElasticNetParam(0.0)
                .setMaxIter(10)
                .setTol(1E-6)

        // Train model
        val model = lr.fit(transformed_data)

        LOGGER.info("Finished Training LinearRegression Model")

        model.write.overwrite().save(MODEL_SAVING_LOCATION + "/" + MODEL_NAME)

        LOGGER.info("Successfully Saved LinearRegression Model")

        val result = model.transform(transformed_data)

        val evaluator = new RegressionEvaluator()
            .setLabelCol("Liked")
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        val rmse = evaluator.evaluate(result)

        result.select("Liked", "prediction").show()
        
        println(s"RMSE: ${rmse}")
        // Print the coefficients and intercept for linear regression
        println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

        spark.stop()
    }
}