package ru.otus.sparkstreaming

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.Pipeline

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import scala.collection.Seq

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

        // val df1 = df
        //             //.withColumn("Unliked", when(array_contains($"feedback", "Unliked"), 1).otherwise(0))
        //             //.withColumn("Disliked", when(array_contains($"feedback", "Disliked"), 1).otherwise(0))
        //             .withColumn("Commented", when(array_contains($"feedback", "Commented"), 1).otherwise(0))
        //             .withColumn("ReShared", when(array_contains($"feedback", "ReShared"), 1).otherwise(0))
        //             .withColumn("Viewed", when(array_contains($"feedback", "Viewed"), 1).otherwise(0))
        //             .withColumn("CreatedAt_Hour", from_unixtime(abs($"metadata_createdAt") / 1000, "HH").cast(IntegerType))
        //             .withColumn("AuditedAt_Hour", from_unixtime(abs($"audit_timestamp") / 1000, "HH").cast(IntegerType))

        //val allColumns = df.columns.toSeq
        val numericColumns = df.dtypes
            .filter(!_._2.equals("StringType"))
            .filter(!_._2.equals("ArrayType(StringType,true)"))
            .filter(!_._2.equals("DateType"))
            .map(d => d._1).toSeq
        //val otherColumns = allColumns.filterNot(c => numericColumns.exists(_ == c))

        val dfWithTarget = df.withColumn("Liked", when(array_contains($"feedback", "Liked"), 1).otherwise(0))

        // val objectTypeIndexer = new StringIndexer()
        //     .setInputCol("instanceId_objectType")
        //     .setOutputCol("objectTypeIndex")
        //     .setHandleInvalid("keep")

        // val clientTypeIndexer = new StringIndexer()
        //     .setInputCol("audit_clientType")
        //     .setOutputCol("clientTypeIndex")
        //     .setHandleInvalid("keep")

        // val ownerTypeIndexer = new StringIndexer()
        //     .setInputCol("metadata_ownerType")
        //     .setOutputCol("ownerTypeIndex")
        //     .setHandleInvalid("keep")

        // val platformIndexer = new StringIndexer()
        //     .setInputCol("metadata_platform")
        //     .setOutputCol("platformIndex")
        //     .setHandleInvalid("keep")

        // val statusIndexer = new StringIndexer()
        //     .setInputCol("membership_status")
        //     .setOutputCol("statusIndex")
        //     .setHandleInvalid("keep")

        // val indexer = new Pipeline().setStages(Array(objectTypeIndexer, clientTypeIndexer, ownerTypeIndexer, platformIndexer, statusIndexer))
        // val indexed = indexer.fit(df2).transform(df2)

        // val indexedOnlyNumeric = indexed.drop(otherColumns:_*)
        
        val assembler = new VectorAssembler()
            .setInputCols(numericColumns.toArray)
            .setOutputCol("features")

        val transformed_data = assembler.transform(dfWithTarget)
            //.drop(numericColumns:_*)

        //transformed_data.show()
        //transformed_data.printSchema()

        LOGGER.info("Training LinearRegression Model")

        val lr: LinearRegression = new LinearRegression()
                .setFeaturesCol("features")
                .setLabelCol("Liked")
                .setRegParam(0.0)
                .setElasticNetParam(0.0)
                .setMaxIter(10)
                .setTol(1E-6)

        // Train model. This also runs the indexers.
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