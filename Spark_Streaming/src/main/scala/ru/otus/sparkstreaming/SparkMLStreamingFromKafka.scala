package ru.otus.sparkstreaming

import com.typesafe.config.{Config, ConfigFactory}

import ru.otus.sparkstreaming.utils.ParseKafkaMessage
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, VectorIndexer}


object SparkMLStreamingFromKafka {

  def main(args: Array[String]): Unit = {

    val schema = StructType(Array(
        StructField("instanceId_userId",IntegerType,true), 
        StructField("instanceId_objectType",StringType,true), 
        StructField("instanceId_objectId",IntegerType,true), 
        StructField("audit_pos",LongType,true), 
        StructField("audit_clientType",StringType,true), 
        StructField("audit_timestamp",LongType,true), 
        StructField("audit_timePassed",LongType,true), 
        StructField("audit_resourceType",LongType,true), 
        StructField("metadata_ownerId",IntegerType,true), 
        StructField("metadata_ownerType",StringType,true), 
        StructField("metadata_createdAt",LongType,true), 
        StructField("metadata_authorId",IntegerType,true), 
        StructField("metadata_applicationId",LongType,true), 
        StructField("metadata_numCompanions",IntegerType,true), 
        StructField("metadata_numPhotos",IntegerType,true), 
        StructField("metadata_numPolls",IntegerType,true), 
        StructField("metadata_numSymbols",IntegerType,true), 
        StructField("metadata_numTokens",IntegerType,true), 
        StructField("metadata_numVideos",IntegerType,true), 
        StructField("metadata_platform",StringType,true), 
        StructField("metadata_totalVideoLength",IntegerType,true), 
        StructField("metadata_options",
                    ArrayType(StringType,true),
                    true), 
        StructField("userOwnerCounters_USER_FEED_REMOVE",DoubleType,true), 
        StructField("userOwnerCounters_USER_PROFILE_VIEW",DoubleType,true), 
        StructField("userOwnerCounters_VOTE_POLL",DoubleType,true), 
        StructField("userOwnerCounters_USER_SEND_MESSAGE",DoubleType,true), 
        StructField("userOwnerCounters_USER_DELETE_MESSAGE",DoubleType,true), 
        StructField("userOwnerCounters_USER_INTERNAL_LIKE",DoubleType,true), 
        StructField("userOwnerCounters_USER_INTERNAL_UNLIKE",DoubleType,true), 
        StructField("userOwnerCounters_USER_STATUS_COMMENT_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_PHOTO_COMMENT_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_MOVIE_COMMENT_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_USER_PHOTO_ALBUM_COMMENT_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_COMMENT_INTERNAL_LIKE",DoubleType,true), 
        StructField("userOwnerCounters_USER_FORUM_MESSAGE_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_PHOTO_MARK_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_PHOTO_VIEW",DoubleType,true), 
        StructField("userOwnerCounters_PHOTO_PIN_BATCH_CREATE",DoubleType,true), 
        StructField("userOwnerCounters_PHOTO_PIN_UPDATE",DoubleType,true), 
        StructField("userOwnerCounters_USER_PRESENT_SEND",DoubleType,true), 
        StructField("userOwnerCounters_UNKNOWN",DoubleType,true), 
        StructField("userOwnerCounters_CREATE_TOPIC",DoubleType,true), 
        StructField("userOwnerCounters_CREATE_IMAGE",DoubleType,true), 
        StructField("userOwnerCounters_CREATE_MOVIE",DoubleType,true), 
        StructField("userOwnerCounters_CREATE_COMMENT",DoubleType,true), 
        StructField("userOwnerCounters_CREATE_LIKE",DoubleType,true), 
        StructField("userOwnerCounters_TEXT",DoubleType,true), 
        StructField("userOwnerCounters_IMAGE",DoubleType,true), 
        StructField("userOwnerCounters_VIDEO",DoubleType,true), 
        StructField("membership_status",StringType,true),
        StructField("membership_statusUpdateDate",DoubleType,true), 
        StructField("membership_joinDate",DoubleType,true), 
        StructField("membership_joinRequestDate",DoubleType,true), 
        StructField("user_create_date",DoubleType,true), 
        StructField("user_birth_date",DoubleType,true), 
        StructField("user_gender",DoubleType,true), 
        StructField("user_status",DoubleType,true), 
        StructField("user_ID_country",DoubleType,true), 
        StructField("user_ID_Location",DoubleType,true), 
        StructField("user_is_active",DoubleType,true), 
        StructField("user_is_deleted",DoubleType,true), 
        StructField("user_is_abused",DoubleType,true), 
        StructField("user_is_activated",DoubleType,true), 
        StructField("user_change_datime",DoubleType,true), 
        StructField("user_is_semiactivated",DoubleType,true), 
        StructField("user_region",DoubleType,true), 
        StructField("date",StringType,true),     
        StructField("auditweights_ageMs",DoubleType,true), 
        StructField("auditweights_closed",DoubleType,true), 
        StructField("auditweights_ctr_gender",DoubleType,true), 
        StructField("auditweights_ctr_high",DoubleType,true), 
        StructField("auditweights_ctr_negative",DoubleType,true), 
        StructField("auditweights_dailyRecency",DoubleType,true), 
        StructField("auditweights_feedOwner_RECOMMENDED_GROUP",DoubleType,true), 
        StructField("auditweights_feedStats",DoubleType,true), 
        StructField("auditweights_friendCommentFeeds",DoubleType,true), 
        StructField("auditweights_friendCommenters",DoubleType,true), 
        StructField("auditweights_friendLikes",DoubleType,true), 
        StructField("auditweights_friendLikes_actors",DoubleType,true), 
        StructField("auditweights_hasDetectedText",DoubleType,true), 
        StructField("auditweights_hasText",DoubleType,true), 
        StructField("auditweights_isRandom",DoubleType,true), 
        StructField("auditweights_likersFeedStats_hyper",DoubleType,true), 
        StructField("auditweights_notOriginalPhoto",DoubleType,true), 
        StructField("auditweights_numDislikes",DoubleType,true), 
        StructField("auditweights_numLikes",DoubleType,true), 
        StructField("auditweights_numShows",DoubleType,true), 
        StructField("auditweights_onlineVideo",DoubleType,true), 
        StructField("auditweights_processedVideo",DoubleType,true), 
        StructField("auditweights_relationMasks",DoubleType,true), 
        StructField("auditweights_source_MOVIE_TOP",DoubleType,true), 
        StructField("auditweights_userAge",DoubleType,true), 
        StructField("auditweights_userOwner_CREATE_COMMENT",DoubleType,true), 
        StructField("auditweights_userOwner_CREATE_IMAGE",DoubleType,true), 
        StructField("auditweights_userOwner_CREATE_LIKE",DoubleType,true), 
        StructField("auditweights_userOwner_IMAGE",DoubleType,true), 
        StructField("auditweights_userOwner_PHOTO_VIEW",DoubleType,true), 
        StructField("auditweights_userOwner_TEXT",DoubleType,true), 
        StructField("auditweights_userOwner_UNKNOWN",DoubleType,true), 
        StructField("auditweights_userOwner_USER_FEED_REMOVE",DoubleType,true), 
        StructField("auditweights_userOwner_USER_PROFILE_VIEW",DoubleType,true), 
        StructField("auditweights_userOwner_VIDEO",DoubleType,true), 
        StructField("auditweights_x_ActorsRelations",DoubleType,true), 
        StructField("auditweights_svd",DoubleType,true), 
        StructField("ImageId",
                    ArrayType(StringType,true),
                    true), 
        StructField("auditweights_likersSvd_hyper",DoubleType,true)
    ))

    val config: Config = ConfigFactory.load("application.json")

    val SPARK_APP_NAME: String = config.getString("spark.app_name")
    val SPARK_MASTER: String = config.getString("spark.master")
    val SPARK_BATCH_DURATION: Int = config.getInt("spark.batch_duration")

    val KAFKA_TOPIC: String = config.getString("kafka.consumer_topic")
    val KAFKA_BROKERS: String = config.getString("kafka.brokers")
    val KAFKA_GROUP_ID: String = config.getString("kafka.group_id")
    val KAFKA_OFFSET_RESET: String = config.getString("kafka.auto_offset_reset")
    val MODEL_LOCATION = config.getString("ml.model_location")
    val MODEL_NAME = config.getString("ml.model_name")

    val sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER)
    val sparkStreamingContext = new StreamingContext(sparkConf, Seconds(SPARK_BATCH_DURATION))

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    import spark.implicits._

    val model = LinearRegressionModel.load(MODEL_LOCATION + "/" + MODEL_NAME)

    var dfText = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKERS)
      .option("subscribe", KAFKA_TOPIC)
      .option("startingOffsets", KAFKA_OFFSET_RESET)
      .load()
      .selectExpr("CAST(value AS STRING)")

    val dfFromJson = dfText.withColumn("json", from_json(col("value"), schema)).select("json.*")

    val df = dfFromJson.na.fill(0)

    // val df = dfFilled
    //             .withColumn("CreatedAt_Hour", from_unixtime(abs($"metadata_createdAt") / 1000, "HH").cast(IntegerType))
    //             .withColumn("AuditedAt_Hour", from_unixtime(abs($"audit_timestamp") / 1000, "HH").cast(IntegerType))

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
    // val dfIndexed = indexer.fit(df).transform(df)

    val allColumns = df.columns.toSeq
    val numericColumns = df.dtypes
        .filter(!_._2.equals("StringType"))
        .filter(!_._2.equals("ArrayType(StringType,true)"))
        .filter(!_._2.equals("DateType"))
        .map(d => d._1).toSeq
    // val otherColumns = allColumns.filterNot(c => numericColumns.exists(_ == c))
    
    val assembler = new VectorAssembler()
        .setInputCols(numericColumns.toArray)
        .setOutputCol("features")

    val transformed_data = assembler.transform(df)
      //.drop(allColumns:_*)
    
    val result = model.transform(transformed_data)

    result.select("features", "prediction")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()

    spark.stop()
  }

}
