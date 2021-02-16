name := "spark-streaming-scala"

scalaVersion := "2.11.12"

fork in Test := true

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10_2.11" % "2.4.7"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"
