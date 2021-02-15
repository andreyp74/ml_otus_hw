name := "spark-streaming-scala"

scalaVersion := "2.11.12"
//scalaVersion := "2.12.0"

fork in Test := true

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10_2.11" % "2.4.7"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.0.0"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.7"
//libraryDependencies += "org.scala-lang" %% "scala-reflect" % scalaVersion.value
libraryDependencies += "com.typesafe" % "config" % "1.3.2"

// val circeVersion = "0.9.0"
// libraryDependencies ++= Seq(
//   "io.circe" %% "circe-core",
//   "io.circe" %% "circe-generic",
//   "io.circe" %% "circe-parser"
// ).map(_ % circeVersion)
