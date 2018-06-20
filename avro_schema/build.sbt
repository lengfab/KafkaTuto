name := "avro_schema"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.0"

resolvers ++= Seq(
  Classpaths.typesafeReleases,
  "confluent" at "http://packages.confluent.io/maven/"
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.apache.kafka" % "kafka_2.12" % "1.0.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "io.confluent" % "kafka-avro-serializer" % "3.2.1"
)

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"
