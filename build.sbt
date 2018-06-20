name := "streaming"

version := "0.1"

scalaVersion := "2.12.4"

resolvers ++= Seq(
  "confluent-repository" at "http://packages.confluent.io/maven/"
)

libraryDependencies ++= Seq(
  "org.apache.kafka" %% "kafka" % "1.0.0",
  "org.apache.kafka" % "kafka-streams" % "1.0.0",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.avro" % "avro" % "1.8.2",
  "io.confluent" % "kafka-streams-avro-serde" % "3.3.0",
  "org.rogach" %% "scallop" % "3.1.1"

)
resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "fr.psug.kafka" %% "typesafe-kafka-streams-11" % "0.2.3"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.0"
libraryDependencies += "com.twitter" %% "chill-bijection" % "0.9.2"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.0.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.8"

libraryDependencies ++= {
  val akka = "com.typesafe.akka"
  Seq(
    akka    %% "akka-actor"           % "2.5.8",
    akka    %% "akka-http-core"       % "10.0.11",
    akka    %% "akka-stream-kafka"    % "0.18",
    akka    %% "akka-http"            % "10.0.11",
    akka    %% "akka-http-spray-json" % "10.0.11"
  )
}

