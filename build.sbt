name := "kafkaquery"

scalaVersion := "2.12.15"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked",
  "-language:higherKinds"
)

enablePlugins(PackPlugin)
packMain := Map(name.value -> "org.kafkaquery.CLI")
packExtraClasspath := Map(name.value -> Seq("${PROG_HOME}/udf_dependencies/*"))

lazy val flinkVersion = "1.12.7"
lazy val kafkaVersion = "3.1.0"
lazy val log4jVersion = "2.17.2"
lazy val scalatestVersion = "3.2.12"

libraryDependencies ++= Seq(
  // scala-steward:off
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion % Runtime,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" % "flink-core" % flinkVersion classifier "tests",
  "org.apache.zookeeper" % "zookeeper" % "3.7.0",
  "io.dropwizard.metrics" % "metrics-core" % "4.2.9" % Test,
  "org.scalactic" %% "scalactic" % scalatestVersion % Test,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.mockito" %% "mockito-scala" % "1.17.5" % Test,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion % Test,
  "org.apache.avro" % "avro" % "1.11.0",
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
  "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion,
  "org.apache.flink" % "flink-json" % flinkVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % Test,
  "org.apache.flink" %% "flink-runtime" % flinkVersion % Test classifier "tests",
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion % Test classifier "tests",
  "com.github.scopt" %% "scopt" % "4.0.1",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.12" % Test
)

// Fork all tasks
fork := true
