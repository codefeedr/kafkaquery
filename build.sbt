name := "kafkaquery"

scalaVersion := "2.12.12"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-language:higherKinds"
)

enablePlugins(PackPlugin)
packMain := Map(name.value -> "org.kafkaquery.CLI")
packExtraClasspath := Map(name.value -> Seq("${PROG_HOME}/udf_dependencies/*"))

lazy val flinkVersion       = "1.12.1"
lazy val kafkaVersion       = "2.7.0"
lazy val log4jVersion       = "2.14.0"
lazy val scalatestVersion   = "3.2.5"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion,
  "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion      % Runtime,
  "org.apache.logging.log4j"  %% "log4j-api-scala"                % "12.0",

  "org.apache.flink"          %% "flink-scala"                    % flinkVersion,
  "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion,
  "org.apache.flink"          %% "flink-connector-kafka"          % flinkVersion,
  "org.apache.flink"          %% "flink-clients"                  % flinkVersion,
  "org.apache.flink"           % "flink-core"                     % flinkVersion      classifier "tests",

  "org.apache.zookeeper"       % "zookeeper"                      % "3.6.2",
  "io.dropwizard.metrics"      % "metrics-core"                   % "4.1.18"          % Test,

  "org.scalactic"             %% "scalactic"                      % scalatestVersion  % Test,
  "org.scalatest"             %% "scalatest"                      % scalatestVersion  % Test,
  "org.mockito"               %% "mockito-scala"                  % "1.16.29"         % Test,

  "org.apache.kafka"           % "kafka-clients"                  % kafkaVersion,
  "io.github.embeddedkafka"   %% "embedded-kafka"                 % kafkaVersion      % Test,

  "org.apache.avro"            % "avro"                           % "1.10.1",

  "org.apache.flink"          %% "flink-table-api-scala-bridge"   % flinkVersion,
  "org.apache.flink"          %% "flink-table-planner-blink"      % flinkVersion,
  "org.apache.flink"           % "flink-json"                     % flinkVersion,

  "org.apache.flink"          %% "flink-test-utils"               % flinkVersion      % Test,
  "org.apache.flink"          %% "flink-runtime"                  % flinkVersion      % Test classifier "tests",
  "org.apache.flink"          %% "flink-streaming-java"           % flinkVersion      % Test classifier "tests",

  "com.github.scopt"          %% "scopt"                          % "4.0.0",
  "com.sksamuel.avro4s"       %% "avro4s-core"                    % "4.0.4"           % Test,
)

// Fork all tasks
fork := true
