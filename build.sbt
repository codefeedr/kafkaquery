name := "kafkaquery"

version := "0.1"

scalaVersion := "2.12.12"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-language:higherKinds"
)

enablePlugins(PackPlugin)
packMain := Map("codefeedr" -> "org.codefeedr.kafkaquery.CLI")

lazy val flinkVersion       = "1.12.0"
lazy val log4jVersion       = "2.14.0"
lazy val scalatestVersion   = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion,
  "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion      % Runtime,
  "org.apache.logging.log4j"  %% "log4j-api-scala"                % "12.0",

  "org.apache.flink"          %% "flink-scala"                    % flinkVersion,
  "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion,
  "org.apache.flink"          %% "flink-connector-kafka"          % flinkVersion,
  "org.apache.flink"          %% "flink-clients"                  % flinkVersion,

  "org.apache.kafka"           % "kafka-clients"                  % "2.4.1",
  "org.apache.zookeeper"       % "zookeeper"                      % "3.4.14",

  "org.scalactic"             %% "scalactic"                      % scalatestVersion  % Test,
  "org.scalatest"             %% "scalatest"                      % scalatestVersion  % Test,
  "org.mockito"               %% "mockito-scala"                  % "1.16.3"          % Test,

  "io.github.embeddedkafka"   %% "embedded-kafka"                 % "2.4.1.1"         % Test,

  "org.apache.avro"            % "avro"                           % "1.10.1",
  "com.sksamuel.avro4s"       %% "avro4s-core"                    % "4.0.3",

  "org.apache.flink"          %% "flink-table-api-scala-bridge"   % flinkVersion,
  "org.apache.flink"          %% "flink-table-planner-blink"      % flinkVersion,
  "org.apache.flink"           % "flink-json"                     % flinkVersion,

  "org.apache.flink"          %% "flink-test-utils"               % flinkVersion      % Test,
  "org.apache.flink"          %% "flink-runtime"                  % flinkVersion      % Test classifier "tests",
  "org.apache.flink"          %% "flink-streaming-java"           % flinkVersion      % Test classifier "tests",

  "com.github.scopt"          %% "scopt"                          % "4.0.0"
)

// Fork all tasks
fork := true
