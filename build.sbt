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

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:higherKinds"
)

lazy val flinkVersion       = "1.11.2"
lazy val log4jVersion       = "2.13.0"
lazy val scalatestVersion   = "3.1.4"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j"   % "log4j-api"                      % log4jVersion,
  "org.apache.logging.log4j"   % "log4j-core"                     % log4jVersion     % Runtime,
  "org.apache.logging.log4j"  %% "log4j-api-scala"                % "12.0",

  "org.apache.flink"          %% "flink-scala"                    % flinkVersion      % Provided,
  "org.apache.flink"          %% "flink-streaming-scala"          % flinkVersion      % Provided,
  "org.apache.flink"          %% "flink-connector-kafka"          % flinkVersion,
  "org.apache.flink"          %% "flink-clients"                  % flinkVersion,

  "org.apache.kafka"           % "kafka-clients"                  % "2.4.0",
  "org.apache.zookeeper"       % "zookeeper"                      % "3.4.9",

  "org.scalactic"             %% "scalactic"                      % scalatestVersion  % Test,
  "org.scalatest"             %% "scalatest"                      % scalatestVersion  % Test,
  "org.mockito"               %% "mockito-scala"                  % "1.15.0"          % Test,

  "io.github.embeddedkafka"   %% "embedded-kafka"                 % "2.4.0"           % Test,

  "org.apache.avro"            % "avro"                           % "1.10.0",
  "com.sksamuel.avro4s"       %% "avro4s-core"                    % "3.1.1",

  "org.apache.flink"          %% "flink-table-api-scala-bridge"   % flinkVersion      % Provided,
  "org.apache.flink"          %% "flink-table-planner-blink"      % flinkVersion      % Provided,
  "org.apache.flink"           % "flink-json"                     % flinkVersion,

  "org.apache.flink"          %% "flink-test-utils"               % flinkVersion      % Test classifier "tests",
  "org.apache.flink"          %% "flink-runtime"                  % flinkVersion      % Test classifier "tests",
  "org.apache.flink"          %% "flink-streaming-java"           % flinkVersion      % Test classifier "tests",

  "com.github.scopt"          %% "scopt"                          % "3.7.1"
)

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

parallelExecution in Test := false
