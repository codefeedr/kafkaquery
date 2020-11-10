package org.codefeedr.kafkaquery.commands

import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{
  StreamTableEnvironment,
  tableConversions
}
import org.apache.flink.types.Row
import org.codefeedr.kafkaquery.parsers.Configurations.QueryConfig
import org.codefeedr.kafkaquery.transforms.{QuerySetup, TimeOutFunction}
import org.codefeedr.kafkaquery.util.ZookeeperSchemaExposer

class QueryCommand(
    qConfig: QueryConfig,
    zkExposer: ZookeeperSchemaExposer,
    kafkaAddr: String
) {

  val fsSettings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()

  val fsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.getExecutionEnvironment

  fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  fsEnv.getConfig.enableObjectReuse()

  val fsTableEnv: StreamTableEnvironment =
    StreamTableEnvironment.create(fsEnv, fsSettings)

  //

  private val supportedFormats = zkExposer.getAllChildren
  println("Supported Plugins: " + supportedFormats)

  private val requestedTopics =
    QuerySetup.extractTopics(qConfig.query, supportedFormats)
  println("Requested: " + requestedTopics)

  for (topicName <- requestedTopics) {
    val result = zkExposer.get(topicName)

    if (result.isDefined) {
      val ddlString = QuerySetup.getTableCreationCommand(
        topicName,
        QuerySetup.generateTableSchema(result.get),
        kafkaAddr,
        qConfig.checkLatest
      )

      fsTableEnv.executeSql(ddlString)
    }
  }

  private val ds =
    fsTableEnv.sqlQuery(qConfig.query).toRetractStream[Row].map(_._2)

  //

  if (qConfig.timeout > 0) {
    ds
      .keyBy(new NullByteKeySelector[Row]())
      .process(new TimeOutFunction(qConfig.timeout * 1000))
  }

  if (qConfig.outTopic.nonEmpty) {
    QuerySetup.queryToKafkaTopic(
      qConfig.outTopic,
      ds,
      kafkaAddr
    )
  } else if (qConfig.port != -1) {
    QuerySetup.queryToSocket(qConfig.port, ds)
  } else {
    QuerySetup.queryToConsole(ds)
  }

  ds.executionEnvironment.execute()

}
