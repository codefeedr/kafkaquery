package org.codefeedr.kafkaquery.commands

import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{
  StreamTableEnvironment,
  tableConversions
}
import org.apache.flink.types.Row
import org.codefeedr.kafkaquery.parsers.Configurations.QueryConfig
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.codefeedr.kafkaquery.transforms.{
  QueryOutput,
  QuerySetup,
  TimeOutFunction
}
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

  fsEnv.getConfig.enableObjectReuse()

  val fsTableEnv: StreamTableEnvironment =
    StreamTableEnvironment.create(fsEnv, fsSettings)

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
        QuerySetup.generateTableSchema(result.get, getNestedSchema),
        kafkaAddr,
        qConfig.checkLatest
      )

      fsTableEnv.executeSql(ddlString)
    }
  }

  val ds: DataStream[Row] =
    fsTableEnv.sqlQuery(qConfig.query).toRetractStream[Row].map(_._2)

  if (qConfig.timeout > 0) {
    ds
      .keyBy(new NullByteKeySelector[Row]())
      .process(new TimeOutFunction(qConfig.timeout * 1000, qConfig.timeoutFunc))
  }
  QueryOutput.selectOutput(ds, qConfig, kafkaAddr)

  def execute(): Unit = fsEnv.execute()

}
