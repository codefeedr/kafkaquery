package org.codefeedr.kafkaquery.commands

import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.configuration.{Configuration, TaskManagerOptions}
import org.apache.flink.runtime.client.JobExecutionException
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
    kafkaAddr: String,
    config: Configuration = new Configuration()
) {

  val fsSettings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()

  val fsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment(
      StreamExecutionEnvironment.getDefaultLocalParallelism,
      config
    )

  fsEnv.getConfig.enableObjectReuse()

  val fsTableEnv: StreamTableEnvironment =
    StreamTableEnvironment.create(fsEnv, fsSettings)

  private val supportedFormats = zkExposer.getAllChildren

  private val requestedTopics =
    QuerySetup.extractTopics(qConfig.query, supportedFormats)

  for (topicName <- requestedTopics) {
    val result = zkExposer.get(topicName)

    if (result.isDefined) {
      val ddlString = QuerySetup.getTableCreationCommand(
        topicName,
        QuerySetup.generateTableSchema(result.get, getNestedSchema),
        kafkaAddr,
        qConfig.checkLatest,
        qConfig.ignoreParseErr
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

  def execute(): Unit = {
    try {
      fsEnv.execute()
    } catch {
      case e: JobExecutionException =>
        var rootCause = e.getCause
        while (rootCause.getCause != null) {
          rootCause = rootCause.getCause
        }

        rootCause.getLocalizedMessage match {
          case s if s.matches("Insufficient number of network buffers.*") =>
            val newMemAmount =
              config.get(TaskManagerOptions.NETWORK_MEMORY_MIN).multiply(2)
            config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, newMemAmount)
            config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, newMemAmount)

            new QueryCommand(qConfig, zkExposer, kafkaAddr, config).execute()

          case _ => e.printStackTrace()
        }
    }
  }

}
