package org.codefeedr.kafkaquery.commands

import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{
  StreamTableEnvironment,
  tableConversions
}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.testutils.ClassLoaderUtils.withRoot
import org.apache.flink.types.Row
import org.apache.flink.util.TemporaryClassLoaderContext
import org.codefeedr.kafkaquery.parsers.Configurations.QueryConfig
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.codefeedr.kafkaquery.transforms.{
  QueryOutput,
  QuerySetup,
  TimeOutFunction
}
import org.codefeedr.kafkaquery.util.{
  StreamEnvironmentConfigHelper,
  ZookeeperSchemaExposer
}

import java.io.File
import scala.io.Source

class QueryCommand(
    qConfig: QueryConfig,
    zkExposer: ZookeeperSchemaExposer,
    kafkaAddr: String
) {

  private var classLoaderBuilder = withRoot(new File("custom"))

  qConfig.userFunctions.foreach { case (name, file) =>
    val fileContents = Source.fromFile(file.getAbsoluteFile)
    classLoaderBuilder =
      classLoaderBuilder.addClass(name, fileContents.mkString)
    fileContents.close()
  }

  private val functionClassLoader = classLoaderBuilder.build()
  TemporaryClassLoaderContext.of(functionClassLoader)

  val fsSettings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()

  val fsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment(
      StreamExecutionEnvironment.getDefaultLocalParallelism,
      StreamEnvironmentConfigHelper.getConfig(functionClassLoader)
    )

  val fsTableEnv: StreamTableEnvironment =
    StreamTableEnvironment.create(fsEnv, fsSettings)

  qConfig.userFunctions.foreach { case (name, _) =>
    // TODO try parent of ScalarFunction
    val func =
      functionClassLoader.loadClass(name).asInstanceOf[Class[ScalarFunction]]
    fsTableEnv.createFunction(name, func)
  }

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

  def execute(): Unit = fsEnv.execute()

}
