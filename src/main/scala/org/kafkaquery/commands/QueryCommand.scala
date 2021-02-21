package org.kafkaquery.commands

import org.apache.flink.configuration.{Configuration, TaskManagerOptions}
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{
  StreamTableEnvironment,
  tableConversions
}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.testutils.ClassLoaderUtils.withRoot
import org.apache.flink.types.Row
import org.apache.flink.util.TemporaryClassLoaderContext
import org.kafkaquery.parsers.Configurations.QueryConfig
import org.kafkaquery.transforms.SchemaConverter.getNestedSchema
import org.kafkaquery.transforms.{QueryOutput, QuerySetup, TimeOutFunction}
import org.kafkaquery.util.{StreamEnvConfigurator, ZookeeperSchemaExposer}

import java.io.File
import scala.io.Source

class QueryCommand(
    qConfig: QueryConfig,
    zkExposer: ZookeeperSchemaExposer,
    kafkaAddr: String,
    config: Configuration = new Configuration()
) {
  private val root = new File("custom")
  root.deleteOnExit()
  private var classLoaderBuilder = withRoot(root)

  qConfig.userFunctions.foreach { case (name, file) =>
    val fileContents = Source.fromFile(file.getAbsoluteFile)
    classLoaderBuilder =
      classLoaderBuilder.addClass(name, fileContents.mkString)
    fileContents.close()
  }

  private val functionClassLoader = classLoaderBuilder.build()
  TemporaryClassLoaderContext.of(functionClassLoader)

  //Mark every temporary udf for deletion
  private val rootList = root.list()
  if (rootList != null) {
    root
      .list()
      .foreach(udfName =>
        new File(root.getAbsolutePath + "/" + udfName).deleteOnExit()
      )
  }

  val fsSettings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()

  val fsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment(
      StreamExecutionEnvironment.getDefaultLocalParallelism,
      StreamEnvConfigurator.withClassLoader(functionClassLoader)
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

  private val requestedTopics =
    QuerySetup.extractTopics(qConfig.query, supportedFormats)

  for (topicName <- requestedTopics) {
    val result = zkExposer.get(topicName)

    if (result.isDefined) {
      val ddlString = QuerySetup.getTableCreationCommand(
        topicName,
        QuerySetup.generateTableSchema(result.get, getNestedSchema),
        kafkaAddr,
        qConfig.startStrategy,
        qConfig.ignoreParseErr
      )

      fsTableEnv.executeSql(ddlString)
    }
  }

  private val table = fsTableEnv.sqlQuery(qConfig.query)

  private val dataTypes = fromDataTypeToLegacyInfo(
    table.getSchema.getFieldDataTypes
  )
  private val fieldNames = table.getSchema.getFieldNames

  val ds: DataStream[Row] =
    table.toRetractStream[Row].map(_._2)

  if (qConfig.timeout > 0) {
    ds.process(new TimeOutFunction(qConfig.timeout * 1000, qConfig.timeoutFunc))
  }
  QueryOutput.selectOutput(ds, qConfig.output, kafkaAddr, dataTypes, fieldNames)

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
