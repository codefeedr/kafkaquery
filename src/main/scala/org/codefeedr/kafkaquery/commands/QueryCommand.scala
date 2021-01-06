package org.codefeedr.kafkaquery.commands

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util
import java.util.Arrays

import javax.tools.{JavaCompiler, ToolProvider}
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.{
  StreamTableEnvironment,
  tableConversions
}
import org.apache.flink.table.functions.{ScalarFunction, UserDefinedFunction}
import org.apache.flink.types.Row
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
import javax.tools.JavaCompiler
import javax.tools.ToolProvider
import org.apache.flink.configuration.{
  ConfigUtils,
  Configuration,
  DeploymentOptions,
  PipelineOptions
}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.testutils.ClassLoaderUtils
import org.apache.flink.util.TemporaryClassLoaderContext

class QueryCommand(
    qConfig: QueryConfig,
    zkExposer: ZookeeperSchemaExposer,
    kafkaAddr: String
) {

  val folder = new File("custom")
  val functionClassloader: URLClassLoader = ClassLoaderUtils.compileAndLoadJava(
    folder,
    "StringFunc.java",
    "" + "import org.apache.flink.table.functions.ScalarFunction;" + "\n" + "public class StringFunc extends ScalarFunction {\n" + "\tpublic String eval(String b) {\n" + "\t\treturn b + \" : udf invoked\";\n" + "\t}\n" + "}"
  )

  val ignored = TemporaryClassLoaderContext.of(functionClassloader)

  val fsSettings: EnvironmentSettings = EnvironmentSettings
    .newInstance()
    .useBlinkPlanner()
    .inStreamingMode()
    .build()

  val fsEnv: StreamExecutionEnvironment =
    StreamExecutionEnvironment.createLocalEnvironment(
      4,
      StreamEnvironmentConfigHelper.getConfig(functionClassloader)
    )
  //new StreamExecutionEnvironment(StreamEnvironmentConfigHelper.getConfig(functionClassloader));

  fsEnv.getConfig.enableObjectReuse()

  val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

  /*qConfig.userFunctions.foreach(tuple => {
    val name = tuple._1
    val udf = tuple._2

    val classLoader: ClassLoader = Thread.currentThread().getContextClassLoader
    val cls = classLoader.loadClass(udf.getName.substring(0, udf.getName.length - 5))
    val instance = cls.newInstance()
    fsTableEnv.createTemporaryFunction(name, instance.asInstanceOf[ScalarFunction])
  }
  )*/

  val stringFunc: Class[ScalarFunction] = functionClassloader
    .loadClass("StringFunc")
    .asInstanceOf[Class[ScalarFunction]]
  fsTableEnv.createFunction("StringFunc", stringFunc)

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
