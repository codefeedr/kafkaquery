package org.kafkaquery.commands

import java.io.{File, PrintWriter}
import java.util
import java.util.Collections
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.avro.Schema
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row
import org.kafkaquery.parsers.Configurations.{EarliestQueryStart, QueryConfig}
import org.kafkaquery.parsers.Parser
import org.kafkaquery.util.ZookeeperSchemaExposer
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class QueryCommandTest extends AnyFunSuite with BeforeAndAfter with EmbeddedKafka with MockitoSugar {

  test ("Query should run and produce expected results") {
    CollectRowSink.values.clear()

    val tableName = "t1"

    val zkExposerMock = mock[ZookeeperSchemaExposer]
    doReturn(List("t1", "t2")).when(zkExposerMock).getAllChildren
    val t1Schema = new Schema.Parser().parse(
      """
        |{ "type": "record", "name": "t1", "fields": [ { "name": "f1", "type": "string" },
        |{ "name": "f2", "type": "int" } ], "rowtime": "false" }
        |""".stripMargin)
    doReturn(Option(t1Schema)).when(zkExposerMock).get(tableName)

    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0
    )

    withRunningKafkaOnFoundPort(config) { implicit config =>
      publishStringMessageToKafka(tableName, """{ "f1": "val1", "f2": 1 }""")
      publishStringMessageToKafka(tableName, """{ "f1": "val2", "f2": 2 }""")
      publishStringMessageToKafka(tableName, "")

      val qc = new QueryCommand(
        QueryConfig(timeout = 2, query = "select f1 from t1", startStrategy = EarliestQueryStart(), ignoreParseErr = false, timeoutFunc = () => ()),
        zkExposerMock,
        s"localhost:${config.kafkaPort}"
      )
      qc.ds.addSink(new CollectRowSink)

      try {
        qc.execute()
      } catch {
        case _: JobExecutionException =>
      }

      assertResult(util.Arrays.asList(Row.of("val1"), Row.of("val2")))(CollectRowSink.values)
    }

  }

  test ("usage of a User-defined function") {
    CollectRowSink.values.clear()

    val tableName = "t1"

    val zkExposerMock = mock[ZookeeperSchemaExposer]
    doReturn(List("t1", "t2")).when(zkExposerMock).getAllChildren
    val t1Schema = new Schema.Parser().parse(
      """
        |{ "type": "record", "name": "t1", "fields": [ { "name": "f1", "type": "string" },
        |{ "name": "f2", "type": "int" } ], "rowtime": "false" }
        |""".stripMargin)
    doReturn(Option(t1Schema)).when(zkExposerMock).get(tableName)

    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0
    )

    withRunningKafkaOnFoundPort(config) { implicit config =>
      publishStringMessageToKafka(tableName, """{ "f1": "val1", "f2": 1 }""")
      publishStringMessageToKafka(tableName, """{ "f1": "val2", "f2": 2 }""")
      publishStringMessageToKafka(tableName, "")

      val udfName = "MyUDF.java"

      new PrintWriter(udfName) {write(
        """import org.apache.flink.table.functions.ScalarFunction;
          |
          |public class MyUDF extends ScalarFunction {
          |    public String eval(String input) {
          |        return input + " : udf invoked";
          |    }
          |}""".stripMargin); close()}

      val udfFile = new File(udfName)
      udfFile.deleteOnExit()
      val qc = new QueryCommand(
        QueryConfig(timeout = 2, query = "select MyUDF(f1) from t1", startStrategy = EarliestQueryStart(), ignoreParseErr = false, timeoutFunc = () => (), userFunctions = new Parser().getClassNameList(List(udfFile))),
        zkExposerMock,
        s"localhost:${config.kafkaPort}"
      )
      qc.ds.addSink(new CollectRowSink)

      try {
        qc.execute()
      } catch {
        case _: JobExecutionException =>
      }

      assertResult(util.Arrays.asList(Row.of("val1 : udf invoked"), Row.of("val2 : udf invoked")))(CollectRowSink.values)
    }

  }

}

class CollectRowSink extends SinkFunction[Row] {
  override def invoke(value: Row, context: SinkFunction.Context): Unit = {
    CollectRowSink.values.add(value)
  }
}

object CollectRowSink {
  val values: util.List[Row] = Collections.synchronizedList(new util.ArrayList())
}
