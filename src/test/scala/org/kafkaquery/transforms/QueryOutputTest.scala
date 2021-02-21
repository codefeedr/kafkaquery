package org.kafkaquery.transforms

import net.manub.embeddedkafka.Codecs.stringDeserializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.types.Row
import org.kafkaquery.parsers.Configurations.{ConsoleQueryOut, KafkaQueryOut, KafkaQueryOutJson, SocketQueryOut}
import org.kafkaquery.sinks.SocketSink
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.Socket

class QueryOutputTest extends AnyFunSuite with BeforeAndAfter with EmbeddedKafka with MockitoSugar {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  var env: StreamExecutionEnvironment = _
  var ds: DataStream[Row] = _

  before {
    flinkCluster.before()

    env = StreamExecutionEnvironment.getExecutionEnvironment
    ds = env.fromElements(Row.of("val1"), Row.of("val2"))
  }

  after {
    flinkCluster.after()
  }

  test("Query should be able to output to console") {
    val oldOut = System.out
    val outContent = new ByteArrayOutputStream
    System.setOut(new PrintStream(outContent))

    QueryOutput.selectOutput(ds, ConsoleQueryOut(), "", null, null)
    env.execute()

    System.out.flush()
    System.setOut(oldOut)
    assertResult(outContent.toString.lines.toArray)(Array("val1", "val2"))
  }

  test("Query should be able to output to socket") {
    SocketSink.setSocket(mock[Socket])
    val outputStream = new ByteArrayOutputStream()
    doReturn(outputStream).when(SocketSink.getSocket).getOutputStream

    QueryOutput.selectOutput(ds, SocketQueryOut(port = 0), "", null, null)
    env.execute()

    assertResult(outputStream.toString.lines.toArray)(Array("val1", "val2"))
  }

  test("Query should be able to output to Kafka topic") {
    val topicName = "testOutTopic"
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0
    )

    withRunningKafkaOnFoundPort(config) {
      implicit config =>

      QueryOutput.selectOutput(ds, KafkaQueryOut(topic = topicName),"localhost:" + config.kafkaPort, null, null)
      env.execute()

      assertResult(consumeFirstMessageFrom(topicName))("val1")
      assertResult(consumeFirstMessageFrom(topicName))("val2")
    }

  }

  test("Query should be able to output in JSON format to Kafka topic") {
    val topicName = "testOutTopic"
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0
    )

    withRunningKafkaOnFoundPort(config) {
      implicit config =>

        QueryOutput.selectOutput(ds, KafkaQueryOutJson(topic = topicName),"localhost:" + config.kafkaPort, Array(Types.STRING), Array("f0"))
        env.execute()

        assertResult(consumeFirstMessageFrom(topicName))("{\"f0\":\"val1\"}")
        assertResult(consumeFirstMessageFrom(topicName))("{\"f0\":\"val2\"}")

    }

  }

}
