package org.codefeedr.kafkaquery.transforms

import java.io.{ByteArrayOutputStream, PrintStream}

import net.manub.embeddedkafka.Codecs.stringDeserializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.types.Row
import org.codefeedr.kafkaquery.parsers.Configurations.QueryConfig
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class QueryOutputTest extends AnyFunSuite with BeforeAndAfter with EmbeddedKafka {

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

    QueryOutput.selectOutput(ds, QueryConfig(), "")
    env.execute()

    System.out.flush()
    System.setOut(oldOut)
    assertResult(outContent.toString.lines.toArray)(Array("val1", "val2"))
  }

  test("Query should be able to output to socket") {
    // TODO
  }

  test("Query should be able to output to Kafka topic") {
    val topicName = "testOutTopic"
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = 0,
      zooKeeperPort = 0
    )

    withRunningKafkaOnFoundPort(config) {
      implicit config =>

      QueryOutput.selectOutput(ds, QueryConfig(outTopic = topicName),"localhost:" + config.kafkaPort)
      env.execute()

      assertResult(consumeFirstMessageFrom(topicName))("val1")
      assertResult(consumeFirstMessageFrom(topicName))("val2")
    }

  }

}
