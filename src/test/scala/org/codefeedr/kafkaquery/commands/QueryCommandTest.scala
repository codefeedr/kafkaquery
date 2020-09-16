package org.codefeedr.kafkaquery.commands

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader, PrintStream}
import java.net.Socket
import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.types.Row
import org.codefeedr.kafkaquery.parsers.Configurations.{Config, QueryConfig}
import org.codefeedr.kafkaquery.transforms.CollectRowSink
import org.mockito.internal.stubbing.answers.CallsRealMethods
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class QueryCommandTest extends AnyFunSuite with EmbeddedKafka with MockitoSugar with ArgumentMatchersSugar with BeforeAndAfter {

  var env: StreamExecutionEnvironment = _
  var queryCommandMock: QueryCommand = _

  implicit val typeInfo: TypeInformation[Row] = TypeInformation.of(classOf[Row])

  val pypiMessages: List[Row] = List(
    Row.of("title1"),
    Row.of("title2"),
    Row.of("title3")
  )

  before {
    CollectRowSink.result.clear()
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.fromCollection(pypiMessages)
    queryCommandMock = mock[QueryCommand](new CallsRealMethods())
    doReturn((ds, env)).when(queryCommandMock)
      .registerAndApply(any[String], any[String], any[String], any[Boolean])
  }

  test("queryToConsole") {
    val oldOut = System.out
    val outContent = new ByteArrayOutputStream
    System.setOut(new PrintStream(outContent))
    queryCommandMock.apply(Config(queryConfig = QueryConfig(timeout = 1)))
    System.setOut(oldOut)
    assert(outContent.toString.replaceAll("[\n|\r]", "") == "title1title2title3")
  }

  test("queryToSocket") {
    var port = 0
    val buffOut = new ByteArrayOutputStream()
    // Get available port from server
    Console.withOut(buffOut) {
      new Thread {
        override def run(): Unit = {
          queryCommandMock.apply(Config(queryConfig = QueryConfig(port = port)))
        }
      }.start()
      val reg = """Writing query output to available sockets on port (\d+)\.\.\.""".r
      while (buffOut.toString.isEmpty && !reg.pattern.matcher(buffOut.toString).matches) {}
      port = reg.findFirstMatchIn(buffOut.toString).get.group(1).toInt
    }
    val buffIn = new BufferedReader(new InputStreamReader(new Socket("localhost", port).getInputStream))
    val socketLines = new ListBuffer[String]
    while (socketLines.size < 3) socketLines += buffIn.readLine
    assert(socketLines.toList == List("title1", "title2", "title3"))
  }

  test("queryToKafkaTopic") {
    val portKafka = 32581
    val portZookeeper = 32582
    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(
      kafkaPort = portKafka,
      zooKeeperPort = portZookeeper
    )
    withRunningKafkaOnFoundPort(config) {
      implicit config =>
        new Thread {
          override def run(): Unit = {
            queryCommandMock.apply(Config(
              queryConfig = QueryConfig(outTopic = "test"),
              kafkaAddress = "localhost:" + portKafka,
              zookeeperAddress = "localhost:" + portZookeeper)
            )
          }
        }.start()
        new Thread {
          override def run(): Unit = {
            getDataStreamRow(portKafka).addSink(new CollectRowSink())
          }
        }.start()
        while (CollectRowSink.result.size < 3) {
          Thread.sleep(100)
        }
        assert(CollectRowSink.result.asScala.map(_.getField(0).asInstanceOf[String]).toList
          == List("title1", "title2", "title3"))
    }
  }

  def getDataStreamRow(portKafka: Int): DataStream[Row] = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:" + portKafka)
    val cons = new FlinkKafkaConsumer[Row](
      "test",
      new DeserializationSchema[Row] {
        override def deserialize(message: Array[Byte]): Row = Row.of(new String(message))

        override def isEndOfStream(nextElement: Row): Boolean = false

        override def getProducedType: TypeInformation[Row] = TypeInformation.of(classOf[Row])
      },
      props
    )
    cons.setStartFromEarliest()
    env.addSource(cons)
  }

}
