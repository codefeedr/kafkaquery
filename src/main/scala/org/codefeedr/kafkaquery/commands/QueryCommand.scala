package org.codefeedr.kafkaquery.commands

import java.io.IOException
import java.lang
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaProducer,
  KafkaSerializationSchema
}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.kafkaquery.parsers.Configurations.Config
import org.codefeedr.kafkaquery.transforms.{Register, TimeOutFunction}

import scala.collection.concurrent.TrieMap

class QueryCommand(config: Config) {

  private val stream =
    Register.registerAndApply(
      config.queryConfig.query,
      config.zookeeperAddress,
      config.kafkaAddress,
      config.queryConfig.checkLatest
    )

  if (config.queryConfig.timeout > 0) {
    stream
      .keyBy(new NullByteKeySelector[Row]())
      .process(new TimeOutFunction(config.queryConfig.timeout * 1000))
  }

  if (config.queryConfig.outTopic.nonEmpty) {
    queryToKafkaTopic(
      config.queryConfig.outTopic,
      stream,
      config.kafkaAddress
    )
  } else if (config.queryConfig.port != -1) {
    queryToSocket(config.queryConfig.port, stream)
  } else {
    queryToConsole(stream)
  }

  stream.executionEnvironment.execute()

  /**
    * Print data stream of query results to console.
    *
    * @param ds datastream of query output
    */
  private def queryToConsole(ds: DataStream[Row]): Unit = {
    ds.print()
  }

  /**
    * Write query results to socket of given port.
    *
    * @param port socket port number
    * @param ds   data stream of query output which should be written to socket.
    */
  private def queryToSocket(port: Int, ds: DataStream[Row]): Unit = {
    val server: ServerSocket = new ServerSocket(port)

    new Thread {
      override def run(): Unit = {
        val flinkClient: Socket = server.accept()
        val clientMap = new TrieMap[String, Socket]()

        println(
          s"Writing query output to available sockets on port ${server.getLocalPort}..."
        )

        new Thread {
          override def run(): Unit = {
            while (true) {
              val text = flinkClient.getInputStream.readNBytes(
                flinkClient.getInputStream.available()
              )
              clientMap.values.foreach(x => {
                try {
                  x.getOutputStream.write(text)
                } catch {
                  case _: IOException =>
                    clientMap.remove(s"""${x.getInetAddress}:${x.getPort}""")
                    x.close()
                    println(
                      s"""Client ${x.getInetAddress}:${x.getPort} disconnected."""
                    )
                }
              })
            }
          }
        }.start()

        while (true) {
          val newSocket = server.accept()
          println(
            s"""Client ${newSocket.getInetAddress}:${newSocket.getPort} connected."""
          )
          clientMap.put(
            s"""${newSocket.getInetAddress}:${newSocket.getPort}""",
            newSocket
          )
        }
      }
    }.start()

    ds.map(_.toString + "\n")
      .writeToSocket("localhost", server.getLocalPort, new SimpleStringSchema)
  }

  /**
    * Write query results to given Kafka topic.
    *
    * @param outTopic topic name to write results to
    * @param ds       data stream of query output
    */
  private def queryToKafkaTopic(
      outTopic: String,
      ds: DataStream[Row],
      kafkaAddress: String
  ): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", kafkaAddress)
    props.put("acks", "all")
    println(s"Query results are being sent to $outTopic")
    val producer = new FlinkKafkaProducer[Row](
      outTopic,
      new KafkaSerializationSchema[Row] {
        override def serialize(
            element: Row,
            timestamp: lang.Long
        ): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord[Array[Byte], Array[Byte]](
            outTopic,
            element.toString.getBytes(StandardCharsets.UTF_8)
          )
        }
      },
      props,
      FlinkKafkaProducer.Semantic.NONE
    )
    ds.addSink(producer)
  }

}
