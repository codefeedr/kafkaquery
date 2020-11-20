package org.codefeedr.kafkaquery.transforms

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaProducer,
  KafkaSerializationSchema
}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.kafkaquery.parsers.Configurations.QueryConfig
import org.codefeedr.kafkaquery.sinks.{SimplePrintSinkFunction, SocketSink}

object QueryOutput {

  def selectOutput(
      ds: DataStream[Row],
      qConfig: QueryConfig,
      kafkaAddr: String
  ): Unit = {
    if (qConfig.outTopic.nonEmpty) {
      queryToKafkaTopic(
        qConfig.outTopic,
        ds,
        kafkaAddr
      )
    } else if (qConfig.port != -1) {
      queryToSocket(qConfig.port, ds)
    } else {
      queryToConsole(ds)
    }
  }

  /**
    * Print data stream of query results to console.
    *
    * @param ds datastream of query output
    */
  private def queryToConsole(ds: DataStream[Row]): Unit = {
    ds.addSink(new SimplePrintSinkFunction[Row]())
  }

  /**
    * Write query results to socket of given port.
    *
    * @param port socket port number (0 - random)
    * @param ds   data stream of query output which should be written to socket.
    * @return output port
    */
  private def queryToSocket(port: Int, ds: DataStream[Row]): Unit = {
    ds.addSink(new SocketSink[Row](port))
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
            timestamp: java.lang.Long
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
