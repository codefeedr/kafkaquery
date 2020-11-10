package org.codefeedr.kafkaquery.transforms

import java.io.IOException
import java.net.{ServerSocket, Socket}
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{
  FlinkKafkaProducer,
  KafkaSerializationSchema
}
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord
import org.codefeedr.kafkaquery.transforms.SchemaConverter.getNestedSchema

import scala.collection.concurrent.TrieMap
import scala.collection.JavaConverters._

object QuerySetup {

  /**
    * Extract the table names from the query and return the ones that are supported.
    *
    * @param query            sql query to be matched
    * @param supportedPlugins list of supported plugins from zookeeper
    * @return the subset of supported plugins
    */
  def extractTopics(
      query: String,
      supportedPlugins: List[String]
  ): List[String] = {
    supportedPlugins.intersect(query.split("\\s+|,|;|\\(|\\)|`"))
  }

  /**
    * Returns the Flink DDL for creating the specified temporary table.
    * @param name name of the table/Kafka topic
    * @param tableFields DDL StringBuilder of the table fields
    * @param kafkaAddr Kafka server address
    * @param checkLatest record retrieval strategy (from LATEST or EARLIEST)
    * @return temporary table DDL string
    */
  def getTableCreationCommand(
      name: String,
      tableFields: java.lang.StringBuilder,
      kafkaAddr: String,
      checkLatest: Boolean
  ): String = {
    "CREATE TEMPORARY TABLE `" + name + "` (" + tableFields + ") " +
      "WITH (" +
      "'connector.type' = 'kafka', " +
      "'connector.version' = 'universal', " +
      "'connector.topic' = '" + name + "', " +
      "'connector.properties.bootstrap.servers' = '" + kafkaAddr + "', " +
      "'connector.startup-mode' = '" +
      (if (checkLatest) "latest-offset" else "earliest-offset") +
      "', " +
      "'format.type' = 'json', " +
      "'format.fail-on-missing-field' = 'false'" +
      ")"
  }

  /**
    * Generate a Flink table schema from an Avro Schema.
    *
    * @param avroSchema  the Avro Schema
    * @return a java.lang.StringBuilder with the DDL description of the schema
    */
  def generateTableSchema(
      avroSchema: org.apache.avro.Schema
  ): java.lang.StringBuilder = {
    val res = new java.lang.StringBuilder()

    val rowtimeEnabled = !"false".equals(avroSchema.getProp("rowtime"))

    var rowtimeFound = false

    for (field <- avroSchema.getFields.asScala) {
      val fieldInfo = getNestedSchema(field.name(), field.schema())

      if (
        rowtimeEnabled && !rowtimeFound && field.getProp("rowtime") == "true"
      ) {
        rowtimeFound = true
        res.append(
          fieldInfo._1 + " TIMESTAMP(3), WATERMARK FOR " + fieldInfo._1 +
            " AS " + fieldInfo._1 + " - INTERVAL '0.001' SECOND, "
        )
      } else {
        res.append(fieldInfo._1 + " " + fieldInfo._2 + ", ")
      }
    }

    if (rowtimeEnabled && !rowtimeFound) {
      res.append("`kafka_time` AS PROCTIME(), ")
    }

    if (res.length() - 2 == res.lastIndexOf(", "))
      res.delete(res.length() - 2, res.length())

    res
  }

  /**
    * Print data stream of query results to console.
    *
    * @param ds datastream of query output
    */
  def queryToConsole(ds: DataStream[Row]): Unit = {
    ds.print()
  }

  /**
    * Write query results to socket of given port.
    *
    * @param port socket port number (0 - random)
    * @param ds   data stream of query output which should be written to socket.
    * @return output port
    */
  def queryToSocket(port: Int, ds: DataStream[Row]): Int = {
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

    server.getLocalPort
  }

  /**
    * Write query results to given Kafka topic.
    *
    * @param outTopic topic name to write results to
    * @param ds       data stream of query output
    */
  def queryToKafkaTopic(
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
