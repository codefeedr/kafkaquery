package org.kafkaquery.parsers

import org.kafkaquery.parsers.Configurations.Mode.Mode

import java.io.File
import scala.collection.mutable

object Configurations {

  /** Config which contains all the copied the arguments from the CLI
    *
    * @param mode        an enum of the command being used
    * @param queryConfig query config containing the arguments related to the query
    * @param topicName   topic name
    * @param kafkaAddress the Kafka address (default localhost:9092 or environment variable KAFKA_ADDR if present)
    * @param zookeeperAddress the ZooKeeper address (default localhost:2181 or environment variable ZK_ADDR if present)
    */
  case class Config(
      mode: Mode = null,
      queryConfig: QueryConfig = QueryConfig(),
      topicName: String = "",
      avroSchema: String = "",
      kafkaAddress: String = sys.env.getOrElse("KAFKA_ADDR", "localhost:9092"),
      zookeeperAddress: String = sys.env.getOrElse("ZK_ADDR", "localhost:2181")
  )

  /** Mode decides which config arguments to choose
    */
  object Mode extends Enumeration {
    type Mode = Value
    val Query, Topic, Topics, Schema, Infer = Value
  }

  /** Query config which contains copied arguments from the CLI
    * @param query query string
    * @param output output channel for query results
    * @param startStrategy define start point for fetching elements from Kafka (e.g. from latest/earliest)
    * @param timeout timeout before query prints results
    * @param timeoutFunc function to be executed when timeout is triggered
    * @param ignoreParseErr ignore JSON parse errors during deserialization
    * @param userFunctions list containing tuples with the names and files containing the user defined functions
    */
  case class QueryConfig(
      query: String = "",
      output: QueryOut = ConsoleQueryOut(),
      startStrategy: QueryStart = EarliestQueryStart(),
      timeout: Int = -1,
      timeoutFunc: () => Unit = () => System.exit(0),
      ignoreParseErr: Boolean = true,
      userFunctions: List[(String, File)] = Nil
  )

  abstract class QueryOut()
  object QueryOut {
    val idStrings: mutable.Set[String] = new mutable.HashSet()

    def initFromString(k: String, v: String): QueryOut = k match {
      case "kafka"  => KafkaQueryOut(v)
      case "socket" => SocketQueryOut(v.toInt)
      case _        => ConsoleQueryOut()
    }

  }
  case class ConsoleQueryOut() extends QueryOut
  case class KafkaQueryOut(topic: String) extends QueryOut
  case class SocketQueryOut(port: Int) extends QueryOut

  abstract class QueryStart() {
    def getProperty: String
  }
  object QueryStart {
    def initFromString(k: String): QueryStart = k match {
      case "earliest" => EarliestQueryStart()
      case "latest"   => LatestQueryStart()
    }
  }
  case class EarliestQueryStart() extends QueryStart {
    override def getProperty: String = "earliest-offset"
  }
  case class LatestQueryStart() extends QueryStart {
    override def getProperty: String = "latest-offset"
  }

}
